package stream

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// handleSensorEvent evaluates the trigger condition for a sensor write
// and starts the Step Function execution if all conditions are met.
func handleSensorEvent(ctx context.Context, d *lambda.Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	cfg, err := lambda.GetValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline", "pipelineId", pipelineID)
		return nil
	}

	trigger := cfg.Schedule.Trigger

	// Check if this sensor key matches the trigger condition: the key itself
	// or a per-period key like "hourly-status#2026-03-03T18".
	sensorKey := strings.TrimPrefix(sk, "SENSOR#")
	if !lambda.SensorKeyMatches(sensorKey, trigger.Key) {
		// Trigger key doesn't match — check if this sensor matches a post-run rule.
		if cfg.PostRun != nil && lambda.MatchesPostRunRule(sensorKey, cfg.PostRun.Rules) {
			sensorData := lambda.ExtractSensorData(record.Change.NewImage)
			return handlePostRunSensorEvent(ctx, d, cfg, pipelineID, sensorKey, sensorData)
		}
		return nil
	}

	// Extract sensor data from the stream record's NewImage.
	sensorData := lambda.ExtractSensorData(record.Change.NewImage)

	// Capture current time once for consistent use across rule evaluation,
	// calendar checks, and execution date resolution.
	now := d.Now()

	// Build a validation rule from the trigger condition and evaluate it.
	rule := types.ValidationRule{
		Key:   trigger.Key,
		Check: trigger.Check,
		Field: trigger.Field,
		Value: trigger.Value,
	}
	result := validation.EvaluateRule(rule, sensorData, now)
	if !result.Passed {
		d.Logger.Info("trigger condition not met",
			"pipelineId", pipelineID,
			"sensor", sensorKey,
			"reason", result.Reason,
		)
		return nil
	}

	// Check calendar exclusions (wall-clock date).
	if lambda.IsExcluded(cfg, now) {
		d.Logger.Info("pipeline excluded by calendar",
			"pipelineId", pipelineID,
			"date", now.Format("2006-01-02"),
		)
		scheduleIDForEvent := lambda.ResolveScheduleID(cfg)
		dateForEvent := lambda.ResolveExecutionDate(sensorData, now)
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleIDForEvent, dateForEvent,
			fmt.Sprintf("sensor trigger suppressed for %s: wall-clock date excluded by calendar", pipelineID)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
		return nil
	}

	// Resolve schedule ID and date.
	scheduleID := lambda.ResolveScheduleID(cfg)
	date := lambda.ResolveExecutionDate(sensorData, now)

	// Dry-run mode: observe and record what would happen, but never start SFN.
	if cfg.DryRun {
		return handleDryRunTrigger(ctx, d, cfg, pipelineID, scheduleID, date, now)
	}

	// After the cutoff nothing starts for the date. Close it when it has no
	// trigger row yet; otherwise only late data is handled. An error fails
	// the record, so its redelivery retries the close.
	if lambda.CutoffPassed(cfg, date, now) {
		closed, err := lambda.CloseRunAtCutoff(ctx, d, cfg, pipelineID, scheduleID, date, "stream-router")
		if err != nil {
			return fmt.Errorf("close %q at its cutoff: %w", pipelineID, err)
		}
		if !closed {
			handleLateData(ctx, d, cfg, pipelineID, scheduleID, date, now)
			d.Logger.InfoContext(ctx, "sensor ignored: run date will not start",
				"pipelineId", pipelineID, "schedule", scheduleID, "date", date)
		}
		return nil
	}

	// Acquire trigger lock to prevent duplicate executions.
	acquired, err := d.Store.AcquireTriggerLock(ctx, pipelineID, scheduleID, date, lambda.ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("acquire trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		handleLateData(ctx, d, cfg, pipelineID, scheduleID, date, now)
		d.Logger.InfoContext(ctx, "trigger lock already held",
			"pipelineId", pipelineID,
			"schedule", scheduleID,
			"date", date,
		)
		return nil
	}

	lambda.RecordFirstSensorArrival(ctx, d, pipelineID, date, now)

	// Start Step Function execution.
	if err := lambda.StartSFN(ctx, d, cfg, pipelineID, scheduleID, date); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, scheduleID, date); relErr != nil {
			d.Logger.Warn("failed to release lock after SFN start failure", "error", relErr)
		}
		return fmt.Errorf("start SFN for %q: %w", pipelineID, err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventJobTriggered), pipelineID, scheduleID, date,
		fmt.Sprintf("stream trigger fired for %s", pipelineID)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventJobTriggered, "error", err)
	}

	d.Logger.Info("started step function execution",
		"pipelineId", pipelineID,
		"schedule", scheduleID,
		"date", date,
	)
	return nil
}

// handleLateData requests a rerun when the sensor is late data for a date
// whose run completed, and logs a failed check.
func handleLateData(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, now time.Time) {
	if err := checkLateDataArrival(ctx, d, cfg, pipelineID, scheduleID, date, now); err != nil {
		d.Logger.WarnContext(ctx, "late data check failed", "error", err)
	}
}
