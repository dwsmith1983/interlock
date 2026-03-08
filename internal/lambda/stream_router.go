package lambda

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/internal/validation"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// ResolveTriggerLockTTL returns the trigger lock TTL based on the
// SFN_TIMEOUT_SECONDS env var plus a 30-minute buffer. Defaults to
// 4h30m if the env var is not set or invalid.
func ResolveTriggerLockTTL() time.Duration {
	s := os.Getenv("SFN_TIMEOUT_SECONDS")
	if s == "" {
		return DefaultTriggerLockTTL
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return DefaultTriggerLockTTL
	}
	return time.Duration(sec)*time.Second + TriggerLockBuffer
}

// getValidatedConfig loads a pipeline config and validates its retry/timeout
// fields. Returns nil (with a warning log) if validation fails, signalling the
// caller to skip processing for this pipeline.
func getValidatedConfig(ctx context.Context, d *Deps, pipelineID string) (*types.PipelineConfig, error) {
	cfg, err := d.ConfigCache.Get(ctx, pipelineID)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, nil
	}
	if errs := validation.ValidatePipelineConfig(cfg); len(errs) > 0 {
		d.Logger.Warn("invalid pipeline config, skipping",
			"pipelineId", pipelineID,
			"errors", errs,
		)
		return nil, nil
	}
	return cfg, nil
}

// HandleStreamEvent processes a DynamoDB stream event, routing each record
// to the appropriate handler based on the SK prefix. Errors are logged but
// do not fail the batch (returns nil) to prevent infinite retries.
func HandleStreamEvent(ctx context.Context, d *Deps, event StreamEvent) error {
	for i := range event.Records {
		if err := handleRecord(ctx, d, event.Records[i]); err != nil {
			d.Logger.Error("stream record error",
				"error", err,
				"eventID", event.Records[i].EventID,
			)
		}
	}
	return nil
}

// handleRecord extracts PK/SK and routes to the appropriate handler.
func handleRecord(ctx context.Context, d *Deps, record events.DynamoDBEventRecord) error {
	pk, sk := extractKeys(record)
	if pk == "" || sk == "" {
		return fmt.Errorf("record missing PK or SK")
	}

	switch {
	case strings.HasPrefix(sk, "SENSOR#"):
		return handleSensorEvent(ctx, d, pk, sk, record)
	case sk == types.ConfigSK:
		d.Logger.Info("config changed, invalidating cache", "pk", pk)
		d.ConfigCache.Invalidate()
		return nil
	case strings.HasPrefix(sk, "JOB#"):
		return handleJobLogEvent(ctx, d, pk, sk, record)
	case strings.HasPrefix(sk, "RERUN_REQUEST#"):
		return handleRerunRequest(ctx, d, pk, sk, record)
	default:
		return nil
	}
}

// handleJobLogEvent processes a JOB# stream record, routing to failure
// re-run logic or success notification based on the job event outcome.
func handleJobLogEvent(ctx context.Context, d *Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	// Extract the "event" attribute from NewImage (success/fail/timeout).
	eventAttr, ok := record.Change.NewImage["event"]
	if !ok || eventAttr.DataType() != events.DataTypeString {
		d.Logger.Warn("JOB record missing event attribute", "pk", pk, "sk", sk)
		return nil
	}
	jobEvent := eventAttr.String()

	// Parse schedule and date from SK: JOB#<schedule>#<date>#<timestamp>
	schedule, date, err := parseJobSK(sk)
	if err != nil {
		return err
	}

	switch jobEvent {
	case types.JobEventFail, types.JobEventTimeout:
		return handleJobFailure(ctx, d, pipelineID, schedule, date, jobEvent)
	case types.JobEventSuccess:
		return handleJobSuccess(ctx, d, pipelineID, schedule, date)
	default:
		d.Logger.Warn("unknown job event", "event", jobEvent, "pipelineId", pipelineID)
		return nil
	}
}

// parseJobSK extracts schedule and date from a JOB# sort key.
// Expected format: JOB#<schedule>#<date>#<timestamp>
func parseJobSK(sk string) (schedule, date string, err error) {
	trimmed := strings.TrimPrefix(sk, "JOB#")
	parts := strings.SplitN(trimmed, "#", 3)
	if len(parts) < 3 {
		return "", "", fmt.Errorf("invalid JOB SK format: %q", sk)
	}
	return parts[0], parts[1], nil
}

// handleJobSuccess publishes a job-completed event to EventBridge.
func handleJobSuccess(ctx context.Context, d *Deps, pipelineID, schedule, date string) error {
	return publishEvent(ctx, d, string(types.EventJobCompleted), pipelineID, schedule, date,
		fmt.Sprintf("job completed for %s", pipelineID))
}

// handleSensorEvent evaluates the trigger condition for a sensor write
// and starts the Step Function execution if all conditions are met.
func handleSensorEvent(ctx context.Context, d *Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	cfg, err := getValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline", "pipelineId", pipelineID)
		return nil
	}

	// Only process if the pipeline has a stream trigger condition.
	trigger := cfg.Schedule.Trigger
	if trigger == nil {
		return nil
	}

	// Check if this sensor key matches the trigger condition (prefix match
	// allows per-period sensor keys like "hourly-status#2026-03-03T18").
	sensorKey := strings.TrimPrefix(sk, "SENSOR#")
	if !strings.HasPrefix(sensorKey, trigger.Key) {
		// Trigger key doesn't match — check if this sensor matches a post-run rule.
		if cfg.PostRun != nil && matchesPostRunRule(sensorKey, cfg.PostRun.Rules) {
			sensorData := extractSensorData(record.Change.NewImage)
			return handlePostRunSensorEvent(ctx, d, cfg, pipelineID, sensorKey, sensorData)
		}
		return nil
	}

	// Extract sensor data from the stream record's NewImage.
	sensorData := extractSensorData(record.Change.NewImage)

	// Capture current time once for consistent use across rule evaluation,
	// calendar checks, and execution date resolution.
	now := d.now()

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
	if isExcluded(cfg, now) {
		d.Logger.Info("pipeline excluded by calendar",
			"pipelineId", pipelineID,
			"date", now.Format("2006-01-02"),
		)
		scheduleIDForEvent := resolveScheduleID(cfg)
		dateForEvent := ResolveExecutionDate(sensorData, now)
		if pubErr := publishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleIDForEvent, dateForEvent,
			fmt.Sprintf("sensor trigger suppressed for %s: wall-clock date excluded by calendar", pipelineID)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
		return nil
	}

	// Resolve schedule ID and date.
	scheduleID := resolveScheduleID(cfg)
	date := ResolveExecutionDate(sensorData, now)

	// Acquire trigger lock to prevent duplicate executions.
	acquired, err := d.Store.AcquireTriggerLock(ctx, pipelineID, scheduleID, date, ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("acquire trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		// Check if this is late data arriving after a completed pipeline.
		if err := checkLateDataArrival(ctx, d, pipelineID, scheduleID, date); err != nil {
			d.Logger.WarnContext(ctx, "late data check failed", "error", err)
		}
		d.Logger.InfoContext(ctx, "trigger lock already held",
			"pipelineId", pipelineID,
			"schedule", scheduleID,
			"date", date,
		)
		return nil
	}

	// Start Step Function execution.
	if err := startSFN(ctx, d, cfg, pipelineID, scheduleID, date); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, scheduleID, date); relErr != nil {
			d.Logger.Warn("failed to release lock after SFN start failure", "error", relErr)
		}
		return fmt.Errorf("start SFN for %q: %w", pipelineID, err)
	}

	if err := publishEvent(ctx, d, string(types.EventJobTriggered), pipelineID, scheduleID, date,
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
