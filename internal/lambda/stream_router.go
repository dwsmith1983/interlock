package lambda

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// HandleStreamEvent processes a DynamoDB stream event, routing each record
// to the appropriate handler based on the SK prefix. Per-record errors are
// collected as BatchItemFailures so the Lambda runtime can use DynamoDB's
// ReportBatchItemFailures to retry only the failed records.
func HandleStreamEvent(ctx context.Context, d *Deps, event StreamEvent) (events.DynamoDBEventResponse, error) {
	var resp events.DynamoDBEventResponse
	for i := range event.Records {
		if err := handleRecord(ctx, d, event.Records[i]); err != nil {
			d.Logger.Error("stream record error",
				"error", err,
				"eventID", event.Records[i].EventID,
			)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: event.Records[i].EventID,
			})
		}
	}
	return resp, nil
}

// handleRecord extracts PK/SK and routes to the appropriate handler.
func handleRecord(ctx context.Context, d *Deps, record events.DynamoDBEventRecord) error {
	pk, sk := ExtractKeys(record)
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
	return PublishEvent(ctx, d, string(types.EventJobCompleted), pipelineID, schedule, date,
		fmt.Sprintf("job completed for %s", pipelineID))
}

// handleSensorEvent evaluates the trigger condition for a sensor write
// and starts the Step Function execution if all conditions are met.
func handleSensorEvent(ctx context.Context, d *Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	cfg, err := GetValidatedConfig(ctx, d, pipelineID)
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
		if cfg.PostRun != nil && MatchesPostRunRule(sensorKey, cfg.PostRun.Rules) {
			sensorData := ExtractSensorData(record.Change.NewImage)
			return handlePostRunSensorEvent(ctx, d, cfg, pipelineID, sensorKey, sensorData)
		}
		return nil
	}

	// Extract sensor data from the stream record's NewImage.
	sensorData := ExtractSensorData(record.Change.NewImage)

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
	if IsExcluded(cfg, now) {
		d.Logger.Info("pipeline excluded by calendar",
			"pipelineId", pipelineID,
			"date", now.Format("2006-01-02"),
		)
		scheduleIDForEvent := ResolveScheduleID(cfg)
		dateForEvent := ResolveExecutionDate(sensorData, now)
		if pubErr := PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleIDForEvent, dateForEvent,
			fmt.Sprintf("sensor trigger suppressed for %s: wall-clock date excluded by calendar", pipelineID)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
		return nil
	}

	// Resolve schedule ID and date.
	scheduleID := ResolveScheduleID(cfg)
	date := ResolveExecutionDate(sensorData, now)

	// Dry-run mode: observe and record what would happen, but never start SFN.
	if cfg.DryRun {
		return handleDryRunTrigger(ctx, d, cfg, pipelineID, scheduleID, date, now)
	}

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

	// Record first sensor arrival time (idempotent — only writes if absent).
	// This timestamp serves as T=0 for relative SLA calculation.
	arrivalKey := "first-sensor-arrival#" + date
	if _, writeErr := d.Store.WriteSensorIfAbsent(ctx, pipelineID, arrivalKey, map[string]interface{}{
		"arrivedAt": now.UTC().Format(time.RFC3339),
	}); writeErr != nil {
		d.Logger.WarnContext(ctx, "failed to write first-sensor-arrival",
			"pipelineId", pipelineID, "date", date, "error", writeErr)
	}

	// Start Step Function execution.
	if err := StartSFN(ctx, d, cfg, pipelineID, scheduleID, date); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, scheduleID, date); relErr != nil {
			d.Logger.Warn("failed to release lock after SFN start failure", "error", relErr)
		}
		return fmt.Errorf("start SFN for %q: %w", pipelineID, err)
	}

	if err := PublishEvent(ctx, d, string(types.EventJobTriggered), pipelineID, scheduleID, date,
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
