package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebTypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"

	"github.com/dwsmith1983/interlock/internal/validation"
	v2 "github.com/dwsmith1983/interlock/pkg/types/v2"
)

// triggerLockTTL is the default TTL for trigger dedup locks.
const triggerLockTTL = 24 * time.Hour

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
	case sk == v2.ConfigSK:
		d.Logger.Info("config changed, invalidating cache", "pk", pk)
		d.ConfigCache.Invalidate()
		return nil
	case strings.HasPrefix(sk, "JOB#"):
		return handleJobLogEvent(ctx, d, pk, sk, record)
	default:
		return nil
	}
}

// handleJobLogEvent is a stub for job failure re-run logic. Task 13 will implement it.
func handleJobLogEvent(_ context.Context, _ *Deps, _, _ string, _ events.DynamoDBEventRecord) error {
	return nil
}

// handleSensorEvent evaluates the trigger condition for a sensor write
// and starts the Step Function execution if all conditions are met.
func handleSensorEvent(ctx context.Context, d *Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	cfg, err := d.ConfigCache.Get(ctx, pipelineID)
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

	// Check if this sensor key matches the trigger condition.
	sensorKey := strings.TrimPrefix(sk, "SENSOR#")
	if sensorKey != trigger.Key {
		return nil
	}

	// Extract sensor data from the stream record's NewImage.
	sensorData := extractSensorData(record.Change.NewImage)

	// Build a validation rule from the trigger condition and evaluate it.
	rule := v2.ValidationRule{
		Key:   trigger.Key,
		Check: trigger.Check,
		Field: trigger.Field,
		Value: trigger.Value,
	}
	result := validation.EvaluateRule(rule, sensorData, time.Now())
	if !result.Passed {
		d.Logger.Info("trigger condition not met",
			"pipelineId", pipelineID,
			"sensor", sensorKey,
			"reason", result.Reason,
		)
		return nil
	}

	// Check calendar exclusions.
	now := time.Now()
	if isExcluded(cfg, now) {
		d.Logger.Info("pipeline excluded by calendar",
			"pipelineId", pipelineID,
			"date", now.Format("2006-01-02"),
		)
		return nil
	}

	// Resolve schedule ID and date.
	scheduleID := resolveScheduleID(cfg)
	date := now.Format("2006-01-02")

	// Acquire trigger lock to prevent duplicate executions.
	acquired, err := d.Store.AcquireTriggerLock(ctx, pipelineID, scheduleID, date, triggerLockTTL)
	if err != nil {
		return fmt.Errorf("acquire trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		d.Logger.Info("trigger lock already held",
			"pipelineId", pipelineID,
			"schedule", scheduleID,
			"date", date,
		)
		return nil
	}

	// Start Step Function execution.
	if err := startSFN(ctx, d, pipelineID, scheduleID, date); err != nil {
		return fmt.Errorf("start SFN for %q: %w", pipelineID, err)
	}

	_ = publishEvent(ctx, d, string(v2.EventJobTriggered), pipelineID, scheduleID, date,
		fmt.Sprintf("stream trigger fired for %s", pipelineID))

	d.Logger.Info("started step function execution",
		"pipelineId", pipelineID,
		"schedule", scheduleID,
		"date", date,
	)
	return nil
}

// startSFN starts a Step Function execution with the orchestrator input payload.
func startSFN(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) error {
	input := OrchestratorInput{
		Mode:       "evaluate",
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal SFN input: %w", err)
	}

	name := fmt.Sprintf("%s-%s-%s", pipelineID, scheduleID, date)
	inputStr := string(payload)

	_, err = d.SFNClient.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: &d.StateMachineARN,
		Name:            &name,
		Input:           &inputStr,
	})
	if err != nil {
		return fmt.Errorf("StartExecution: %w", err)
	}
	return nil
}

// extractKeys returns the PK and SK string values from a DynamoDB stream record.
func extractKeys(record events.DynamoDBEventRecord) (pk, sk string) {
	keys := record.Change.Keys
	if pkAttr, ok := keys["PK"]; ok && pkAttr.DataType() == events.DataTypeString {
		pk = pkAttr.String()
	}
	if skAttr, ok := keys["SK"]; ok && skAttr.DataType() == events.DataTypeString {
		sk = skAttr.String()
	}
	return pk, sk
}

// extractSensorData converts a DynamoDB stream NewImage to a plain map,
// skipping PK, SK, and ttl keys.
func extractSensorData(newImage map[string]events.DynamoDBAttributeValue) map[string]interface{} {
	if newImage == nil {
		return nil
	}

	skipKeys := map[string]bool{"PK": true, "SK": true, "ttl": true}
	result := make(map[string]interface{}, len(newImage))

	for k, av := range newImage {
		if skipKeys[k] {
			continue
		}
		result[k] = convertAttributeValue(av)
	}
	return result
}

// convertAttributeValue converts a DynamoDB stream attribute value to a Go native type.
func convertAttributeValue(av events.DynamoDBAttributeValue) interface{} {
	switch av.DataType() {
	case events.DataTypeString:
		return av.String()
	case events.DataTypeNumber:
		// Try int first, fall back to float.
		if i, err := strconv.ParseInt(av.Number(), 10, 64); err == nil {
			return float64(i)
		}
		if f, err := strconv.ParseFloat(av.Number(), 64); err == nil {
			return f
		}
		return av.Number()
	case events.DataTypeBoolean:
		return av.Boolean()
	case events.DataTypeNull:
		return nil
	case events.DataTypeMap:
		m := av.Map()
		out := make(map[string]interface{}, len(m))
		for k, v := range m {
			out[k] = convertAttributeValue(v)
		}
		return out
	case events.DataTypeList:
		l := av.List()
		out := make([]interface{}, len(l))
		for i, v := range l {
			out[i] = convertAttributeValue(v)
		}
		return out
	default:
		return nil
	}
}

// resolveScheduleID returns "cron" if the pipeline uses a cron schedule,
// otherwise returns "stream".
func resolveScheduleID(cfg *v2.PipelineConfig) string {
	if cfg.Schedule.Cron != "" {
		return "cron"
	}
	return "stream"
}

// isExcluded checks whether the pipeline should be excluded from running
// based on calendar exclusions (weekends and specific dates).
func isExcluded(cfg *v2.PipelineConfig, now time.Time) bool {
	excl := cfg.Schedule.Exclude
	if excl == nil {
		return false
	}

	// Resolve timezone if configured.
	t := now
	if cfg.Schedule.Timezone != "" {
		if loc, err := time.LoadLocation(cfg.Schedule.Timezone); err == nil {
			t = now.In(loc)
		}
	}

	// Check weekends.
	if excl.Weekends {
		day := t.Weekday()
		if day == time.Saturday || day == time.Sunday {
			return true
		}
	}

	// Check specific dates.
	dateStr := t.Format("2006-01-02")
	for _, d := range excl.Dates {
		if d == dateStr {
			return true
		}
	}

	return false
}

// publishEvent sends an event to EventBridge. It is safe to call when
// EventBridge is nil or EventBusName is empty (returns nil with no action).
func publishEvent(ctx context.Context, d *Deps, eventType, pipelineID, schedule, date, message string) error {
	if d.EventBridge == nil || d.EventBusName == "" {
		return nil
	}

	detail := v2.InterlockEvent{
		PipelineID: pipelineID,
		ScheduleID: schedule,
		Date:       date,
		Message:    message,
		Timestamp:  time.Now(),
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal event detail: %w", err)
	}

	source := v2.EventSource
	detailStr := string(detailJSON)

	_, err = d.EventBridge.PutEvents(ctx, &eventbridge.PutEventsInput{
		Entries: []ebTypes.PutEventsRequestEntry{
			{
				Source:       &source,
				DetailType:   &eventType,
				Detail:       &detailStr,
				EventBusName: &d.EventBusName,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish %s event: %w", eventType, err)
	}
	return nil
}
