package lambda

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
	"github.com/dwsmith1983/interlock/pkg/types"
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

// handleJobFailure processes a job failure or timeout by either re-running
// the pipeline (if under the retry limit) or marking it as permanently failed.
func handleJobFailure(ctx context.Context, d *Deps, pipelineID, schedule, date, jobEvent string) error {
	cfg, err := d.ConfigCache.Get(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline, skipping rerun", "pipelineId", pipelineID)
		return nil
	}

	maxRetries := cfg.Job.MaxRetries

	rerunCount, err := d.Store.CountReruns(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("count reruns for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	if rerunCount >= maxRetries {
		// Retry limit reached — publish exhaustion event and mark as final failure.
		_ = publishEvent(ctx, d, string(types.EventRetryExhausted), pipelineID, schedule, date,
			fmt.Sprintf("retry limit reached (%d/%d) for %s", rerunCount, maxRetries, pipelineID))

		if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
			return fmt.Errorf("set trigger status FAILED_FINAL for %q: %w", pipelineID, err)
		}

		d.Logger.Info("retry limit reached",
			"pipelineId", pipelineID,
			"schedule", schedule,
			"date", date,
			"reruns", rerunCount,
			"maxRetries", maxRetries,
		)
		return nil
	}

	// Under retry limit — write rerun record and restart the pipeline.
	attempt, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, jobEvent, jobEvent)
	if err != nil {
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	if err := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); err != nil {
		return fmt.Errorf("release trigger lock for %q: %w", pipelineID, err)
	}

	if _, err := d.Store.AcquireTriggerLock(ctx, pipelineID, schedule, date, triggerLockTTL); err != nil {
		return fmt.Errorf("re-acquire trigger lock for %q: %w", pipelineID, err)
	}

	// Use a unique execution name that includes the rerun attempt number.
	execName := fmt.Sprintf("%s-%s-%s-rerun-%d", pipelineID, schedule, date, attempt)
	if err := startSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		return fmt.Errorf("start SFN rerun for %q: %w", pipelineID, err)
	}

	d.Logger.Info("started rerun",
		"pipelineId", pipelineID,
		"schedule", schedule,
		"date", date,
		"attempt", attempt,
	)
	return nil
}

// handleJobSuccess publishes a job-completed event to EventBridge.
func handleJobSuccess(ctx context.Context, d *Deps, pipelineID, schedule, date string) error {
	return publishEvent(ctx, d, string(types.EventJobCompleted), pipelineID, schedule, date,
		fmt.Sprintf("job completed for %s", pipelineID))
}

// handleRerunRequest processes a RERUN_REQUEST# stream record. It implements
// a circuit breaker that prevents unnecessary re-runs when the previous run
// succeeded and no sensor data has changed since.
func handleRerunRequest(ctx context.Context, d *Deps, pk, sk string, _ events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	schedule, date, err := parseRerunRequestSK(sk)
	if err != nil {
		return err
	}

	cfg, err := d.ConfigCache.Get(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline, skipping rerun request", "pipelineId", pipelineID)
		return nil
	}

	job, err := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("get latest job event for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	// Decision logic:
	// - No job record → allow (never ran)
	// - Failed/timeout/infra-exhausted → allow (previous run was not successful)
	// - Success → check sensor freshness (only re-run if data changed)
	allowed := true
	reason := ""

	if job != nil {
		switch job.Event {
		case types.JobEventFail, types.JobEventTimeout, types.JobEventInfraTriggerExhausted:
			// Terminal failure — always allow rerun.
			allowed = true
		case types.JobEventSuccess:
			// Check if sensor data has changed since the successful job.
			fresh, err := checkSensorFreshness(ctx, d, pipelineID, job.SK)
			if err != nil {
				return fmt.Errorf("check sensor freshness for %q: %w", pipelineID, err)
			}
			if !fresh {
				allowed = false
				reason = "previous run succeeded and no sensor data has changed"
			}
		default:
			// Unknown event — allow to be safe.
			allowed = true
		}
	}

	if !allowed {
		// Write rejection audit trail.
		_ = d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
			types.JobEventRerunRejected, "", 0, reason)

		_ = publishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, reason))

		d.Logger.Info("rerun request rejected",
			"pipelineId", pipelineID,
			"schedule", schedule,
			"date", date,
			"reason", reason,
		)
		return nil
	}

	// Write acceptance audit trail.
	_ = d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventRerunAccepted, "", 0, "")

	// Release existing lock and re-acquire for the new execution.
	if err := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); err != nil {
		return fmt.Errorf("release trigger lock for %q: %w", pipelineID, err)
	}

	if _, err := d.Store.AcquireTriggerLock(ctx, pipelineID, schedule, date, triggerLockTTL); err != nil {
		return fmt.Errorf("re-acquire trigger lock for %q: %w", pipelineID, err)
	}

	execName := fmt.Sprintf("%s-%s-%s-manual-rerun-%d", pipelineID, schedule, date, time.Now().Unix())
	if err := startSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		return fmt.Errorf("start SFN manual rerun for %q: %w", pipelineID, err)
	}

	d.Logger.Info("started manual rerun",
		"pipelineId", pipelineID,
		"schedule", schedule,
		"date", date,
	)
	return nil
}

// parseRerunRequestSK extracts schedule and date from a RERUN_REQUEST# sort key.
// Expected format: RERUN_REQUEST#<schedule>#<date>
func parseRerunRequestSK(sk string) (schedule, date string, err error) {
	trimmed := strings.TrimPrefix(sk, "RERUN_REQUEST#")
	parts := strings.SplitN(trimmed, "#", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid RERUN_REQUEST SK format: %q", sk)
	}
	return parts[0], parts[1], nil
}

// checkSensorFreshness determines whether any sensor data has been updated
// after the given job completed. The job timestamp is extracted from the job
// SK (format: JOB#schedule#date#<unixMillis>). Returns true if data has
// changed (rerun should proceed) or if freshness cannot be determined.
func checkSensorFreshness(ctx context.Context, d *Deps, pipelineID, jobSK string) (bool, error) {
	// Extract timestamp from the job SK.
	parts := strings.Split(jobSK, "#")
	if len(parts) < 4 {
		// Can't parse timestamp — allow to be safe.
		return true, nil
	}
	jobTimestamp, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		// Can't parse timestamp — allow to be safe.
		return true, nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return false, fmt.Errorf("get sensors for %q: %w", pipelineID, err)
	}
	if len(sensors) == 0 {
		// No sensors — can't prove unchanged, allow.
		return true, nil
	}

	hasAnyUpdatedAt := false
	for _, data := range sensors {
		updatedAt, ok := data["updatedAt"]
		if !ok {
			continue
		}
		hasAnyUpdatedAt = true

		var ts int64
		switch v := updatedAt.(type) {
		case float64:
			ts = int64(v)
		case int64:
			ts = v
		case string:
			ts, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				continue
			}
		default:
			continue
		}

		if ts > jobTimestamp {
			return true, nil // Data changed after job — allow rerun.
		}
	}

	if !hasAnyUpdatedAt {
		// No sensors have updatedAt — can't prove unchanged, allow.
		return true, nil
	}

	// All sensor timestamps are older than the job — data unchanged.
	return false, nil
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

	// Check if this sensor key matches the trigger condition (prefix match
	// allows per-period sensor keys like "hourly-status#2026-03-03T18").
	sensorKey := strings.TrimPrefix(sk, "SENSOR#")
	if !strings.HasPrefix(sensorKey, trigger.Key) {
		return nil
	}

	// Extract sensor data from the stream record's NewImage.
	sensorData := extractSensorData(record.Change.NewImage)

	// Build a validation rule from the trigger condition and evaluate it.
	rule := types.ValidationRule{
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
	date := ResolveExecutionDate(sensorData)

	// Acquire trigger lock to prevent duplicate executions.
	acquired, err := d.Store.AcquireTriggerLock(ctx, pipelineID, scheduleID, date, triggerLockTTL)
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
		return fmt.Errorf("start SFN for %q: %w", pipelineID, err)
	}

	_ = publishEvent(ctx, d, string(types.EventJobTriggered), pipelineID, scheduleID, date,
		fmt.Sprintf("stream trigger fired for %s", pipelineID))

	d.Logger.Info("started step function execution",
		"pipelineId", pipelineID,
		"schedule", scheduleID,
		"date", date,
	)
	return nil
}

// checkLateDataArrival detects sensor updates after a pipeline has completed
// successfully. If the trigger is in terminal COMPLETED state and the latest
// job event is success, this sensor write represents late data that arrived
// after post-job monitoring closed. Dual-writes a joblog entry and publishes
// a LATE_DATA_ARRIVAL event.
func checkLateDataArrival(ctx context.Context, d *Deps, pipelineID, schedule, date string) error {
	trigger, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
	if err != nil || trigger == nil {
		return err
	}

	if trigger.Status != types.TriggerStatusCompleted {
		return nil // still running or failed — not late data
	}

	job, err := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if err != nil || job == nil {
		return err
	}

	if job.Event != types.JobEventSuccess {
		return nil // job didn't succeed — not a "late data after success" scenario
	}

	// Dual-write: joblog entry (audit) + EventBridge event (alerting).
	_ = d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventLateDataArrival, "", 0,
		"sensor updated after pipeline completed successfully")

	_ = publishEvent(ctx, d, string(types.EventLateDataArrival), pipelineID, schedule, date,
		fmt.Sprintf("late data arrival for %s: sensor updated after job completion", pipelineID))

	return nil
}

// sfnInput is the top-level input for the Step Function state machine.
// It includes pipeline identity fields and a config block used by Wait states.
type sfnInput struct {
	PipelineID string    `json:"pipelineId"`
	ScheduleID string    `json:"scheduleId"`
	Date       string    `json:"date"`
	Config     sfnConfig `json:"config"`
}

// sfnConfig holds timing parameters for the SFN evaluation loop and SLA branch.
type sfnConfig struct {
	EvaluationIntervalSeconds int              `json:"evaluationIntervalSeconds"`
	EvaluationWindowSeconds   int              `json:"evaluationWindowSeconds"`
	JobCheckIntervalSeconds   int              `json:"jobCheckIntervalSeconds"`
	SLA                       *types.SLAConfig `json:"sla,omitempty"`
}

// buildSFNConfig converts a PipelineConfig into the config block for the SFN input.
func buildSFNConfig(cfg *types.PipelineConfig) sfnConfig {
	sc := sfnConfig{
		EvaluationIntervalSeconds: 300,  // 5m default
		EvaluationWindowSeconds:   3600, // 1h default
		JobCheckIntervalSeconds:   60,   // 1m default
	}

	if d, err := time.ParseDuration(cfg.Schedule.Evaluation.Interval); err == nil && d > 0 {
		sc.EvaluationIntervalSeconds = int(d.Seconds())
	}
	if d, err := time.ParseDuration(cfg.Schedule.Evaluation.Window); err == nil && d > 0 {
		sc.EvaluationWindowSeconds = int(d.Seconds())
	}

	if cfg.SLA != nil {
		sla := *cfg.SLA
		if sla.Timezone == "" {
			sla.Timezone = "UTC"
		}
		sc.SLA = &sla
	}

	return sc
}

// startSFN starts a Step Function execution with a unique execution name.
// The name includes a Unix timestamp suffix to avoid ExecutionAlreadyExists
// errors when a previous execution for the same pipeline/schedule/date failed.
func startSFN(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string) error {
	name := fmt.Sprintf("%s-%s-%s-%d", pipelineID, scheduleID, date, time.Now().Unix())
	return startSFNWithName(ctx, d, cfg, pipelineID, scheduleID, date, name)
}

// startSFNWithName starts a Step Function execution with a custom execution name.
func startSFNWithName(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, name string) error {
	input := sfnInput{
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
		Config:     buildSFNConfig(cfg),
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("marshal SFN input: %w", err)
	}

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

// extractSensorData converts a DynamoDB stream NewImage to a plain map
// suitable for validation rule evaluation. If the item uses the canonical
// ControlRecord format (sensor fields nested inside a "data" map attribute),
// the "data" map is unwrapped so fields are accessible at the top level.
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

	// Unwrap the "data" map if present (canonical ControlRecord sensor format).
	if dataMap, ok := result["data"].(map[string]interface{}); ok {
		return dataMap
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

// ResolveExecutionDate builds the execution date from sensor data fields.
// If both "date" and "hour" are present, returns "YYYY-MM-DDThh".
// If only "date", returns "YYYY-MM-DD". Falls back to today's date.
func ResolveExecutionDate(sensorData map[string]interface{}) string {
	dateStr, _ := sensorData["date"].(string)
	hourStr, _ := sensorData["hour"].(string)

	if dateStr == "" {
		return time.Now().Format("2006-01-02")
	}

	normalized := normalizeDate(dateStr)

	if hourStr != "" {
		return normalized + "T" + hourStr
	}
	return normalized
}

// normalizeDate converts YYYYMMDD to YYYY-MM-DD. Already-dashed dates pass through.
func normalizeDate(s string) string {
	if len(s) == 8 && !strings.Contains(s, "-") {
		return s[:4] + "-" + s[4:6] + "-" + s[6:8]
	}
	return s
}

// resolveScheduleID returns "cron" if the pipeline uses a cron schedule,
// otherwise returns "stream".
func resolveScheduleID(cfg *types.PipelineConfig) string {
	if cfg.Schedule.Cron != "" {
		return "cron"
	}
	return "stream"
}

// isExcluded checks whether the pipeline should be excluded from running
// based on calendar exclusions (weekends and specific dates).
func isExcluded(cfg *types.PipelineConfig, now time.Time) bool {
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

	detail := types.InterlockEvent{
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

	source := types.EventSource
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
