package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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

// ResolveTriggerLockTTL returns the trigger lock TTL based on the
// SFN_TIMEOUT_SECONDS env var plus a 30-minute buffer. Defaults to
// 4h30m if the env var is not set or invalid.
func ResolveTriggerLockTTL() time.Duration {
	s := os.Getenv("SFN_TIMEOUT_SECONDS")
	if s == "" {
		return 4*time.Hour + 30*time.Minute
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return 4*time.Hour + 30*time.Minute
	}
	return time.Duration(sec)*time.Second + 30*time.Minute
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

// handleJobFailure processes a job failure or timeout by either re-running
// the pipeline (if under the retry limit) or marking it as permanently failed.
func handleJobFailure(ctx context.Context, d *Deps, pipelineID, schedule, date, jobEvent string) error {
	cfg, err := getValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline, skipping rerun", "pipelineId", pipelineID)
		return nil
	}

	maxRetries := cfg.Job.MaxRetries

	// Check if the latest failure has a category for budget selection.
	latestJob, jobErr := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if jobErr != nil {
		d.Logger.Warn("could not read latest job event for failure category",
			"pipelineId", pipelineID, "error", jobErr)
	}
	if latestJob != nil {
		if types.FailureCategory(latestJob.Category) == types.FailurePermanent {
			maxRetries = types.IntOrDefault(cfg.Job.MaxCodeRetries, 1)
		}
		// TRANSIENT, TIMEOUT, or empty → use cfg.Job.MaxRetries (already set).
	}

	rerunCount, err := d.Store.CountRerunsBySource(ctx, pipelineID, schedule, date, []string{"job-fail-retry"})
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
	attempt, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, "job-fail-retry", jobEvent)
	if err != nil {
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	if err := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); err != nil {
		return fmt.Errorf("release trigger lock for %q: %w", pipelineID, err)
	}

	acquired, err := d.Store.AcquireTriggerLock(ctx, pipelineID, schedule, date, ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("re-acquire trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		d.Logger.Warn("failed to re-acquire trigger lock, skipping rerun",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
		return nil
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

// handleRerunRequest processes a RERUN_REQUEST# stream record. It enforces
// per-source rerun limits (drift vs manual) and implements a circuit breaker
// that prevents unnecessary re-runs when the previous run succeeded and no
// sensor data has changed since.
func handleRerunRequest(ctx context.Context, d *Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	schedule, date, err := parseRerunRequestSK(sk)
	if err != nil {
		return err
	}

	cfg, err := getValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline, skipping rerun request", "pipelineId", pipelineID)
		return nil
	}

	// Extract reason from stream record NewImage. Default to "manual".
	reason := "manual"
	if img := record.Change.NewImage; img != nil {
		if r, ok := img["reason"]; ok && r.DataType() == events.DataTypeString {
			if v := r.String(); v != "" {
				reason = v
			}
		}
	}

	// --- Rerun limit check ---
	var budget int
	var sources []string
	var limitLabel string
	switch reason {
	case "data-drift", "late-data":
		budget = types.IntOrDefault(cfg.Job.MaxDriftReruns, 1)
		sources = []string{"data-drift", "late-data"}
		limitLabel = "drift rerun limit exceeded"
	default:
		budget = types.IntOrDefault(cfg.Job.MaxManualReruns, 1)
		sources = []string{reason}
		limitLabel = "manual rerun limit exceeded"
	}

	count, err := d.Store.CountRerunsBySource(ctx, pipelineID, schedule, date, sources)
	if err != nil {
		return fmt.Errorf("count reruns by source for %q: %w", pipelineID, err)
	}

	if count >= budget {
		_ = d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
			types.JobEventRerunRejected, "", 0, limitLabel)
		_ = publishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, limitLabel))
		d.Logger.Info("rerun request rejected (limit exceeded)",
			"pipelineId", pipelineID, "schedule", schedule, "date", date,
			"reason", reason, "count", count, "budget", budget)
		return nil
	}

	// --- Circuit breaker (sensor freshness) ---
	job, err := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("get latest job event for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	allowed := true
	rejectReason := ""
	if job != nil && job.Event == types.JobEventSuccess {
		fresh, err := checkSensorFreshness(ctx, d, pipelineID, job.SK)
		if err != nil {
			return fmt.Errorf("check sensor freshness for %q: %w", pipelineID, err)
		}
		if !fresh {
			allowed = false
			rejectReason = "previous run succeeded and no sensor data has changed"
		}
	}

	if !allowed {
		_ = d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
			types.JobEventRerunRejected, "", 0, rejectReason)
		_ = publishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, rejectReason))
		d.Logger.Info("rerun request rejected",
			"pipelineId", pipelineID, "schedule", schedule, "date", date,
			"reason", rejectReason)
		return nil
	}

	// --- Acceptance: write rerun record FIRST (before lock release) ---
	if _, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, reason, ""); err != nil {
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	_ = d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventRerunAccepted, "", 0, "")

	// Delete date-scoped postrun-baseline so re-run captures fresh baseline.
	if cfg.PostRun != nil {
		_ = d.Store.DeleteSensor(ctx, pipelineID, "postrun-baseline#"+date)
	}

	// Release existing lock and re-acquire for the new execution.
	if err := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); err != nil {
		return fmt.Errorf("release trigger lock for %q: %w", pipelineID, err)
	}

	acquired, err := d.Store.AcquireTriggerLock(ctx, pipelineID, schedule, date, ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("re-acquire trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		d.Logger.Warn("failed to re-acquire trigger lock, skipping rerun",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
		return nil
	}

	execName := fmt.Sprintf("%s-%s-%s-%s-rerun-%d", pipelineID, schedule, date, reason, time.Now().Unix())
	if err := startSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		return fmt.Errorf("start SFN rerun for %q: %w", pipelineID, err)
	}

	d.Logger.Info("started rerun",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason)
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

	// Trigger a re-run — circuit breaker in handleRerunRequest will validate sensor freshness.
	if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, schedule, date, "late-data"); writeErr != nil {
		d.Logger.WarnContext(ctx, "failed to write rerun request on late data", "pipelineId", pipelineID, "error", writeErr)
	}

	return nil
}

// matchesPostRunRule returns true if the sensor key matches any post-run rule key
// (prefix match to support per-period sensor keys).
func matchesPostRunRule(sensorKey string, rules []types.ValidationRule) bool {
	for _, rule := range rules {
		if strings.HasPrefix(sensorKey, rule.Key) {
			return true
		}
	}
	return false
}

// handlePostRunSensorEvent evaluates post-run rules reactively when a sensor
// arrives via DynamoDB Stream. Compares current sensor values against the
// date-scoped baseline captured at trigger completion.
func handlePostRunSensorEvent(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}) error {
	scheduleID := resolveScheduleID(cfg)
	date := ResolveExecutionDate(sensorData)

	// Consistent read to handle race where sensor stream event arrives
	// before SFN sets trigger to COMPLETED.
	trigger, err := d.Store.GetTrigger(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get trigger for post-run: %w", err)
	}
	if trigger == nil {
		return nil // No trigger for this date — not a post-run event.
	}

	switch trigger.Status {
	case types.TriggerStatusRunning:
		// Job still running — evaluate rules for informational drift detection.
		return handlePostRunInflight(ctx, d, cfg, pipelineID, scheduleID, date, sensorKey, sensorData)

	case types.TriggerStatusCompleted:
		// Job completed — full post-run evaluation with baseline comparison.
		return handlePostRunCompleted(ctx, d, cfg, pipelineID, scheduleID, date, sensorData)

	default:
		// FAILED_FINAL or unknown — skip.
		return nil
	}
}

// handlePostRunInflight evaluates post-run rules while the job is still running.
// If drift is detected, publishes an informational event but does NOT trigger a rerun.
func handlePostRunInflight(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, sensorKey string, sensorData map[string]interface{}) error {
	// Read baseline for comparison.
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for inflight check: %w", err)
	}
	if baseline == nil {
		return nil // No baseline yet — job hasn't completed once.
	}

	prevCount := ExtractFloat(baseline, "sensor_count")
	currCount := ExtractFloat(sensorData, "sensor_count")
	if prevCount > 0 && currCount > 0 && currCount != prevCount {
		_ = publishEvent(ctx, d, string(types.EventPostRunDriftInflight), pipelineID, scheduleID, date,
			fmt.Sprintf("inflight drift detected for %s: %.0f → %.0f (informational)", pipelineID, prevCount, currCount),
			map[string]interface{}{
				"previousCount": prevCount,
				"currentCount":  currCount,
				"sensorKey":     sensorKey,
				"source":        "post-run-stream",
			})
	}
	return nil
}

// handlePostRunCompleted evaluates post-run rules after the job has completed.
// Compares sensor values against the date-scoped baseline and triggers a rerun
// if drift is detected.
func handlePostRunCompleted(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, sensorData map[string]interface{}) error {
	// Read baseline captured at trigger completion.
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for post-run: %w", err)
	}

	// Check for data drift if baseline exists.
	if baseline != nil {
		prevCount := ExtractFloat(baseline, "sensor_count")
		currCount := ExtractFloat(sensorData, "sensor_count")
		if prevCount > 0 && currCount > 0 && currCount != prevCount {
			delta := currCount - prevCount
			_ = publishEvent(ctx, d, string(types.EventPostRunDrift), pipelineID, scheduleID, date,
				fmt.Sprintf("post-run drift detected for %s: %.0f → %.0f records", pipelineID, prevCount, currCount),
				map[string]interface{}{
					"previousCount": prevCount,
					"currentCount":  currCount,
					"delta":         delta,
					"source":        "post-run-stream",
				})

			// Trigger rerun via the existing circuit breaker path.
			if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, scheduleID, date, "data-drift"); writeErr != nil {
				d.Logger.WarnContext(ctx, "failed to write rerun request on post-run drift",
					"pipelineId", pipelineID, "error", writeErr)
			}
			return nil
		}
	}

	// Evaluate post-run validation rules.
	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors for post-run rules: %w", err)
	}
	RemapPerPeriodSensors(sensors, date)

	result := validation.EvaluateRules("ALL", cfg.PostRun.Rules, sensors, time.Now())

	if result.Passed {
		_ = publishEvent(ctx, d, string(types.EventPostRunPassed), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run validation passed for %s", pipelineID))
	} else {
		_ = publishEvent(ctx, d, string(types.EventPostRunFailed), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run validation failed for %s", pipelineID))
	}

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
	JobPollWindowSeconds      int              `json:"jobPollWindowSeconds"`
	SLA                       *types.SLAConfig `json:"sla,omitempty"`
}

// buildSFNConfig converts a PipelineConfig into the config block for the SFN input.
func buildSFNConfig(cfg *types.PipelineConfig) sfnConfig {
	sc := sfnConfig{
		EvaluationIntervalSeconds: 300,  // 5m default
		EvaluationWindowSeconds:   3600, // 1h default
		JobCheckIntervalSeconds:   60,   // 1m default
		JobPollWindowSeconds:      3600, // 1h default
	}

	if d, err := time.ParseDuration(cfg.Schedule.Evaluation.Interval); err == nil && d > 0 {
		sc.EvaluationIntervalSeconds = int(d.Seconds())
	}
	if d, err := time.ParseDuration(cfg.Schedule.Evaluation.Window); err == nil && d > 0 {
		sc.EvaluationWindowSeconds = int(d.Seconds())
	}

	if cfg.Job.JobPollWindowSeconds != nil && *cfg.Job.JobPollWindowSeconds > 0 {
		sc.JobPollWindowSeconds = *cfg.Job.JobPollWindowSeconds
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
	sc := buildSFNConfig(cfg)

	// Warn if the sum of evaluation + poll windows exceeds the SFN timeout.
	totalWindowSec := sc.EvaluationWindowSeconds + sc.JobPollWindowSeconds
	sfnTimeout := ResolveTriggerLockTTL() - 30*time.Minute // strip the buffer to get raw SFN timeout
	if sfnTimeout > 0 && time.Duration(totalWindowSec)*time.Second > sfnTimeout {
		d.Logger.Warn("combined pipeline windows exceed SFN timeout",
			"pipelineId", pipelineID,
			"evalWindowSec", sc.EvaluationWindowSeconds,
			"jobPollWindowSec", sc.JobPollWindowSeconds,
			"totalWindowSec", totalWindowSec,
			"sfnTimeoutSec", int(sfnTimeout.Seconds()),
		)
	}

	input := sfnInput{
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
		Config:     sc,
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
func publishEvent(ctx context.Context, d *Deps, eventType, pipelineID, schedule, date, message string, detail ...map[string]interface{}) error {
	if d.EventBridge == nil || d.EventBusName == "" {
		return nil
	}

	evt := types.InterlockEvent{
		PipelineID: pipelineID,
		ScheduleID: schedule,
		Date:       date,
		Message:    message,
		Timestamp:  time.Now(),
	}
	if len(detail) > 0 && detail[0] != nil {
		evt.Detail = detail[0]
	}
	detailJSON, err := json.Marshal(evt)
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
