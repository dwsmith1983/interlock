package lambda

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/pkg/types"
)

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

	// Dry-run pipelines evaluate all checks but publish observation events
	// instead of executing side effects.
	if cfg.DryRun {
		return handleDryRunRerunRequest(ctx, d, cfg, pipelineID, schedule, date, record)
	}

	// --- Calendar exclusion check (execution date) ---
	if isExcludedDate(cfg, date) {
		if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date, types.JobEventRerunRejected, "", 0, "excluded by calendar"); err != nil {
			d.Logger.Warn("failed to write rerun-rejected joblog for calendar exclusion", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
		}
		if pubErr := publishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, schedule, date,
			fmt.Sprintf("rerun blocked for %s: execution date %s excluded by calendar", pipelineID, date)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
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
		if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
			types.JobEventRerunRejected, "", 0, limitLabel); err != nil {
			d.Logger.Warn("failed to write rerun-rejected joblog for limit exceeded", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
		}
		if err := publishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, limitLabel)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunRejected, "error", err)
		}
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
		if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
			types.JobEventRerunRejected, "", 0, rejectReason); err != nil {
			d.Logger.Warn("failed to write rerun-rejected joblog for circuit breaker", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
		}
		if err := publishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, rejectReason)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunRejected, "error", err)
		}
		d.Logger.Info("rerun request rejected",
			"pipelineId", pipelineID, "schedule", schedule, "date", date,
			"reason", rejectReason)
		return nil
	}

	// --- Acceptance: acquire lock FIRST (before writing rerun) ---
	acquired, err := d.Store.ResetTriggerLock(ctx, pipelineID, schedule, date, ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("reset trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		if pubErr := publishEvent(ctx, d, string(types.EventInfraFailure), pipelineID, schedule, date,
			fmt.Sprintf("lock reset failed for rerun of %s", pipelineID)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "error", pubErr)
		}
		d.Logger.Warn("failed to reset trigger lock for rerun",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
		return nil
	}

	// Delete date-scoped postrun-baseline so re-run captures fresh baseline.
	if cfg.PostRun != nil {
		if err := d.Store.DeleteSensor(ctx, pipelineID, "postrun-baseline#"+date); err != nil {
			d.Logger.Warn("failed to delete postrun-baseline sensor", "error", err, "pipeline", pipelineID, "date", date)
		}
	}

	// Write rerun record AFTER lock is confirmed.
	if _, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, reason, ""); err != nil {
		// Lock acquired but write failed — release lock to avoid deadlock.
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); relErr != nil {
			d.Logger.Warn("failed to release lock after rerun write failure", "error", relErr)
		}
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	// Publish acceptance event only after lock and rerun record confirmed.
	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventRerunAccepted, "", 0, ""); err != nil {
		d.Logger.Warn("failed to write rerun-accepted joblog", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
	}

	if pubErr := publishEvent(ctx, d, string(types.EventRerunAccepted), pipelineID, schedule, date,
		fmt.Sprintf("rerun accepted for %s (reason: %s)", pipelineID, reason)); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunAccepted, "error", pubErr)
	}

	execName := truncateExecName(fmt.Sprintf("%s-%s-%s-%s-rerun-%d", pipelineID, schedule, date, reason, d.now().Unix()))
	if err := startSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); relErr != nil {
			d.Logger.Warn("failed to release lock after SFN start failure", "error", relErr)
		}
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

	// Dry-run pipelines evaluate retry logic but publish observation events
	// instead of executing side effects.
	if cfg.DryRun {
		return handleDryRunJobFailure(ctx, d, cfg, pipelineID, schedule, date)
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
		if err := publishEvent(ctx, d, string(types.EventRetryExhausted), pipelineID, schedule, date,
			fmt.Sprintf("retry limit reached (%d/%d) for %s", rerunCount, maxRetries, pipelineID)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRetryExhausted, "error", err)
		}

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

	// Calendar exclusion check: skip retry if the execution date is excluded.
	// Mark trigger as terminal so the lock doesn't silently expire via TTL.
	if isExcludedDate(cfg, date) {
		if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
			d.Logger.WarnContext(ctx, "failed to set trigger status after calendar exclusion", "error", err)
		}
		if pubErr := publishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, schedule, date,
			fmt.Sprintf("job failure retry skipped for %s: execution date %s excluded by calendar", pipelineID, date)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
		return nil
	}

	// Under retry limit — write rerun record and restart the pipeline.
	attempt, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, "job-fail-retry", jobEvent)
	if err != nil {
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	acquired, err := d.Store.ResetTriggerLock(ctx, pipelineID, schedule, date, ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("reset trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		d.Logger.Warn("failed to reset trigger lock, skipping rerun",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
		return nil
	}

	// Use a unique execution name that includes the rerun attempt number.
	execName := truncateExecName(fmt.Sprintf("%s-%s-%s-rerun-%d", pipelineID, schedule, date, attempt))
	if err := startSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); relErr != nil {
			d.Logger.Warn("failed to release lock after SFN start failure", "error", relErr)
		}
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

// handleDryRunRerunRequest evaluates all rerun checks (calendar, limit,
// circuit breaker) and publishes observation events instead of executing
// side effects. Mirrors the production handleRerunRequest logic.
func handleDryRunRerunRequest(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string, record events.DynamoDBEventRecord) error {
	// Calendar exclusion check.
	if isExcludedDate(cfg, date) {
		if pubErr := publishEvent(ctx, d, string(types.EventDryRunRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: rerun rejected for %s: execution date %s excluded by calendar", pipelineID, date),
			map[string]interface{}{
				"reason": "excluded by calendar",
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRerunRejected, "error", pubErr)
		}
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

	// Rerun limit check.
	var budget int
	var sources []string
	switch reason {
	case "data-drift", "late-data":
		budget = types.IntOrDefault(cfg.Job.MaxDriftReruns, 1)
		sources = []string{"data-drift", "late-data"}
	default:
		budget = types.IntOrDefault(cfg.Job.MaxManualReruns, 1)
		sources = []string{reason}
	}

	count, err := d.Store.CountRerunsBySource(ctx, pipelineID, schedule, date, sources)
	if err != nil {
		return fmt.Errorf("dry-run: count reruns by source for %q: %w", pipelineID, err)
	}

	if count >= budget {
		if pubErr := publishEvent(ctx, d, string(types.EventDryRunRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: rerun rejected for %s: limit exceeded (%d/%d)", pipelineID, count, budget),
			map[string]interface{}{
				"reason":     "limit exceeded",
				"rerunCount": count,
				"budget":     budget,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRerunRejected, "error", pubErr)
		}
		return nil
	}

	// Circuit breaker (sensor freshness).
	cbStatus := "passed"
	job, err := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("dry-run: get latest job event for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	if job == nil {
		cbStatus = "skipped (no job history)"
	} else if job.Event == types.JobEventSuccess {
		fresh, freshErr := checkSensorFreshness(ctx, d, pipelineID, job.SK)
		if freshErr != nil {
			return fmt.Errorf("dry-run: check sensor freshness for %q: %w", pipelineID, freshErr)
		}
		if !fresh {
			if pubErr := publishEvent(ctx, d, string(types.EventDryRunRerunRejected), pipelineID, schedule, date,
				fmt.Sprintf("dry-run: rerun rejected for %s: previous run succeeded and no sensor data has changed", pipelineID),
				map[string]interface{}{
					"reason":         "circuit breaker",
					"circuitBreaker": "rejected",
				}); pubErr != nil {
				d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRerunRejected, "error", pubErr)
			}
			return nil
		}
	}

	// All checks pass — publish would-rerun event.
	if pubErr := publishEvent(ctx, d, string(types.EventDryRunWouldRerun), pipelineID, schedule, date,
		fmt.Sprintf("dry-run: would rerun %s (reason: %s)", pipelineID, reason),
		map[string]interface{}{
			"reason":         reason,
			"circuitBreaker": cbStatus,
			"rerunCount":     count,
			"budget":         budget,
		}); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunWouldRerun, "error", pubErr)
	}

	d.Logger.Info("dry-run: would rerun",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason)
	return nil
}

// handleDryRunJobFailure evaluates retry logic for a dry-run pipeline and
// publishes observation events instead of executing side effects.
func handleDryRunJobFailure(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string) error {
	maxRetries := cfg.Job.MaxRetries

	// Read latest job event for failure category (read-only).
	latestJob, jobErr := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if jobErr != nil {
		d.Logger.WarnContext(ctx, "dry-run: could not read latest job event for failure category",
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
		return fmt.Errorf("dry-run: count reruns for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}

	if rerunCount >= maxRetries {
		if pubErr := publishEvent(ctx, d, string(types.EventDryRunRetryExhausted), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: retry limit reached (%d/%d) for %s", rerunCount, maxRetries, pipelineID),
			map[string]interface{}{
				"retries":    rerunCount,
				"maxRetries": maxRetries,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRetryExhausted, "error", pubErr)
		}
		return nil
	}

	// Calendar exclusion check.
	if isExcludedDate(cfg, date) {
		if pubErr := publishEvent(ctx, d, string(types.EventDryRunRetryExhausted), pipelineID, schedule, date,
			fmt.Sprintf("dry-run: retry skipped for %s: execution date %s excluded by calendar", pipelineID, date),
			map[string]interface{}{
				"reason":     "excluded by calendar",
				"retries":    rerunCount,
				"maxRetries": maxRetries,
			}); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunRetryExhausted, "error", pubErr)
		}
		return nil
	}

	// Under budget — publish would-retry event.
	if pubErr := publishEvent(ctx, d, string(types.EventDryRunWouldRetry), pipelineID, schedule, date,
		fmt.Sprintf("dry-run: would retry %s (%d/%d)", pipelineID, rerunCount, maxRetries),
		map[string]interface{}{
			"retries":    rerunCount,
			"maxRetries": maxRetries,
		}); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventDryRunWouldRetry, "error", pubErr)
	}

	d.Logger.Info("dry-run: would retry",
		"pipelineId", pipelineID, "schedule", schedule, "date", date,
		"retries", rerunCount, "maxRetries", maxRetries)
	return nil
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

		// Normalize epoch: if ts looks like seconds (< 1e12), convert to millis.
		// Epoch millis won't be < 1e12 until ~2001, and epoch seconds won't
		// exceed 1e12 until ~33658 CE.
		if ts > 0 && ts < 1e12 {
			ts *= 1000
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
	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventLateDataArrival, "", 0,
		"sensor updated after pipeline completed successfully"); err != nil {
		d.Logger.Warn("failed to write late-data-arrival joblog", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
	}

	if err := publishEvent(ctx, d, string(types.EventLateDataArrival), pipelineID, schedule, date,
		fmt.Sprintf("late data arrival for %s: sensor updated after job completion", pipelineID)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventLateDataArrival, "error", err)
	}

	// Trigger a re-run — circuit breaker in handleRerunRequest will validate sensor freshness.
	if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, schedule, date, "late-data"); writeErr != nil {
		d.Logger.WarnContext(ctx, "failed to write rerun request on late data", "pipelineId", pipelineID, "error", writeErr)
	}

	return nil
}
