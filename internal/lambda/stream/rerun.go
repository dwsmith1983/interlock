package stream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleRerunRequest processes a RERUN_REQUEST# stream record.
func handleRerunRequest(ctx context.Context, d *lambda.Deps, pk, sk string, record events.DynamoDBEventRecord) error {
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		return fmt.Errorf("unexpected PK format: %q", pk)
	}

	// A bad row is skipped, not returned as an error: a retry cannot fix it
	// and would hold up the rest of the shard.
	schedule, date, err := parseRerunRequestSK(sk)
	if err != nil {
		d.Logger.Warn("skipping rerun request", "pipelineId", pipelineID, "error", err)
		return nil
	}

	cfg, err := lambda.GetValidatedConfig(ctx, d, pipelineID)
	if err != nil {
		return fmt.Errorf("load config for %q: %w", pipelineID, err)
	}
	if cfg == nil {
		d.Logger.Warn("no config found for pipeline, skipping rerun request", "pipelineId", pipelineID)
		return nil
	}
	if want := lambda.ResolveScheduleID(cfg); schedule != want {
		d.Logger.Warn("skipping rerun request for a schedule the pipeline does not use",
			"pipelineId", pipelineID, "schedule", schedule, "want", want)
		return nil
	}

	if cfg.DryRun {
		return handleDryRunRerunRequest(ctx, d, cfg, pipelineID, schedule, date, record)
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

	// An automatic request is re-checked: the post-run watch window may have
	// ended since it was written.
	if reason == "late-data" || reason == "data-drift" {
		tr, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
		if err != nil {
			return fmt.Errorf("re-check rerun request: %w", err)
		}
		if tr != nil && tr.Status == types.TriggerStatusCompleted && !lambda.PostRunWatched(cfg, tr, d.Now()) {
			d.Logger.InfoContext(ctx, "rerun request skipped: post-run watch window has ended",
				"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason, "completedAt", tr.CompletedAt)
			return nil
		}
	}

	// Calendar exclusion check (execution date).
	if lambda.IsExcludedDate(cfg, date) {
		if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date, types.JobEventRerunRejected, "", 0, "excluded by calendar"); err != nil {
			d.Logger.Warn("failed to write rerun-rejected joblog for calendar exclusion", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
		}
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, schedule, date,
			fmt.Sprintf("rerun blocked for %s: execution date %s excluded by calendar", pipelineID, date)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
		return nil
	}

	// Rerun limit check.
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
		if err := lambda.PublishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, limitLabel)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunRejected, "error", err)
		}
		d.Logger.Info("rerun request rejected (limit exceeded)",
			"pipelineId", pipelineID, "schedule", schedule, "date", date,
			"reason", reason, "count", count, "budget", budget)
		return nil
	}

	// Circuit breaker (sensor freshness).
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
		if err := lambda.PublishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
			fmt.Sprintf("rerun rejected for %s: %s", pipelineID, rejectReason)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunRejected, "error", err)
		}
		d.Logger.Info("rerun request rejected",
			"pipelineId", pipelineID, "schedule", schedule, "date", date,
			"reason", rejectReason)
		return nil
	}

	// Acceptance: acquire lock FIRST (before writing rerun).
	acquired, err := d.Store.ResetTriggerLock(ctx, pipelineID, schedule, date, lambda.ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("reset trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventInfraFailure), pipelineID, schedule, date,
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
	attempt, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, reason, "")
	if err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); relErr != nil {
			d.Logger.Warn("failed to release lock after rerun write failure", "error", relErr)
		}
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventRerunAccepted, "", 0, ""); err != nil {
		d.Logger.Warn("failed to write rerun-accepted joblog", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
	}

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventRerunAccepted), pipelineID, schedule, date,
		fmt.Sprintf("rerun accepted for %s (reason: %s)", pipelineID, reason)); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunAccepted, "error", pubErr)
	}

	// The attempt keeps two reruns started in the same second apart.
	execName := fmt.Sprintf("%s-%s-%s-rerun%d-%d", pipelineID, schedule, date, attempt, d.Now().Unix())
	if err := lambda.StartSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); relErr != nil {
			d.Logger.Warn("failed to release lock after SFN start failure", "error", relErr)
		}
		return fmt.Errorf("start SFN rerun for %q: %w", pipelineID, err)
	}

	d.Logger.Info("started rerun",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason)
	return nil
}

// parseRerunRequestSK extracts schedule and date from a RERUN_REQUEST# sort
// key. Anyone who can write the table can write one, and the date goes into
// the execution name, so it must be a valid execution date. The caller checks
// the schedule against the pipeline's config.
func parseRerunRequestSK(sk string) (schedule, date string, err error) {
	trimmed := strings.TrimPrefix(sk, "RERUN_REQUEST#")
	parts := strings.SplitN(trimmed, "#", 2)
	if len(parts) < 2 || !lambda.IsValidExecutionDate(parts[1]) {
		return "", "", fmt.Errorf("invalid RERUN_REQUEST SK: %q", sk)
	}
	return parts[0], parts[1], nil
}

// checkSensorFreshness determines whether any sensor data has been updated
// after the given job completed.
func checkSensorFreshness(ctx context.Context, d *lambda.Deps, pipelineID, jobSK string) (bool, error) {
	parts := strings.Split(jobSK, "#")
	if len(parts) < 4 {
		return true, nil
	}
	jobTimestamp, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return true, nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return false, fmt.Errorf("get sensors for %q: %w", pipelineID, err)
	}
	if len(sensors) == 0 {
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

		if ts > 0 && ts < 1e12 {
			ts *= 1000
		}

		if ts > jobTimestamp {
			return true, nil
		}
	}

	if !hasAnyUpdatedAt {
		return true, nil
	}

	return false, nil
}

// checkLateDataArrival detects sensor updates after a pipeline has completed.
// Only a date inside its post-run watch window at now is acted on; after the
// window the sensor is ignored.
func checkLateDataArrival(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string, now time.Time) error {
	trigger, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
	if err != nil || trigger == nil {
		return err
	}

	if trigger.Status != types.TriggerStatusCompleted {
		return nil
	}

	if !lambda.PostRunWatched(cfg, trigger, now) {
		d.Logger.InfoContext(ctx, "sensor ignored: post-run watch window has ended",
			"pipelineId", pipelineID, "schedule", schedule, "date", date, "completedAt", trigger.CompletedAt)
		return nil
	}

	job, err := d.Store.GetLatestJobEvent(ctx, pipelineID, schedule, date)
	if err != nil || job == nil {
		return err
	}

	if job.Event != types.JobEventSuccess {
		return nil
	}

	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventLateDataArrival, "", 0,
		"sensor updated after pipeline completed successfully"); err != nil {
		d.Logger.Warn("failed to write late-data-arrival joblog", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventLateDataArrival), pipelineID, schedule, date,
		fmt.Sprintf("late data arrival for %s: sensor updated after job completion", pipelineID)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventLateDataArrival, "error", err)
	}

	if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, schedule, date, "late-data"); writeErr != nil {
		d.Logger.WarnContext(ctx, "failed to write rerun request on late data", "pipelineId", pipelineID, "error", writeErr)
	}

	return nil
}
