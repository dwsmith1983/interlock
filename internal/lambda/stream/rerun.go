package stream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
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

	// The date's trigger row as it was before this request: nil when the date
	// has none.
	tr, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("read trigger for rerun request of %q: %w", pipelineID, err)
	}

	// An automatic request is re-checked: automatic reruns (bounded by the
	// post-run watch window and job.maxAutoRerunAge) may have ended since it
	// was written.
	automatic := reason == "late-data" || reason == "data-drift"
	if automatic && tr != nil && tr.Status == types.TriggerStatusCompleted && !lambda.AutoRerunAllowed(cfg, tr, d.Now()) {
		endedBy := logAutoRerunEnded(ctx, d, cfg, tr, "rerun request skipped: automatic reruns have ended", pipelineID, schedule, date, reason)
		return requireManualRerunOnRequest(ctx, d, cfg, pipelineID, schedule, date, tr, reason, endedBy, record)
	}

	// Calendar exclusion check (execution date).
	if lambda.IsExcludedDate(cfg, date) {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, schedule, date,
			fmt.Sprintf("rerun blocked for %s: execution date %s excluded by calendar", pipelineID, date),
			map[string]interface{}{"reason": reason, "why": "excluded by calendar"}); pubErr != nil {
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
		rejectRerun(ctx, d, pipelineID, schedule, date, reason, limitLabel)
		return nil
	}

	// A rerun beside a run still in progress would launch the job twice, and
	// whichever finished last would decide the date's status.
	if tr != nil && tr.Status == types.TriggerStatusRunning {
		rejectRerun(ctx, d, pipelineID, schedule, date, reason, runInProgress(date))
		return nil
	}

	breaker, err := runCircuitBreaker(ctx, d, pipelineID, schedule, date, reason)
	if err != nil {
		return err
	}
	if breaker == breakerRejected {
		rejectRerun(ctx, d, pipelineID, schedule, date, reason, "previous run succeeded and no sensor data has changed")
		return nil
	}

	// Acceptance: acquire lock FIRST (before writing rerun). The reset refuses
	// a RUNNING row, so a run that took the date after the read above is
	// rejected here like one the read saw.
	acquired, err := d.Store.ResetTriggerLock(ctx, pipelineID, schedule, date, lambda.ResolveTriggerLockTTL())
	if err != nil {
		return fmt.Errorf("reset trigger lock for %q: %w", pipelineID, err)
	}
	if !acquired && tr != nil {
		rejectRerun(ctx, d, pipelineID, schedule, date, reason, runInProgress(date))
		return nil
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

	// Write rerun record AFTER lock is confirmed.
	attempt, err := d.Store.WriteRerun(ctx, pipelineID, schedule, date, reason, "")
	if err != nil {
		undoRerunStart(ctx, d, pipelineID, schedule, date, tr, -1)
		return fmt.Errorf("write rerun for %q: %w", pipelineID, err)
	}

	// The record's creation time, not the clock, so a redelivered request
	// asks for the same execution. The attempt keeps two reruns started in
	// the same second apart.
	created := record.Change.ApproximateCreationDateTime.Time
	if created.Unix() <= 0 {
		created = d.Now()
	}
	execName := fmt.Sprintf("%s-%s-%s-rerun%d-%d", pipelineID, schedule, date, attempt, created.Unix())
	if err := lambda.StartSFNWithName(ctx, d, cfg, pipelineID, schedule, date, execName); err != nil {
		// Counted as started on purpose, unlike startRetry in orchestrator/complete.go.
		var exists *sfntypes.ExecutionAlreadyExists
		if !errors.As(err, &exists) {
			undoRerunStart(ctx, d, pipelineID, schedule, date, tr, attempt)
			return fmt.Errorf("start SFN rerun for %q: %w", pipelineID, err)
		}
		d.Logger.Info("rerun execution already exists, counting it as started",
			"pipelineId", pipelineID, "schedule", schedule, "date", date, "execution", execName)
	}

	// Delete date-scoped postrun-baseline so re-run captures fresh baseline.
	if cfg.PostRun != nil {
		if err := d.Store.DeleteSensor(ctx, pipelineID, "postrun-baseline#"+date); err != nil {
			d.Logger.Warn("failed to delete postrun-baseline sensor", "error", err, "pipeline", pipelineID, "date", date)
		}
	}

	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventRerunAccepted, "", 0, ""); err != nil {
		d.Logger.Warn("failed to write rerun-accepted joblog", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
	}

	if pubErr := lambda.PublishEvent(ctx, d, string(types.EventRerunAccepted), pipelineID, schedule, date,
		fmt.Sprintf("rerun accepted for %s (reason: %s)", pipelineID, reason)); pubErr != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunAccepted, "error", pubErr)
	}

	d.Logger.Info("started rerun",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason)
	return nil
}

// rerunUndoTimeout bounds undoRerunStart, which runs on a context of its own
// so an expired request context still lets it put the date back.
const rerunUndoTimeout = 5 * time.Second

// undoRerunStart puts a date back as it was before a rerun that could not
// start: it deletes the rerun row when one was written (attempt >= 0) and
// restores the trigger status tr held before the lock reset. Failures are
// logged. A row that could not be deleted still counts against the budget;
// a status that could not be restored leaves the date RUNNING until the
// watchdog's stale-trigger check closes it.
func undoRerunStart(ctx context.Context, d *lambda.Deps, pipelineID, schedule, date string, tr *types.ControlRecord, attempt int) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), rerunUndoTimeout)
	defer cancel()
	if attempt >= 0 {
		if err := d.Store.DeleteRerun(ctx, pipelineID, schedule, date, attempt); err != nil {
			d.Logger.ErrorContext(ctx, "failed to delete rerun row of a rerun that did not start",
				"pipelineId", pipelineID, "schedule", schedule, "date", date, "attempt", attempt, "error", err)
		}
	}
	if tr == nil {
		d.Logger.ErrorContext(ctx, "no earlier trigger status to restore after a rerun did not start",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
		return
	}
	if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, tr.Status); err != nil {
		d.Logger.ErrorContext(ctx, "failed to restore trigger status after a rerun did not start",
			"pipelineId", pipelineID, "schedule", schedule, "date", date, "status", tr.Status, "error", err)
	}
}

// rejectRerun records a rejected rerun request: a RERUN_REJECTED event whose
// detail carries the request's reason and why, and an info log naming both.
// It writes no job row.
func rejectRerun(ctx context.Context, d *lambda.Deps, pipelineID, schedule, date, reason, why string) {
	if err := lambda.PublishEvent(ctx, d, string(types.EventRerunRejected), pipelineID, schedule, date,
		fmt.Sprintf("rerun rejected for %s: %s", pipelineID, why),
		map[string]interface{}{"reason": reason, "why": why}); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRerunRejected, "error", err)
	}
	d.Logger.Info("rerun request rejected",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason, "why", why)
}

// runInProgress is the rejection reason for a date whose run is RUNNING.
func runInProgress(date string) string {
	return "a run is in progress for " + date
}

// Circuit breaker verdicts. The dry-run detail reports them as they are.
const (
	breakerPassed    = "passed"
	breakerRejected  = "rejected"
	breakerNoHistory = "skipped (no job history)"
	breakerAutomatic = "skipped (automatic request)"
)

// runCircuitBreaker decides whether a rerun request may run again after the
// date's last run succeeded: only when some sensor changed since. The last
// run is the newest job outcome, not a row about an earlier request or late
// data. A late-data or data-drift request skips it, since the stream-router
// wrote that request on a change it had already seen.
func runCircuitBreaker(ctx context.Context, d *lambda.Deps, pipelineID, schedule, date, reason string) (string, error) {
	if reason == "late-data" || reason == "data-drift" {
		return breakerAutomatic, nil
	}
	job, err := d.Store.GetLatestJobOutcome(ctx, pipelineID, schedule, date)
	if err != nil {
		return "", fmt.Errorf("get latest job outcome for %q/%s/%s: %w", pipelineID, schedule, date, err)
	}
	if job == nil {
		return breakerNoHistory, nil
	}
	if job.Event != types.JobEventSuccess {
		return breakerPassed, nil
	}
	fresh, err := checkSensorFreshness(ctx, d, pipelineID, job.SK)
	if err != nil {
		return "", fmt.Errorf("check sensor freshness for %q: %w", pipelineID, err)
	}
	if !fresh {
		return breakerRejected, nil
	}
	return breakerPassed, nil
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
// after the given job completed. An updatedAt that validation.ParseTimestamp
// cannot read counts as absent.
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

	jobTime := time.UnixMilli(jobTimestamp)
	sawTimestamp := false
	for key, data := range sensors {
		updatedAt, ok := data["updatedAt"]
		if !ok {
			continue
		}
		ts, ok := validation.ParseTimestamp(updatedAt)
		if !ok {
			d.Logger.Warn("ignoring sensor updatedAt that is not a timestamp",
				"pipelineId", pipelineID, "sensor", key)
			continue
		}
		sawTimestamp = true

		if ts.After(jobTime) {
			return true, nil
		}
	}

	if !sawTimestamp {
		return true, nil
	}

	return false, nil
}

// checkLateDataArrival detects sensor updates after a pipeline has completed.
// Only a date inside its post-run watch window at now is acted on; after the
// window the sensor is ignored. Inside it LATE_DATA_ARRIVAL is published, and
// a late-data rerun is requested only while automatic reruns are allowed;
// after that a manual rerun is asked for, with late describing the write.
func checkLateDataArrival(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string, now time.Time, late manualRerunInput) error {
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

	job, err := d.Store.GetLatestJobOutcome(ctx, pipelineID, schedule, date)
	if err != nil || job == nil {
		return err
	}

	if job.Event != types.JobEventSuccess {
		return nil
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventLateDataArrival), pipelineID, schedule, date,
		fmt.Sprintf("late data arrival for %s: sensor updated after job completion", pipelineID)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventLateDataArrival, "error", err)
	}

	// Past the automatic rerun end no request is written, so a pending manual
	// request is not replaced by one that would be skipped.
	if !lambda.AutoRerunAllowed(cfg, trigger, now) {
		late.reason, late.source, late.now = "late-data", "sensor", now
		late.blockedBy = logAutoRerunEnded(ctx, d, cfg, trigger, "late data rerun not requested: automatic reruns have ended", pipelineID, schedule, date, late.reason)
		recordManualRerunRequired(ctx, d, cfg, pipelineID, schedule, date, trigger, job, late)
		return nil
	}
	if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, schedule, date, "late-data"); writeErr != nil {
		d.Logger.WarnContext(ctx, "failed to write rerun request on late data", "pipelineId", pipelineID, "error", writeErr)
	}

	return nil
}

// endedByNoCompletion is the endedBy label of a row without a parseable
// completedAt.
const endedByNoCompletion = "no-completion-time"

// logAutoRerunEnded logs that an automatic rerun of a completed date was not
// requested or started because its automatic reruns have ended, naming the
// bound that ended them: postRun.watchWindow, job.maxAutoRerunAge, or
// endedByNoCompletion. It returns that label.
func logAutoRerunEnded(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, tr *types.ControlRecord, msg, pipelineID, schedule, date, reason string) string {
	end := lambda.AutoRerunEnd(cfg, tr)
	var endedBy string
	switch {
	case end.IsZero():
		endedBy = endedByNoCompletion
	case end.Equal(lambda.PostRunWatchEnd(cfg, tr)):
		endedBy = "postRun.watchWindow"
	default:
		endedBy = "job.maxAutoRerunAge"
	}
	d.Logger.InfoContext(ctx, msg, "pipelineId", pipelineID, "schedule", schedule, "date", date, "reason", reason,
		"completedAt", tr.CompletedAt, "maxAutoRerunAge", lambda.MaxAutoRerunAge(cfg).String(), "endedBy", endedBy)
	return endedBy
}
