package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/internal/metrics"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Watcher loop defaults.
const (
	defaultEvalTimeoutSec     = 30
	lockTTLBufferSec          = 30
	defaultMonitoringDuration = 2 * time.Hour
)

// runTransition describes a CAS run state transition.
type runTransition struct {
	RunID           string
	ExpectedVersion int
	NewStatus       types.RunStatus
	Metadata        map[string]interface{}
	UpdatedAt       time.Time
}

// transitionRun performs a CAS on run state, logging failures instead of silently discarding.
func (w *Watcher) transitionRun(ctx context.Context, t runTransition) (bool, error) {
	run, err := w.provider.GetRunState(ctx, t.RunID)
	if err != nil {
		return false, fmt.Errorf("getting run state: %w", err)
	}

	newRun := *run
	newRun.Status = t.NewStatus
	newRun.Version = t.ExpectedVersion + 1
	newRun.UpdatedAt = t.UpdatedAt
	if t.Metadata != nil {
		if newRun.Metadata == nil {
			newRun.Metadata = make(map[string]interface{})
		}
		for k, v := range t.Metadata {
			newRun.Metadata[k] = v
		}
	}

	ok, err := w.provider.CompareAndSwapRunState(ctx, t.RunID, t.ExpectedVersion, newRun)
	if err != nil {
		w.logger.Error("CAS failed", "runID", t.RunID, "expectedVersion", t.ExpectedVersion, "newStatus", t.NewStatus, "error", err)
		return false, err
	}
	if !ok {
		w.logger.Warn("CAS conflict", "runID", t.RunID, "expectedVersion", t.ExpectedVersion, "newStatus", t.NewStatus)
		return false, nil
	}
	return true, nil
}

// runLogUpdate describes fields to update on a RunLog entry.
type runLogUpdate struct {
	PipelineID      string
	Date            string
	ScheduleID      string
	RunID           string
	Status          types.RunStatus
	FailureMessage  string
	FailureCategory types.FailureCategory
	SetCompletedAt  bool
	UpdatedAt       time.Time
}

// updateRunLog fetches the run log, checks RunID match, and updates fields.
func (w *Watcher) updateRunLog(ctx context.Context, u runLogUpdate) error {
	scheduleID := u.ScheduleID
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	runLog, err := w.provider.GetRunLog(ctx, u.PipelineID, u.Date, scheduleID)
	if err != nil {
		return fmt.Errorf("getting run log: %w", err)
	}
	if runLog == nil || runLog.RunID != u.RunID {
		return nil
	}
	runLog.Status = u.Status
	if u.FailureMessage != "" {
		runLog.FailureMessage = u.FailureMessage
	}
	if u.FailureCategory != "" {
		runLog.FailureCategory = u.FailureCategory
	}
	if u.SetCompletedAt {
		t := u.UpdatedAt
		runLog.CompletedAt = &t
	}
	runLog.UpdatedAt = u.UpdatedAt
	if err := w.provider.PutRunLog(ctx, *runLog); err != nil {
		return fmt.Errorf("putting run log: %w", err)
	}
	return nil
}

// computeLockTTL calculates a lock TTL based on the number and timeout of traits.
func computeLockTTL(pipeline types.PipelineConfig, fallback time.Duration) time.Duration {
	maxTimeout := defaultEvalTimeoutSec
	count := 0
	for _, tc := range pipeline.Traits {
		count++
		if tc.Timeout > maxTimeout {
			maxTimeout = tc.Timeout
		}
	}
	if count == 0 {
		count = 1
	}
	computed := time.Duration(maxTimeout*count+lockTTLBufferSec) * time.Second
	if computed < fallback*2 {
		return fallback * 2
	}
	return computed
}

// tick processes a single pipeline evaluation cycle for a specific schedule.
func (w *Watcher) tick(ctx context.Context, pipeline types.PipelineConfig, interval time.Duration, sched types.ScheduleConfig) {
	now := time.Now()

	if !w.acquireLock(ctx, pipeline, sched, interval) {
		return
	}
	defer w.releaseLock(ctx, pipeline, sched)

	if w.handleActiveRun(ctx, pipeline, sched, now) {
		return
	}

	runLog, proceed := w.checkRunLog(ctx, pipeline, sched, now)
	if !proceed {
		return
	}

	result, ok := w.evaluateReadiness(ctx, pipeline, sched, now)
	if !ok || result.Status != types.Ready {
		return
	}

	w.triggerPipeline(ctx, pipeline, sched, runLog, now)
}

// acquireLock attempts to acquire the evaluation lock for the pipeline and schedule.
func (w *Watcher) acquireLock(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, interval time.Duration) bool {
	lockKey := schedule.LockKey(pipeline.Name, sched.Name)
	acquired, err := w.provider.AcquireLock(ctx, lockKey, computeLockTTL(pipeline, interval))
	if err != nil {
		w.logger.Error("failed to acquire lock", "pipeline", pipeline.Name, "schedule", sched.Name, "error", err)
		return false
	}
	return acquired
}

// releaseLock releases the evaluation lock for the pipeline and schedule.
func (w *Watcher) releaseLock(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig) {
	lockKey := schedule.LockKey(pipeline.Name, sched.Name)
	if err := w.provider.ReleaseLock(ctx, lockKey); err != nil {
		w.logger.Error("failed to release lock", "pipeline", pipeline.Name, "schedule", sched.Name, "error", err)
	}
}

// handleActiveRun checks for active (non-terminal) runs for this schedule and handles them.
// Returns true if an active run was found (caller should return).
//
// Uses RunLog lookups (today + yesterday) to find the active RunID in O(1) per
// date, then fetches a single RunState by ID. This avoids the previous approach
// of scanning up to 20 recent runs with ListRuns.
func (w *Watcher) handleActiveRun(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, now time.Time) bool {
	today := now.Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")

	for _, date := range []string{today, yesterday} {
		runLog, err := w.provider.GetRunLog(ctx, pipeline.Name, date, sched.Name)
		if err != nil {
			w.logger.Error("failed to get run log", "pipeline", pipeline.Name, "date", date, "schedule", sched.Name, "error", err)
			continue
		}
		if runLog == nil || runLog.RunID == "" || lifecycle.IsTerminal(runLog.Status) {
			continue
		}

		run, err := w.provider.GetRunState(ctx, runLog.RunID)
		if err != nil {
			w.logger.Error("failed to get run state", "pipeline", pipeline.Name, "runID", runLog.RunID, "error", err)
			continue
		}
		if run == nil || lifecycle.IsTerminal(run.Status) {
			continue
		}

		if run.Status == types.RunCompletedMonitoring {
			w.checkMonitoring(ctx, pipeline, sched, *run, now)
		} else {
			w.checkRunStatus(ctx, pipeline, sched, *run, now)
			w.checkCompletionSLA(ctx, pipeline, sched, *run, now)
		}
		return true
	}
	return false
}

// checkRunLog examines today's run log for the schedule and decides whether to proceed.
// Returns the run log (may be nil) and whether to proceed.
func (w *Watcher) checkRunLog(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, now time.Time) (*types.RunLogEntry, bool) {
	today := now.Format("2006-01-02")
	runLog, err := w.provider.GetRunLog(ctx, pipeline.Name, today, sched.Name)
	if err != nil {
		w.logger.Error("failed to get run log", "pipeline", pipeline.Name, "schedule", sched.Name, "error", err)
		return nil, false
	}

	if runLog == nil {
		return nil, true
	}

	// Already succeeded today
	if runLog.Status == types.RunCompleted {
		return runLog, false
	}

	retryPolicy := w.retryPolicyFor(pipeline)

	// Max retries exhausted
	if runLog.AttemptNumber >= retryPolicy.MaxAttempts {
		if !runLog.AlertSent {
			w.fireAlert(types.Alert{
				Level:      types.AlertLevelError,
				PipelineID: pipeline.Name,
				Message:    fmt.Sprintf("Pipeline %s exhausted %d retry attempts", pipeline.Name, retryPolicy.MaxAttempts),
				Timestamp:  now,
			})
			if err := w.appendEvent(ctx, types.Event{
				Kind:       types.EventRetryExhausted,
				PipelineID: pipeline.Name,
				Message:    fmt.Sprintf("exhausted %d attempts", retryPolicy.MaxAttempts),
				Timestamp:  now,
			}); err != nil {
				w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
			}
			runLog.AlertSent = true
			runLog.UpdatedAt = now
			if err := w.provider.PutRunLog(ctx, *runLog); err != nil {
				w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
			}
		}
		return runLog, false
	}

	// Check if failure is retryable
	if runLog.FailureCategory != "" && !IsRetryable(retryPolicy, runLog.FailureCategory) {
		if !runLog.AlertSent {
			w.fireAlert(types.Alert{
				Level:      types.AlertLevelError,
				PipelineID: pipeline.Name,
				Message:    fmt.Sprintf("Pipeline %s failed with non-retryable error: %s (%s)", pipeline.Name, runLog.FailureMessage, runLog.FailureCategory),
				Timestamp:  now,
			})
			runLog.AlertSent = true
			runLog.UpdatedAt = now
			if err := w.provider.PutRunLog(ctx, *runLog); err != nil {
				w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
			}
		}
		return runLog, false
	}

	// In backoff window?
	backoff := CalculateBackoff(retryPolicy, runLog.AttemptNumber)
	if now.Before(runLog.UpdatedAt.Add(backoff)) {
		return runLog, false
	}

	return runLog, true
}

// evaluateReadiness evaluates the pipeline and checks SLA. Returns the result and whether evaluation succeeded.
func (w *Watcher) evaluateReadiness(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, now time.Time) (*types.ReadinessResult, bool) {
	result, err := w.engine.Evaluate(ctx, pipeline.Name)
	if err != nil {
		w.logger.Error("evaluation failed", "pipeline", pipeline.Name, "error", err)
		return nil, false
	}

	if err := w.appendEvent(ctx, types.Event{
		Kind:       types.EventWatcherEvaluation,
		PipelineID: pipeline.Name,
		Status:     string(result.Status),
		Timestamp:  now,
	}); err != nil {
		w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
	}

	w.checkEvaluationSLA(ctx, pipeline, sched, result, now)
	return result, true
}

// triggerPipeline creates a run, executes the trigger, and handles the outcome.
func (w *Watcher) triggerPipeline(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, runLog *types.RunLogEntry, now time.Time) {
	today := now.Format("2006-01-02")
	retryPolicy := w.retryPolicyFor(pipeline)

	attemptNum := 1
	if runLog != nil {
		attemptNum = runLog.AttemptNumber + 1
	}

	runID := fmt.Sprintf("%s-%d", pipeline.Name, now.UnixNano())
	run := types.RunState{
		RunID:      runID,
		PipelineID: pipeline.Name,
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
		Metadata: map[string]interface{}{
			"scheduleId": sched.Name,
		},
	}
	if err := w.provider.PutRunState(ctx, run); err != nil {
		w.logger.Error("failed to create run", "pipeline", pipeline.Name, "error", err)
		return
	}

	// CAS to TRIGGERING
	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := w.provider.CompareAndSwapRunState(ctx, runID, 1, run)
	if err != nil || !ok {
		w.logger.Error("failed to CAS to TRIGGERING", "pipeline", pipeline.Name, "error", err)
		return
	}

	// Create/update run log
	entry := types.RunLogEntry{
		PipelineID:    pipeline.Name,
		Date:          today,
		ScheduleID:    sched.Name,
		Status:        types.RunTriggering,
		AttemptNumber: attemptNum,
		RunID:         runID,
		StartedAt:     now,
		UpdatedAt:     now,
	}

	// Execute trigger
	metrics.TriggersTotal.Add(1)
	triggerMeta, triggerErr := w.runner.Execute(ctx, pipeline.Trigger)

	if triggerErr != nil {
		metrics.TriggersFailed.Add(1)
		fc := trigger.ClassifyFailure(triggerErr)
		entry.Status = types.RunFailed
		entry.FailureMessage = triggerErr.Error()
		entry.FailureCategory = fc
		entry.UpdatedAt = time.Now()

		completedAt := time.Now()
		entry.CompletedAt = &completedAt

		if err := w.provider.PutRunLog(ctx, entry); err != nil {
			w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
		}

		// Update run state to FAILED
		run.Status = types.RunFailed
		run.Version = 3
		run.UpdatedAt = time.Now()
		if ok, err := w.provider.CompareAndSwapRunState(ctx, runID, 2, run); err != nil || !ok {
			w.logger.Warn("failed to CAS run to FAILED", "pipeline", pipeline.Name, "runID", runID, "error", err)
		}

		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventTriggerFailed,
			PipelineID: pipeline.Name,
			RunID:      runID,
			Status:     string(types.RunFailed),
			Message:    triggerErr.Error(),
			Timestamp:  time.Now(),
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}

		if IsRetryable(retryPolicy, fc) && attemptNum < retryPolicy.MaxAttempts {
			metrics.RetriesScheduled.Add(1)
			if err := w.appendEvent(ctx, types.Event{
				Kind:       types.EventRetryScheduled,
				PipelineID: pipeline.Name,
				RunID:      runID,
				Message:    fmt.Sprintf("retry %d/%d scheduled after %v", attemptNum+1, retryPolicy.MaxAttempts, CalculateBackoff(retryPolicy, attemptNum)),
				Timestamp:  time.Now(),
			}); err != nil {
				w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
			}
		}

		w.logger.Warn("trigger failed", "pipeline", pipeline.Name, "attempt", attemptNum, "error", triggerErr, "category", fc)
		return
	}

	// Trigger succeeded — merge metadata from trigger into run
	if triggerMeta != nil {
		if run.Metadata == nil {
			run.Metadata = make(map[string]interface{})
		}
		for k, v := range triggerMeta {
			run.Metadata[k] = v
		}
	}

	if pipeline.Trigger != nil && pipeline.Trigger.Type == types.TriggerCommand {
		if monitoringEnabled(pipeline) {
			entry.Status = types.RunCompletedMonitoring
			if run.Metadata == nil {
				run.Metadata = make(map[string]interface{})
			}
			run.Metadata["monitoringStartedAt"] = time.Now().Format(time.RFC3339)

			run.Status = types.RunCompletedMonitoring
			run.Version = 3
			run.UpdatedAt = time.Now()
			if ok, err := w.provider.CompareAndSwapRunState(ctx, runID, 2, run); err != nil || !ok {
				w.logger.Warn("failed to CAS run to COMPLETED_MONITORING", "pipeline", pipeline.Name, "runID", runID, "error", err)
			}
		} else {
			entry.Status = types.RunCompleted
			completedAt := time.Now()
			entry.CompletedAt = &completedAt

			run.Status = types.RunCompleted
			run.Version = 3
			run.UpdatedAt = time.Now()
			if ok, err := w.provider.CompareAndSwapRunState(ctx, runID, 2, run); err != nil || !ok {
				w.logger.Warn("failed to CAS run to COMPLETED", "pipeline", pipeline.Name, "runID", runID, "error", err)
			}
		}
	} else {
		entry.Status = types.RunRunning

		run.Status = types.RunRunning
		run.Version = 3
		run.UpdatedAt = time.Now()
		if ok, err := w.provider.CompareAndSwapRunState(ctx, runID, 2, run); err != nil || !ok {
			w.logger.Warn("failed to CAS run to RUNNING", "pipeline", pipeline.Name, "runID", runID, "error", err)
		}
	}

	entry.UpdatedAt = time.Now()
	if err := w.provider.PutRunLog(ctx, entry); err != nil {
		w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
	}

	if err := w.appendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: pipeline.Name,
		RunID:      runID,
		Status:     string(entry.Status),
		Timestamp:  time.Now(),
	}); err != nil {
		w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
	}

	if entry.Status == types.RunCompletedMonitoring {
		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventMonitoringStarted,
			PipelineID: pipeline.Name,
			RunID:      runID,
			Message:    fmt.Sprintf("post-completion monitoring started (duration: %s)", monitoringDuration(pipeline)),
			Timestamp:  time.Now(),
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}
	}

	w.logger.Info("trigger succeeded", "pipeline", pipeline.Name, "run", runID, "attempt", attemptNum)
}

// checkRunStatus polls the remote trigger system for run completion and transitions
// the run state accordingly. On success it moves to COMPLETED (or COMPLETED_MONITORING),
// on failure it marks FAILED with the provider state as failure message.
func (w *Watcher) checkRunStatus(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, run types.RunState, now time.Time) {
	if pipeline.Trigger == nil {
		return
	}

	var headers map[string]string
	if pipeline.Trigger != nil {
		headers = pipeline.Trigger.Headers
	}

	result, err := w.runner.CheckStatus(ctx, pipeline.Trigger.Type, run.Metadata, headers)
	if err != nil {
		w.logger.Error("failed to check run status", "pipeline", pipeline.Name, "run", run.RunID, "triggerType", pipeline.Trigger.Type, "error", err)
		return
	}

	today := now.Format("2006-01-02")

	switch result.State {
	case trigger.RunCheckSucceeded:
		targetStatus := types.RunCompleted
		var meta map[string]interface{}
		if monitoringEnabled(pipeline) {
			targetStatus = types.RunCompletedMonitoring
			meta = map[string]interface{}{
				"monitoringStartedAt": now.Format(time.RFC3339),
			}
		}

		ok, err := w.transitionRun(ctx, runTransition{
			RunID:           run.RunID,
			ExpectedVersion: run.Version,
			NewStatus:       targetStatus,
			Metadata:        meta,
			UpdatedAt:       now,
		})
		if err != nil || !ok {
			return
		}
		if err := w.updateRunLog(ctx, runLogUpdate{
			PipelineID:     pipeline.Name,
			Date:           today,
			ScheduleID:     sched.Name,
			RunID:          run.RunID,
			Status:         targetStatus,
			SetCompletedAt: targetStatus == types.RunCompleted,
			UpdatedAt:      now,
		}); err != nil {
			w.logger.Error("failed to update run log", "pipeline", pipeline.Name, "error", err)
		}
		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventCallbackReceived,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(targetStatus),
			Message:    fmt.Sprintf("run completed successfully (provider state: %s)", result.Message),
			Timestamp:  now,
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}
		if targetStatus == types.RunCompletedMonitoring {
			if err := w.appendEvent(ctx, types.Event{
				Kind:       types.EventMonitoringStarted,
				PipelineID: pipeline.Name,
				RunID:      run.RunID,
				Message:    fmt.Sprintf("post-completion monitoring started (duration: %s)", monitoringDuration(pipeline)),
				Timestamp:  now,
			}); err != nil {
				w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
			}
		}
		w.logger.Info("run completed", "pipeline", pipeline.Name, "run", run.RunID, "providerState", result.Message)

	case trigger.RunCheckFailed:
		ok, err := w.transitionRun(ctx, runTransition{
			RunID:           run.RunID,
			ExpectedVersion: run.Version,
			NewStatus:       types.RunFailed,
			UpdatedAt:       now,
		})
		if err != nil || !ok {
			return
		}
		if err := w.updateRunLog(ctx, runLogUpdate{
			PipelineID:      pipeline.Name,
			Date:            today,
			ScheduleID:      sched.Name,
			RunID:           run.RunID,
			Status:          types.RunFailed,
			FailureMessage:  fmt.Sprintf("run failed (provider state: %s)", result.Message),
			FailureCategory: types.FailureTransient,
			SetCompletedAt:  true,
			UpdatedAt:       now,
		}); err != nil {
			w.logger.Error("failed to update run log", "pipeline", pipeline.Name, "error", err)
		}
		w.fireAlert(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pipeline.Name,
			Message:    fmt.Sprintf("Run %s failed for pipeline %s (provider state: %s)", run.RunID, pipeline.Name, result.Message),
			Timestamp:  now,
		})
		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventCallbackReceived,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(types.RunFailed),
			Message:    fmt.Sprintf("run failed (provider state: %s)", result.Message),
			Timestamp:  now,
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}
		w.logger.Warn("run failed", "pipeline", pipeline.Name, "run", run.RunID, "providerState", result.Message)
	}
}

// checkEvaluationSLA fires an alert and event if the pipeline's evaluation
// deadline has passed without all traits becoming ready.
// If the schedule has a Deadline, it is used as the evaluation SLA; otherwise
// the pipeline-level EvaluationDeadline is used.
func (w *Watcher) checkEvaluationSLA(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, result *types.ReadinessResult, now time.Time) {
	if result.Status == types.Ready {
		return
	}

	var deadline time.Time
	var deadlineLabel string

	// Prefer schedule-level deadline
	if sched.Deadline != "" {
		loc := time.UTC
		if sched.Timezone != "" {
			if l, err := time.LoadLocation(sched.Timezone); err == nil {
				loc = l
			}
		}
		if t, err := parseTimeOfDay(sched.Deadline, now, loc); err == nil {
			deadline = t
			deadlineLabel = sched.Deadline
		}
	}

	// Fall back to pipeline SLA evaluation deadline
	if deadline.IsZero() {
		if pipeline.SLA == nil || pipeline.SLA.EvaluationDeadline == "" {
			return
		}
		t, err := ParseSLADeadline(pipeline.SLA.EvaluationDeadline, pipeline.SLA.Timezone, now)
		if err != nil {
			w.logger.Error("invalid SLA deadline", "pipeline", pipeline.Name, "error", err)
			return
		}
		deadline = t
		deadlineLabel = pipeline.SLA.EvaluationDeadline
	}

	if IsBreached(deadline, now) {
		metrics.SLABreaches.Add(1)
		w.fireAlert(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pipeline.Name,
			Message:    fmt.Sprintf("Pipeline %s evaluation SLA breached: not ready by %s", pipeline.Name, deadlineLabel),
			Timestamp:  now,
		})
		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventSLABreached,
			PipelineID: pipeline.Name,
			Status:     string(result.Status),
			Message:    fmt.Sprintf("evaluation deadline %s breached", deadlineLabel),
			Timestamp:  now,
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}
	}
}

// checkCompletionSLA fires an alert and event if an active run has exceeded the
// pipeline's completion deadline.
// If the schedule has a Deadline, it is used; otherwise the pipeline-level
// CompletionDeadline is used.
func (w *Watcher) checkCompletionSLA(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, run types.RunState, now time.Time) {
	if lifecycle.IsTerminal(run.Status) {
		return
	}

	var deadline time.Time
	var deadlineLabel string

	// Prefer schedule-level deadline (via scheduleDeadline helper)
	if t, ok := scheduleDeadline(sched, pipeline, now); ok {
		deadline = t
		if sched.Deadline != "" {
			deadlineLabel = sched.Deadline
		} else if pipeline.SLA != nil {
			deadlineLabel = pipeline.SLA.CompletionDeadline
		}
	} else {
		return // no deadline configured at any level
	}

	if IsBreached(deadline, now) {
		metrics.SLABreaches.Add(1)
		w.fireAlert(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pipeline.Name,
			Message:    fmt.Sprintf("Pipeline %s completion SLA breached: run %s still %s past %s", pipeline.Name, run.RunID, run.Status, deadlineLabel),
			Timestamp:  now,
		})
		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventSLABreached,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(run.Status),
			Message:    fmt.Sprintf("completion deadline %s breached", deadlineLabel),
			Timestamp:  now,
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}
	}
}

func (w *Watcher) retryPolicyFor(pipeline types.PipelineConfig) types.RetryPolicy {
	if pipeline.Retry != nil {
		return *pipeline.Retry
	}
	return DefaultRetryPolicy()
}

// checkMonitoring re-evaluates traits during the post-completion monitoring window.
// If drift is detected it creates a rerun record and alerts; otherwise, once the
// monitoring duration expires it transitions the run to COMPLETED.
func (w *Watcher) checkMonitoring(ctx context.Context, pipeline types.PipelineConfig, sched types.ScheduleConfig, run types.RunState, now time.Time) {
	duration := monitoringDuration(pipeline)

	startStr, _ := run.Metadata["monitoringStartedAt"].(string)
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		w.logger.Error("invalid monitoringStartedAt", "pipeline", pipeline.Name, "run", run.RunID, "value", startStr)
		return
	}

	today := now.Format("2006-01-02")

	// Check if monitoring window has expired
	if now.After(start.Add(duration)) {
		ok, err := w.transitionRun(ctx, runTransition{
			RunID:           run.RunID,
			ExpectedVersion: run.Version,
			NewStatus:       types.RunCompleted,
			UpdatedAt:       now,
		})
		if err != nil || !ok {
			return
		}
		if err := w.updateRunLog(ctx, runLogUpdate{
			PipelineID:     pipeline.Name,
			Date:           today,
			ScheduleID:     sched.Name,
			RunID:          run.RunID,
			Status:         types.RunCompleted,
			SetCompletedAt: true,
			UpdatedAt:      now,
		}); err != nil {
			w.logger.Error("failed to update run log", "pipeline", pipeline.Name, "error", err)
		}
		if err := w.appendEvent(ctx, types.Event{
			Kind:       types.EventMonitoringCompleted,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(types.RunCompleted),
			Message:    "monitoring window expired with no drift detected",
			Timestamp:  now,
		}); err != nil {
			w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
		}
		w.logger.Info("monitoring completed, no drift", "pipeline", pipeline.Name, "run", run.RunID)
		return
	}

	// Re-evaluate traits
	result, err := w.engine.Evaluate(ctx, pipeline.Name)
	if err != nil {
		w.logger.Error("monitoring evaluation failed", "pipeline", pipeline.Name, "error", err)
		return
	}

	// No drift — traits still pass
	if result.Status == types.Ready {
		return
	}

	// Drift detected
	metrics.MonitoringDriftDetected.Add(1)

	rerunID := fmt.Sprintf("rerun-%s-%d", pipeline.Name, now.UnixNano())
	rerun := types.RerunRecord{
		RerunID:       rerunID,
		PipelineID:    pipeline.Name,
		OriginalDate:  today,
		OriginalRunID: run.RunID,
		Reason:        "monitoring_drift_detected",
		Description:   fmt.Sprintf("trait drift detected during post-completion monitoring: blocking traits %v", result.Blocking),
		Status:        types.RunPending,
		RequestedAt:   now,
		Metadata: map[string]interface{}{
			"blockingTraits": result.Blocking,
		},
	}
	if err := w.provider.PutRerun(ctx, rerun); err != nil {
		w.logger.Error("failed to store rerun record", "pipeline", pipeline.Name, "rerunID", rerunID, "error", err)
	}

	if err := w.appendEvent(ctx, types.Event{
		Kind:       types.EventMonitoringDrift,
		PipelineID: pipeline.Name,
		RunID:      run.RunID,
		Message:    fmt.Sprintf("trait drift detected, rerun %s created", rerunID),
		Details: map[string]interface{}{
			"rerunId":        rerunID,
			"blockingTraits": result.Blocking,
		},
		Timestamp: now,
	}); err != nil {
		w.logger.Error("failed to append event", "pipeline", pipeline.Name, "error", err)
	}

	w.fireAlert(types.Alert{
		Level:      types.AlertLevelWarning,
		PipelineID: pipeline.Name,
		Message:    fmt.Sprintf("Pipeline %s: trait drift detected during monitoring, rerun %s requested", pipeline.Name, rerunID),
		Details: map[string]interface{}{
			"rerunId":        rerunID,
			"blockingTraits": result.Blocking,
		},
		Timestamp: now,
	})

	// Transition run to COMPLETED — monitoring duty done; rerun is a separate lifecycle
	_, _ = w.transitionRun(ctx, runTransition{
		RunID:           run.RunID,
		ExpectedVersion: run.Version,
		NewStatus:       types.RunCompleted,
		UpdatedAt:       now,
	})
	if err := w.updateRunLog(ctx, runLogUpdate{
		PipelineID:     pipeline.Name,
		Date:           today,
		ScheduleID:     sched.Name,
		RunID:          run.RunID,
		Status:         types.RunCompleted,
		SetCompletedAt: true,
		UpdatedAt:      now,
	}); err != nil {
		w.logger.Error("failed to update run log", "pipeline", pipeline.Name, "error", err)
	}

	w.logger.Info("monitoring drift detected, rerun created", "pipeline", pipeline.Name, "run", run.RunID, "rerunID", rerunID)
}

func (w *Watcher) appendEvent(ctx context.Context, event types.Event) error {
	if err := w.provider.AppendEvent(ctx, event); err != nil {
		return fmt.Errorf("appending %s event for %s: %w", event.Kind, event.PipelineID, err)
	}
	return nil
}

func monitoringEnabled(p types.PipelineConfig) bool {
	return p.Watch != nil && p.Watch.Monitoring != nil && p.Watch.Monitoring.Enabled
}

func monitoringDuration(p types.PipelineConfig) time.Duration {
	if p.Watch != nil && p.Watch.Monitoring != nil && p.Watch.Monitoring.Duration != "" {
		if d, err := time.ParseDuration(p.Watch.Monitoring.Duration); err == nil && d > 0 {
			return d
		}
	}
	return defaultMonitoringDuration
}
