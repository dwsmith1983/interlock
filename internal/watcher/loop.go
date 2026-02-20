package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/interlock-systems/interlock/internal/lifecycle"
	"github.com/interlock-systems/interlock/internal/trigger"
	"github.com/interlock-systems/interlock/pkg/types"
)

// tick processes a single pipeline evaluation cycle.
func (w *Watcher) tick(ctx context.Context, pipeline types.PipelineConfig, interval time.Duration) {
	lockKey := "eval:" + pipeline.Name
	now := time.Now()
	today := now.Format("2006-01-02")

	// Step 1: Acquire lock to prevent double-evaluation
	acquired, err := w.provider.AcquireLock(ctx, lockKey, interval*2)
	if err != nil {
		w.logger.Error("failed to acquire lock", "pipeline", pipeline.Name, "error", err)
		return
	}
	if !acquired {
		return // another instance is handling this pipeline
	}
	defer func() {
		if err := w.provider.ReleaseLock(ctx, lockKey); err != nil {
			w.logger.Error("failed to release lock", "pipeline", pipeline.Name, "error", err)
		}
	}()

	// Step 2: Check for active (non-terminal) runs
	runs, err := w.provider.ListRuns(ctx, pipeline.Name, 1)
	if err != nil {
		w.logger.Error("failed to list runs", "pipeline", pipeline.Name, "error", err)
		return
	}
	if len(runs) > 0 && !lifecycle.IsTerminal(runs[0].Status) {
		// Active run exists — only check completion SLA
		w.checkCompletionSLA(ctx, pipeline, runs[0], now)
		return
	}

	// Step 3: Check run log for today
	runLog, err := w.provider.GetRunLog(ctx, pipeline.Name, today)
	if err != nil {
		w.logger.Error("failed to get run log", "pipeline", pipeline.Name, "error", err)
		return
	}

	retryPolicy := w.retryPolicyFor(pipeline)

	if runLog != nil {
		// Already succeeded today
		if runLog.Status == types.RunCompleted {
			return
		}

		// Max retries exhausted
		if runLog.AttemptNumber >= retryPolicy.MaxAttempts {
			if !runLog.AlertSent {
				w.fireAlert(types.Alert{
					Level:      types.AlertLevelError,
					PipelineID: pipeline.Name,
					Message:    fmt.Sprintf("Pipeline %s exhausted %d retry attempts", pipeline.Name, retryPolicy.MaxAttempts),
					Timestamp:  now,
				})
				w.appendEvent(ctx, types.Event{
					Kind:       types.EventRetryExhausted,
					PipelineID: pipeline.Name,
					Message:    fmt.Sprintf("exhausted %d attempts", retryPolicy.MaxAttempts),
					Timestamp:  now,
				})
				runLog.AlertSent = true
				runLog.UpdatedAt = now
				_ = w.provider.PutRunLog(ctx, *runLog)
			}
			return
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
				_ = w.provider.PutRunLog(ctx, *runLog)
			}
			return
		}

		// In backoff window?
		backoff := CalculateBackoff(retryPolicy, runLog.AttemptNumber)
		if now.Before(runLog.UpdatedAt.Add(backoff)) {
			return
		}
	}

	// Step 4: Evaluate pipeline
	result, err := w.engine.Evaluate(ctx, pipeline.Name)
	if err != nil {
		w.logger.Error("evaluation failed", "pipeline", pipeline.Name, "error", err)
		return
	}

	w.appendEvent(ctx, types.Event{
		Kind:       types.EventWatcherEvaluation,
		PipelineID: pipeline.Name,
		Status:     string(result.Status),
		Timestamp:  now,
	})

	// Step 5: Check evaluation SLA
	w.checkEvaluationSLA(ctx, pipeline, result, now)

	// Step 6: If not ready, skip trigger
	if result.Status != types.Ready {
		return
	}

	// Step 7: Trigger pipeline
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
		Status:        types.RunTriggering,
		AttemptNumber: attemptNum,
		RunID:         runID,
		StartedAt:     now,
		UpdatedAt:     now,
	}

	// Execute trigger
	triggerErr := trigger.Execute(ctx, pipeline.Trigger)

	if triggerErr != nil {
		fc := trigger.ClassifyFailure(triggerErr)
		entry.Status = types.RunFailed
		entry.FailureMessage = triggerErr.Error()
		entry.FailureCategory = fc
		entry.UpdatedAt = time.Now()

		completedAt := time.Now()
		entry.CompletedAt = &completedAt

		_ = w.provider.PutRunLog(ctx, entry)

		// Update run state to FAILED
		run.Status = types.RunFailed
		run.Version = 3
		run.UpdatedAt = time.Now()
		_, _ = w.provider.CompareAndSwapRunState(ctx, runID, 2, run)

		w.appendEvent(ctx, types.Event{
			Kind:       types.EventTriggerFailed,
			PipelineID: pipeline.Name,
			RunID:      runID,
			Status:     string(types.RunFailed),
			Message:    triggerErr.Error(),
			Timestamp:  time.Now(),
		})

		if IsRetryable(retryPolicy, fc) && attemptNum < retryPolicy.MaxAttempts {
			w.appendEvent(ctx, types.Event{
				Kind:       types.EventRetryScheduled,
				PipelineID: pipeline.Name,
				RunID:      runID,
				Message:    fmt.Sprintf("retry %d/%d scheduled after %v", attemptNum+1, retryPolicy.MaxAttempts, CalculateBackoff(retryPolicy, attemptNum)),
				Timestamp:  time.Now(),
			})
		}

		w.logger.Warn("trigger failed", "pipeline", pipeline.Name, "attempt", attemptNum, "error", triggerErr, "category", fc)
		return
	}

	// Trigger succeeded
	if pipeline.Trigger != nil && pipeline.Trigger.Type == types.TriggerCommand {
		entry.Status = types.RunCompleted
		completedAt := time.Now()
		entry.CompletedAt = &completedAt

		run.Status = types.RunCompleted
		run.Version = 3
		run.UpdatedAt = time.Now()
		_, _ = w.provider.CompareAndSwapRunState(ctx, runID, 2, run)
	} else {
		entry.Status = types.RunRunning

		run.Status = types.RunRunning
		run.Version = 3
		run.UpdatedAt = time.Now()
		_, _ = w.provider.CompareAndSwapRunState(ctx, runID, 2, run)
	}

	entry.UpdatedAt = time.Now()
	_ = w.provider.PutRunLog(ctx, entry)

	w.appendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: pipeline.Name,
		RunID:      runID,
		Status:     string(entry.Status),
		Timestamp:  time.Now(),
	})

	w.logger.Info("trigger succeeded", "pipeline", pipeline.Name, "run", runID, "attempt", attemptNum)
}

func (w *Watcher) checkEvaluationSLA(ctx context.Context, pipeline types.PipelineConfig, result *types.ReadinessResult, now time.Time) {
	if pipeline.SLA == nil || pipeline.SLA.EvaluationDeadline == "" {
		return
	}
	if result.Status == types.Ready {
		return
	}

	deadline, err := ParseSLADeadline(pipeline.SLA.EvaluationDeadline, pipeline.SLA.Timezone, now)
	if err != nil {
		w.logger.Error("invalid SLA deadline", "pipeline", pipeline.Name, "error", err)
		return
	}

	if IsBreached(deadline, now) {
		w.fireAlert(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pipeline.Name,
			Message:    fmt.Sprintf("Pipeline %s evaluation SLA breached: not ready by %s", pipeline.Name, pipeline.SLA.EvaluationDeadline),
			Timestamp:  now,
		})
		w.appendEvent(ctx, types.Event{
			Kind:       types.EventSLABreached,
			PipelineID: pipeline.Name,
			Status:     string(result.Status),
			Message:    fmt.Sprintf("evaluation deadline %s breached", pipeline.SLA.EvaluationDeadline),
			Timestamp:  now,
		})
	}
}

func (w *Watcher) checkCompletionSLA(ctx context.Context, pipeline types.PipelineConfig, run types.RunState, now time.Time) {
	if pipeline.SLA == nil || pipeline.SLA.CompletionDeadline == "" {
		return
	}
	if lifecycle.IsTerminal(run.Status) {
		return
	}

	deadline, err := ParseSLADeadline(pipeline.SLA.CompletionDeadline, pipeline.SLA.Timezone, now)
	if err != nil {
		w.logger.Error("invalid SLA deadline", "pipeline", pipeline.Name, "error", err)
		return
	}

	if IsBreached(deadline, now) {
		w.fireAlert(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pipeline.Name,
			Message:    fmt.Sprintf("Pipeline %s completion SLA breached: run %s still %s past %s", pipeline.Name, run.RunID, run.Status, pipeline.SLA.CompletionDeadline),
			Timestamp:  now,
		})
		w.appendEvent(ctx, types.Event{
			Kind:       types.EventSLABreached,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(run.Status),
			Message:    fmt.Sprintf("completion deadline %s breached", pipeline.SLA.CompletionDeadline),
			Timestamp:  now,
		})
	}
}

func (w *Watcher) retryPolicyFor(pipeline types.PipelineConfig) types.RetryPolicy {
	if pipeline.Retry != nil {
		return *pipeline.Retry
	}
	return DefaultRetryPolicy()
}

func (w *Watcher) appendEvent(ctx context.Context, event types.Event) {
	if err := w.provider.AppendEvent(ctx, event); err != nil {
		w.logger.Error("failed to append event", "pipeline", event.PipelineID, "event", string(event.Kind), "error", err)
	}
}
