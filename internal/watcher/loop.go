package watcher

import (
	"context"
	"fmt"
	"time"

	"github.com/interlock-systems/interlock/internal/lifecycle"
	"github.com/interlock-systems/interlock/internal/metrics"
	"github.com/interlock-systems/interlock/internal/trigger"
	"github.com/interlock-systems/interlock/pkg/types"
)

// computeLockTTL calculates a lock TTL based on the number and timeout of traits.
func computeLockTTL(pipeline types.PipelineConfig, fallback time.Duration) time.Duration {
	maxTimeout := 30 // seconds, default
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
	computed := time.Duration(maxTimeout*count+30) * time.Second
	if computed < fallback*2 {
		return fallback * 2
	}
	return computed
}

// tick processes a single pipeline evaluation cycle.
func (w *Watcher) tick(ctx context.Context, pipeline types.PipelineConfig, interval time.Duration) {
	lockKey := "eval:" + pipeline.Name
	now := time.Now()
	today := now.Format("2006-01-02")

	// Step 1: Acquire lock to prevent double-evaluation
	acquired, err := w.provider.AcquireLock(ctx, lockKey, computeLockTTL(pipeline, interval))
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
		if runs[0].Status == types.RunCompletedMonitoring {
			w.checkMonitoring(ctx, pipeline, runs[0], now)
		} else {
			w.checkAirflowRun(ctx, pipeline, runs[0], now)
			w.checkCompletionSLA(ctx, pipeline, runs[0], now)
		}
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
				if err := w.provider.PutRunLog(ctx, *runLog); err != nil {
					w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
				}
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
				if err := w.provider.PutRunLog(ctx, *runLog); err != nil {
					w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
				}
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
	metrics.TriggersTotal.Add(1)
	triggerMeta, triggerErr := trigger.Execute(ctx, pipeline.Trigger)

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
			metrics.RetriesScheduled.Add(1)
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
			_, _ = w.provider.CompareAndSwapRunState(ctx, runID, 2, run)
		} else {
			entry.Status = types.RunCompleted
			completedAt := time.Now()
			entry.CompletedAt = &completedAt

			run.Status = types.RunCompleted
			run.Version = 3
			run.UpdatedAt = time.Now()
			_, _ = w.provider.CompareAndSwapRunState(ctx, runID, 2, run)
		}
	} else {
		entry.Status = types.RunRunning

		run.Status = types.RunRunning
		run.Version = 3
		run.UpdatedAt = time.Now()
		_, _ = w.provider.CompareAndSwapRunState(ctx, runID, 2, run)
	}

	entry.UpdatedAt = time.Now()
	if err := w.provider.PutRunLog(ctx, entry); err != nil {
		w.logger.Error("failed to persist run log", "pipeline", pipeline.Name, "error", err)
	}

	w.appendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: pipeline.Name,
		RunID:      runID,
		Status:     string(entry.Status),
		Timestamp:  time.Now(),
	})

	if entry.Status == types.RunCompletedMonitoring {
		w.appendEvent(ctx, types.Event{
			Kind:       types.EventMonitoringStarted,
			PipelineID: pipeline.Name,
			RunID:      runID,
			Message:    fmt.Sprintf("post-completion monitoring started (duration: %s)", monitoringDuration(pipeline)),
			Timestamp:  time.Now(),
		})
	}

	w.logger.Info("trigger succeeded", "pipeline", pipeline.Name, "run", runID, "attempt", attemptNum)
}

func (w *Watcher) checkAirflowRun(ctx context.Context, pipeline types.PipelineConfig, run types.RunState, now time.Time) {
	dagRunID, _ := run.Metadata["airflow_dag_run_id"].(string)
	if dagRunID == "" {
		return
	}
	airflowURL, _ := run.Metadata["airflow_url"].(string)
	dagID, _ := run.Metadata["airflow_dag_id"].(string)
	if airflowURL == "" || dagID == "" {
		return
	}

	var headers map[string]string
	if pipeline.Trigger != nil {
		headers = pipeline.Trigger.Headers
	}

	state, err := trigger.CheckAirflowStatus(ctx, airflowURL, dagID, dagRunID, headers)
	if err != nil {
		w.logger.Error("failed to check airflow status", "pipeline", pipeline.Name, "run", run.RunID, "error", err)
		return
	}

	today := now.Format("2006-01-02")

	switch state {
	case "success":
		targetStatus := types.RunCompleted
		if monitoringEnabled(pipeline) {
			targetStatus = types.RunCompletedMonitoring
		}

		newRun := run
		newRun.Status = targetStatus
		newRun.Version = run.Version + 1
		newRun.UpdatedAt = now
		if monitoringEnabled(pipeline) {
			if newRun.Metadata == nil {
				newRun.Metadata = make(map[string]interface{})
			}
			newRun.Metadata["monitoringStartedAt"] = now.Format(time.RFC3339)
		}
		ok, err := w.provider.CompareAndSwapRunState(ctx, run.RunID, run.Version, newRun)
		if err != nil || !ok {
			w.logger.Error("failed to CAS airflow run to target status", "pipeline", pipeline.Name, "run", run.RunID, "targetStatus", targetStatus, "error", err)
			return
		}
		if runLog, err := w.provider.GetRunLog(ctx, pipeline.Name, today); err == nil && runLog != nil && runLog.RunID == run.RunID {
			runLog.Status = targetStatus
			if targetStatus == types.RunCompleted {
				runLog.CompletedAt = &now
			}
			runLog.UpdatedAt = now
			_ = w.provider.PutRunLog(ctx, *runLog)
		}
		w.appendEvent(ctx, types.Event{
			Kind:       types.EventCallbackReceived,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(targetStatus),
			Message:    "airflow dag run completed successfully",
			Timestamp:  now,
		})
		if targetStatus == types.RunCompletedMonitoring {
			w.appendEvent(ctx, types.Event{
				Kind:       types.EventMonitoringStarted,
				PipelineID: pipeline.Name,
				RunID:      run.RunID,
				Message:    fmt.Sprintf("post-completion monitoring started (duration: %s)", monitoringDuration(pipeline)),
				Timestamp:  now,
			})
		}
		w.logger.Info("airflow dag run completed", "pipeline", pipeline.Name, "run", run.RunID, "dagRunID", dagRunID)

	case "failed":
		newRun := run
		newRun.Status = types.RunFailed
		newRun.Version = run.Version + 1
		newRun.UpdatedAt = now
		ok, err := w.provider.CompareAndSwapRunState(ctx, run.RunID, run.Version, newRun)
		if err != nil || !ok {
			w.logger.Error("failed to CAS airflow run to FAILED", "pipeline", pipeline.Name, "run", run.RunID, "error", err)
			return
		}
		if runLog, err := w.provider.GetRunLog(ctx, pipeline.Name, today); err == nil && runLog != nil && runLog.RunID == run.RunID {
			runLog.Status = types.RunFailed
			runLog.FailureMessage = "airflow dag run failed"
			runLog.FailureCategory = types.FailureTransient
			runLog.CompletedAt = &now
			runLog.UpdatedAt = now
			_ = w.provider.PutRunLog(ctx, *runLog)
		}
		w.fireAlert(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: pipeline.Name,
			Message:    fmt.Sprintf("Airflow DAG run %s failed for pipeline %s", dagRunID, pipeline.Name),
			Timestamp:  now,
		})
		w.appendEvent(ctx, types.Event{
			Kind:       types.EventCallbackReceived,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(types.RunFailed),
			Message:    "airflow dag run failed",
			Timestamp:  now,
		})
		w.logger.Warn("airflow dag run failed", "pipeline", pipeline.Name, "run", run.RunID, "dagRunID", dagRunID)
	}
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
		metrics.SLABreaches.Add(1)
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
		metrics.SLABreaches.Add(1)
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

func (w *Watcher) checkMonitoring(ctx context.Context, pipeline types.PipelineConfig, run types.RunState, now time.Time) {
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
		newRun := run
		newRun.Status = types.RunCompleted
		newRun.Version = run.Version + 1
		newRun.UpdatedAt = now
		ok, casErr := w.provider.CompareAndSwapRunState(ctx, run.RunID, run.Version, newRun)
		if casErr != nil || !ok {
			w.logger.Error("failed to CAS monitoring run to COMPLETED", "pipeline", pipeline.Name, "run", run.RunID, "error", casErr)
			return
		}
		if runLog, logErr := w.provider.GetRunLog(ctx, pipeline.Name, today); logErr == nil && runLog != nil && runLog.RunID == run.RunID {
			runLog.Status = types.RunCompleted
			completedAt := now
			runLog.CompletedAt = &completedAt
			runLog.UpdatedAt = now
			_ = w.provider.PutRunLog(ctx, *runLog)
		}
		w.appendEvent(ctx, types.Event{
			Kind:       types.EventMonitoringCompleted,
			PipelineID: pipeline.Name,
			RunID:      run.RunID,
			Status:     string(types.RunCompleted),
			Message:    "monitoring window expired with no drift detected",
			Timestamp:  now,
		})
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

	w.appendEvent(ctx, types.Event{
		Kind:       types.EventMonitoringDrift,
		PipelineID: pipeline.Name,
		RunID:      run.RunID,
		Message:    fmt.Sprintf("trait drift detected, rerun %s created", rerunID),
		Details: map[string]interface{}{
			"rerunId":        rerunID,
			"blockingTraits": result.Blocking,
		},
		Timestamp: now,
	})

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
	newRun := run
	newRun.Status = types.RunCompleted
	newRun.Version = run.Version + 1
	newRun.UpdatedAt = now
	ok, casErr := w.provider.CompareAndSwapRunState(ctx, run.RunID, run.Version, newRun)
	if casErr != nil || !ok {
		w.logger.Error("failed to CAS monitoring run to COMPLETED after drift", "pipeline", pipeline.Name, "run", run.RunID, "error", casErr)
	}
	if runLog, logErr := w.provider.GetRunLog(ctx, pipeline.Name, today); logErr == nil && runLog != nil && runLog.RunID == run.RunID {
		runLog.Status = types.RunCompleted
		completedAt := now
		runLog.CompletedAt = &completedAt
		runLog.UpdatedAt = now
		_ = w.provider.PutRunLog(ctx, *runLog)
	}

	w.logger.Info("monitoring drift detected, rerun created", "pipeline", pipeline.Name, "run", run.RunID, "rerunID", rerunID)
}

func (w *Watcher) appendEvent(ctx context.Context, event types.Event) {
	if err := w.provider.AppendEvent(ctx, event); err != nil {
		w.logger.Error("failed to append event", "pipeline", event.PipelineID, "event", string(event.Kind), "error", err)
	}
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
	return 2 * time.Hour // default
}
