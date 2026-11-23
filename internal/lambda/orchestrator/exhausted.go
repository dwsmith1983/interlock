package orchestrator

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Validation-exhausted statuses: a first run released before its cutoff, and
// a date that ends with VALIDATION_EXHAUSTED.
const (
	statusReleased  = "released"
	statusExhausted = "exhausted"
)

// handleValidationExhausted ends an evaluation whose window closed without
// all rules passing. For a rerun, or when no cutoff applies to the date (a
// key of the other cadence, or a row read without validation), the run ends
// (failExhausted). Otherwise, at or after the cutoff the run closes
// (failAtCutoff), and before it the date is released (releaseEvaluation) so
// readiness can be evaluated again until the cutoff.
func handleValidationExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("validation-exhausted get config: %w", err)
	}
	cutoffAt := lambda.ResolveCutoffTime(cfg, input.Date)
	rerun := false
	if !cutoffAt.IsZero() {
		if rerun, err = lambda.IsRerun(ctx, d, input.PipelineID, input.ScheduleID, input.Date); err != nil {
			return lambda.OrchestratorOutput{}, fmt.Errorf("validation-exhausted: %w", err)
		}
	}

	var status string
	switch {
	case cutoffAt.IsZero() || rerun:
		status, err = statusExhausted, failExhausted(ctx, d, input)
	case !d.Now().Before(cutoffAt):
		status, err = statusCutoff, failAtCutoff(ctx, d, cfg, input)
	default:
		status, err = statusReleased, releaseEvaluation(ctx, d, cfg, input, cutoffAt)
	}
	if err != nil {
		return lambda.OrchestratorOutput{}, err
	}
	return lambda.OrchestratorOutput{Mode: "validation-exhausted", Status: status}, nil
}

// releaseEvaluation writes the evaluation-released joblog row, deletes the
// RUNNING trigger row so a later sensor write or the watchdog can start
// evaluation again, then publishes EVALUATION_RELEASED (best effort). A
// failed delete is retried with a second joblog row, which is harmless.
func releaseEvaluation(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, input lambda.OrchestratorInput, cutoffAt time.Time) error {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	if running, err := triggerRunning(ctx, d, input); err != nil || !running {
		return err
	}
	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date, types.JobEventEvaluationReleased, "", 0, "evaluation window exhausted before the cutoff"); err != nil {
		return fmt.Errorf("write evaluation-released joblog: %w", err)
	}
	if err := d.Store.ReleaseTriggerLock(ctx, pipelineID, schedule, date); err != nil {
		return fmt.Errorf("validation-exhausted: %w", err)
	}
	at := cutoffAt.UTC().Format(time.RFC3339)
	detail := map[string]interface{}{"source": "orchestrator", "cutoff": cfg.Schedule.Trigger.Cutoff, "cutoffAt": at,
		"actionHint": "the lock is released; a new sensor write or the watchdog starts evaluation again before the cutoff"}
	if err := lambda.PublishEvent(ctx, d, string(types.EventEvaluationReleased), pipelineID, schedule, date,
		fmt.Sprintf("%s was not ready for %s within its evaluation window; evaluation can start again until the cutoff %s", pipelineID, date, at), detail); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventEvaluationReleased, "error", err)
	}
	d.Logger.InfoContext(ctx, "evaluation released before its cutoff",
		"pipelineId", pipelineID, "schedule", schedule, "date", date, "cutoffAt", at)
	return nil
}

// failExhausted writes the validation-exhausted joblog row, publishes
// VALIDATION_EXHAUSTED (the date's final alert, so a failed publish fails the
// state), then sets the RUNNING trigger row FAILED_FINAL.
func failExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) error {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	if running, err := triggerRunning(ctx, d, input); err != nil || !running {
		return err
	}
	const msg = "evaluation window exhausted without passing"
	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date, types.JobEventValidationExhausted, "", 0, msg); err != nil {
		return fmt.Errorf("write validation-exhausted joblog: %w", err)
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventValidationExhausted), pipelineID, schedule, date, msg); err != nil {
		return fmt.Errorf("publish %s: %w", types.EventValidationExhausted, err)
	}
	if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
		return fmt.Errorf("validation-exhausted: %w", err)
	}
	return nil
}

// failAtCutoff publishes SENSOR_DEADLINE_EXPIRED, then sets the RUNNING
// trigger row FAILED_FINAL. Publishing first keeps a failed publish
// retryable: FAILED_FINAL removes the stale deadline (staleAt), so a write to
// revert would leave a row RUNNING for good.
func failAtCutoff(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, input lambda.OrchestratorInput) error {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	if running, err := triggerRunning(ctx, d, input); err != nil || !running {
		return err
	}
	if err := lambda.PublishCutoffExpired(ctx, d, cfg, pipelineID, schedule, date, "orchestrator"); err != nil {
		return fmt.Errorf("validation-exhausted cutoff: %w", err)
	}
	if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
		return fmt.Errorf("validation-exhausted cutoff: %w", err)
	}
	d.Logger.InfoContext(ctx, "run ended at its cutoff without starting",
		"pipelineId", pipelineID, "schedule", schedule, "date", date)
	return nil
}

// triggerRunning reports whether the date's trigger row is RUNNING. A
// missing or finished row (a Task retry after success, or a run someone else
// ended) is logged and left alone.
func triggerRunning(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (bool, error) {
	tr, err := d.Store.GetTrigger(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return false, fmt.Errorf("validation-exhausted: %w", err)
	}
	if tr != nil && tr.Status == types.TriggerStatusRunning {
		return true, nil
	}
	status := "missing"
	if tr != nil {
		status = tr.Status
	}
	d.Logger.InfoContext(ctx, "validation exhausted, trigger row not RUNNING, leaving it",
		"pipelineId", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date, "status", status)
	return false, nil
}

// handleJobPollExhausted writes a timeout row for the run when the job poll
// window closes without a terminal result. The stream-router publishes
// JOB_FAILED from the row, and CompleteTrigger (fed event=timeout by
// InjectTimeoutEvent) decides whether to retry. The ASL treats this state as
// best effort, so that decision never depends on the row.
func handleJobPollExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventTimeout, input.RunID, 0, "job poll window exhausted"); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write job poll timeout joblog: %w", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "job-poll-exhausted",
		Status: "exhausted",
	}, nil
}

// handleTriggerExhausted ends a date whose launch ran out of retries: it
// writes the infra-trigger-exhausted joblog row (audit), publishes
// RETRY_EXHAUSTED (the date's final alert, so a failed publish fails the
// state), then sets the trigger row FAILED_FINAL, creating it when missing.
// Nothing starts the date again on its own; a RERUN_REQUEST runs it. A
// finished row (a Task retry after this state succeeded) is left alone. A
// state retry after the joblog write adds a second row, which is harmless.
func handleTriggerExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	out := lambda.OrchestratorOutput{Mode: "trigger-exhausted", Status: "exhausted"}
	tr, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger-exhausted: %w", err)
	}
	if tr != nil && (tr.Status == types.TriggerStatusFailedFinal || tr.Status == types.TriggerStatusCompleted) {
		d.Logger.InfoContext(ctx, "trigger exhausted, trigger row already finished, leaving it",
			"pipelineId", pipelineID, "schedule", schedule, "date", date, "status", tr.Status)
		return out, nil
	}

	errMsg := ""
	if cause, ok := input.ErrorInfo["Cause"].(string); ok {
		errMsg = cause
	}
	if err := d.Store.WriteJobEvent(ctx, pipelineID, schedule, date,
		types.JobEventInfraTriggerExhausted, "", 0, errMsg); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write trigger-exhausted joblog: %w", err)
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventRetryExhausted), pipelineID, schedule, date,
		fmt.Sprintf("trigger retries exhausted for %s: %s; submit a RERUN_REQUEST to run it", pipelineID, errMsg)); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("publish %s: %w", types.EventRetryExhausted, err)
	}
	if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("trigger-exhausted: %w", err)
	}
	return out, nil
}
