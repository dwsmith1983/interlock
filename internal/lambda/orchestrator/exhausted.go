package orchestrator

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Validation-exhausted statuses: a first run released before its cutoff, and
// a date that ends with VALIDATION_EXHAUSTED.
const (
	statusReleased  = "released"
	statusExhausted = "exhausted"
)

// handleValidationExhausted ends an evaluation whose window closed without
// all rules passing. For a rerun, or without a cutoff (only a config read
// without validation lacks one), the run ends (failExhausted). Otherwise, at
// or after the cutoff the run closes (failAtCutoff), and before it the date
// is released (releaseEvaluation) so readiness can be evaluated again until
// the cutoff.
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
		return false, fmt.Errorf("%s: %w", input.Mode, err)
	}
	if tr != nil && tr.Status == types.TriggerStatusRunning {
		return true, nil
	}
	status := "missing"
	if tr != nil {
		status = tr.Status
	}
	d.Logger.InfoContext(ctx, "trigger row not RUNNING, leaving it",
		"mode", input.Mode, "pipelineId", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date, "status", status)
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

// handleInfraFailure ends a date whose execution failed on an infrastructure
// error in Evaluate, ValidationExhausted or CheckJob: it publishes
// INFRA_FAILURE (the date's final alert, so a failed publish fails the state),
// then sets the RUNNING trigger row FAILED_FINAL. A missing or finished row, or
// one a later execution holds (releasedOwnLock), is left alone and nothing is
// published. A state retry after a failed status write publishes again.
func handleInfraFailure(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	out := lambda.OrchestratorOutput{Mode: "infra-failure", Status: types.TriggerStatusFailedFinal}
	running, err := triggerRunning(ctx, d, input)
	if err != nil {
		return lambda.OrchestratorOutput{}, err
	}
	if !running {
		return out, nil
	}
	released, err := releasedOwnLock(ctx, d, input)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("infra-failure: %w", err)
	}
	if released {
		d.Logger.InfoContext(ctx, "execution released its trigger lock before it failed, leaving the newer run's row",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
		return out, nil
	}

	errName, _ := input.ErrorInfo["Error"].(string)
	cause, _ := input.ErrorInfo["Cause"].(string)
	cause = store.CutReason(cause)
	// Slack shows the message and no error or cause field, so it carries both.
	msg := fmt.Sprintf("execution for %s failed on an infrastructure error", pipelineID)
	for _, s := range []string{errName, cause} {
		if s != "" {
			msg += ": " + s
		}
	}
	detail := map[string]interface{}{"source": "orchestrator", "error": errName, "cause": cause,
		"actionHint": "the job may still be running if it had been launched: check it, then submit a RERUN_REQUEST to run the date again"}
	if err := lambda.PublishEvent(ctx, d, string(types.EventInfraFailure), pipelineID, schedule, date, msg, detail); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("publish %s: %w", types.EventInfraFailure, err)
	}
	if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("infra-failure: %w", err)
	}
	return out, nil
}

// releasedOwnLock reports whether this execution released its trigger lock
// (releaseEvaluation), so that a RUNNING row belongs to a later one: the date's
// newest outcome row is evaluation-released and was written after this
// execution started. An execution that takes a released lock starts after that
// row, so it never counts an earlier release as its own. A release that failed
// between the row and the delete looks the same; its trigger row is left to the
// watchdog's stale check. A newer outcome of a run started after the release
// hides it, so the failing run closes that run's row; that is accepted, since
// the later run must also write an outcome row within this one's last retries.
// Clock skew between Lambda (the row) and Step Functions (the start) leans
// toward staying silent, not toward closing a wrong row: a run's own release
// normally comes at least one evaluation interval after its start, while a run
// can start right after another's release.
func releasedOwnLock(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (bool, error) {
	startedAt, err := time.Parse(time.RFC3339, input.ExecutionStartedAt)
	if err != nil {
		return false, fmt.Errorf("parse executionStartedAt: %w", err)
	}
	rec, err := d.Store.GetLatestJobOutcome(ctx, input.PipelineID, input.ScheduleID, input.Date)
	if err != nil {
		return false, err
	}
	if rec == nil || rec.Event != types.JobEventEvaluationReleased {
		return false, nil
	}
	// The sort key ends in the row's write time in Unix milliseconds.
	ms, err := strconv.ParseInt(rec.SK[strings.LastIndexByte(rec.SK, '#')+1:], 10, 64)
	if err != nil {
		return false, fmt.Errorf("parse write time of job row %q: %w", rec.SK, err)
	}
	return time.UnixMilli(ms).After(startedAt), nil
}
