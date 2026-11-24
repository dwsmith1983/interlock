package lambda

import (
	"context"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// IsJobTerminal reports whether the date's newest outcome row is terminal: a
// success, fail, timeout, infra-trigger-exhausted or validation-exhausted. A
// failed lookup logs a warning and reports false.
func IsJobTerminal(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) bool {
	rec, err := d.Store.GetLatestJobOutcome(ctx, pipelineID, scheduleID, date)
	if err != nil {
		d.Logger.WarnContext(ctx, "joblog lookup failed, not suppressing",
			"pipeline", pipelineID, "error", err)
		return false
	}
	if rec == nil {
		return false
	}
	switch rec.Event {
	case types.JobEventSuccess, types.JobEventFail, types.JobEventTimeout,
		types.JobEventInfraTriggerExhausted, types.JobEventValidationExhausted:
		return true
	default:
		return false
	}
}

// RunFinished reports whether the run for a pipeline, schedule and date is
// over, so its SLA work should stop, and if so why. tr is the date's trigger
// row (nil when there is none). With a trigger row its status alone decides:
// COMPLETED and FAILED_FINAL are finished, RUNNING is not. Without one
// IsJobTerminal decides: a date whose job reported its result while the
// pipeline was in dry run has job rows and no trigger row.
func RunFinished(ctx context.Context, d *Deps, tr *types.ControlRecord, pipelineID, scheduleID, date string) (finished bool, reason string) {
	if tr != nil {
		if tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal {
			return true, "pipeline already finished"
		}
		return false, ""
	}
	if IsJobTerminal(ctx, d, pipelineID, scheduleID, date) {
		return true, "terminal joblog event found"
	}
	return false, ""
}

// EvaluationReleased reports whether the date's newest outcome row is
// evaluation-released: its evaluation ran out before the cutoff and the
// trigger lock was released, so a new start is a restart. A failed lookup
// logs a warning and reports false.
func EvaluationReleased(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) bool {
	rec, err := d.Store.GetLatestJobOutcome(ctx, pipelineID, scheduleID, date)
	if err != nil {
		d.Logger.WarnContext(ctx, "joblog lookup failed, not treating the date as released",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", err)
		return false
	}
	return rec != nil && rec.Event == types.JobEventEvaluationReleased
}
