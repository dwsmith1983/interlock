package lambda

import (
	"context"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// IsJobTerminal checks the joblog for a terminal event (success, fail, timeout).
// Returns true if the pipeline has finished processing for the given date.
func IsJobTerminal(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) bool {
	rec, err := d.Store.GetLatestJobEvent(ctx, pipelineID, scheduleID, date)
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
// row (nil when there is none). With a trigger row, COMPLETED and
// FAILED_FINAL are finished. A RUNNING row is not finished when the newest
// job row is a fail or timeout: a CompleteTrigger retry keeps the row
// RUNNING while the previous attempt's row is the newest, and the run is
// still going. Nor is it finished with no job row or a non-terminal newest
// row. Any other terminal newest row finishes it: a success is never
// retried (a requested rerun after one has had its SLA verdict), validation
// exhaustion writes its row without touching the trigger row, and a failed
// lock release leaves infra-trigger-exhausted under a RUNNING row. Without
// a trigger row IsJobTerminal decides, for example a trigger-exhausted run
// whose lock was released.
func RunFinished(ctx context.Context, d *Deps, tr *types.ControlRecord, pipelineID, scheduleID, date string) (finished bool, reason string) {
	if tr == nil {
		if IsJobTerminal(ctx, d, pipelineID, scheduleID, date) {
			return true, "terminal joblog event found"
		}
		return false, ""
	}
	if tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal {
		return true, "pipeline already finished"
	}
	rec, err := d.Store.GetLatestJobEvent(ctx, pipelineID, scheduleID, date)
	if err != nil {
		d.Logger.WarnContext(ctx, "joblog lookup failed, not suppressing",
			"pipeline", pipelineID, "error", err)
		return false, ""
	}
	if rec == nil {
		return false, ""
	}
	switch rec.Event {
	case types.JobEventSuccess, types.JobEventInfraTriggerExhausted, types.JobEventValidationExhausted:
		return true, "terminal joblog event found"
	default: // fail and timeout (a retry in progress) or a non-terminal row
		return false, ""
	}
}
