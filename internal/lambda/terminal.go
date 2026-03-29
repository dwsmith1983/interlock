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
		types.JobEventInfraTriggerExhausted, types.JobEventValidationExhausted,
		types.JobEventJobPollExhausted:
		return true
	default:
		return false
	}
}
