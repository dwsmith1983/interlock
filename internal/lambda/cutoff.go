package lambda

import (
	"context"
	"fmt"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// CloseRunAtCutoff closes a run date whose schedule.trigger.cutoff has
// passed: it writes a FAILED_FINAL trigger row and, only when it created the
// row, publishes SENSOR_DEADLINE_EXPIRED with the given source. It reports
// whether it closed the date. An excluded date, a date whose job ran and a
// date that has a trigger row are left alone; a date with only an exhausted
// launch or evaluation job row never ran, so it is closed. A failed
// publish deletes the row again and returns the error, so a retry closes the
// date and alerts.
func CloseRunAtCutoff(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, source string) (bool, error) {
	if IsExcludedDate(cfg, date) || jobRan(ctx, d, pipelineID, scheduleID, date) {
		return false, nil
	}
	created, err := d.Store.CreateTriggerIfAbsent(ctx, pipelineID, scheduleID, date, types.TriggerStatusFailedFinal)
	if err != nil {
		return false, fmt.Errorf("write FAILED_FINAL for %q/%s/%s past its cutoff: %w", pipelineID, scheduleID, date, err)
	}
	if !created {
		return false, nil
	}

	if err := PublishCutoffExpired(ctx, d, cfg, pipelineID, scheduleID, date, source); err != nil {
		if relErr := d.Store.ReleaseTriggerLock(ctx, pipelineID, scheduleID, date); relErr != nil {
			d.Logger.Error("failed to delete FAILED_FINAL after a failed publish",
				"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", relErr)
		}
		return false, fmt.Errorf("publish %s for %q/%s/%s: %w", types.EventSensorDeadlineExpired, pipelineID, scheduleID, date, err)
	}

	d.Logger.Info("run date closed at its cutoff",
		"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "cutoffAt", ResolveCutoffTime(cfg, date).UTC().Format(time.RFC3339), "source", source)
	return true, nil
}

// PublishCutoffExpired publishes SENSOR_DEADLINE_EXPIRED for a run date that
// did not start by its cutoff, with the given source in its detail. cfg must
// have a trigger cutoff (callers check CutoffPassed first). The publish error
// is returned unwrapped for the caller to wrap.
func PublishCutoffExpired(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, source string) error {
	cutoffAt := ResolveCutoffTime(cfg, date).UTC().Format(time.RFC3339)
	alertDetail := map[string]interface{}{"source": source, "cutoff": cfg.Schedule.Trigger.Cutoff, "cutoffAt": cutoffAt,
		"actionHint": "cutoff passed and the pipeline did not start; nothing more will start for this date. " +
			"Investigate, then submit a RERUN_REQUEST to run it manually"}
	return PublishEvent(ctx, d, string(types.EventSensorDeadlineExpired), pipelineID, scheduleID, date,
		fmt.Sprintf("%s did not start for %s by its cutoff %s", pipelineID, date, cutoffAt), alertDetail)
}

// IsRerun reports whether the run date has had a rerun accepted: a RERUN#
// row is written before any rerun or failure retry starts.
func IsRerun(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) (bool, error) {
	n, err := d.Store.CountReruns(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return false, fmt.Errorf("check reruns for %q/%s/%s: %w", pipelineID, scheduleID, date, err)
	}
	return n > 0, nil
}

// jobRan reports whether the date's latest job row is a success, fail or
// timeout, so its job started. A failed lookup counts as not run.
func jobRan(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) bool {
	rec, err := d.Store.GetLatestJobEvent(ctx, pipelineID, scheduleID, date)
	if err != nil {
		d.Logger.WarnContext(ctx, "joblog lookup failed, closing the date",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", err)
		return false
	}
	if rec == nil {
		return false
	}
	switch rec.Event {
	case types.JobEventSuccess, types.JobEventFail, types.JobEventTimeout:
		return true
	default:
		return false
	}
}
