package watchdog

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// checkTriggerDeadlines closes each RUN# key whose schedule.trigger.cutoff
// passed before its run started: a FAILED_FINAL trigger row and one
// SENSOR_DEADLINE_EXPIRED, so nothing more starts for that date.
func checkTriggerDeadlines(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	from := now.UTC().AddDate(0, 0, -runKeysDaysBack).Format("2006-01-02")
	to := now.UTC().AddDate(0, 0, runKeysDaysAhead).Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.Schedule.Trigger == nil || cfg.Schedule.Trigger.Cutoff == "" || cfg.DryRun {
			continue
		}

		keys, err := d.Store.ListRunKeys(ctx, id, from, to)
		if err != nil {
			d.Logger.Error("run key lookup failed in cutoff check", "pipelineId", id, "error", err)
			continue
		}
		if len(keys) == 0 {
			d.Logger.Debug("no RUN# rows in range, no cutoff check", "pipelineId", id)
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)
		for _, key := range keys {
			if !lambda.IsValidExecutionDate(key) {
				d.Logger.Warn("skipping malformed RUN# key", "pipelineId", id, "runKey", key)
				continue
			}
			if lambda.IsExcludedDate(cfg, key) || !lambda.CutoffPassed(cfg, key, now) {
				continue
			}
			tr, err := d.Store.GetTrigger(ctx, id, scheduleID, key)
			if err != nil {
				d.Logger.Error("trigger lookup failed in cutoff check", "pipelineId", id, "date", key, "error", err)
				continue
			}
			if tr != nil || lambda.IsJobTerminal(ctx, d, id, scheduleID, key) {
				continue
			}
			closeSensorTriggerWindow(ctx, d, id, scheduleID, key, cfg)
		}
	}
	return nil
}

// closeSensorTriggerWindow writes FAILED_FINAL for a run date past its cutoff
// and, only when it created the row, publishes SENSOR_DEADLINE_EXPIRED. A
// failed publish deletes the row again, so the next pass retries both.
func closeSensorTriggerWindow(ctx context.Context, d *lambda.Deps, pipelineID, scheduleID, date string, cfg *types.PipelineConfig) {
	created, err := d.Store.CreateTriggerIfAbsent(ctx, pipelineID, scheduleID, date, types.TriggerStatusFailedFinal)
	if err != nil {
		d.Logger.Error("failed to write FAILED_FINAL for expired trigger deadline",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", err)
		return
	}
	if !created {
		d.Logger.Info("trigger appeared during deadline check, skipping window close",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date)
		return
	}

	cutoffAt := lambda.ResolveCutoffTime(cfg, date).UTC().Format(time.RFC3339)
	alertDetail := map[string]interface{}{"source": "watchdog", "cutoff": cfg.Schedule.Trigger.Cutoff, "cutoffAt": cutoffAt,
		"actionHint": "cutoff passed and the pipeline did not start; nothing more will start for this date. " +
			"Investigate, then submit a RERUN_REQUEST to run it manually"}
	if err := lambda.PublishEvent(ctx, d, string(types.EventSensorDeadlineExpired), pipelineID, scheduleID, date,
		fmt.Sprintf("%s did not start for %s by its cutoff %s", pipelineID, date, cutoffAt), alertDetail); err != nil {
		d.Logger.Error("failed to publish sensor deadline expired event", "pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", err)
		if err := d.Store.ReleaseTriggerLock(ctx, pipelineID, scheduleID, date); err != nil {
			d.Logger.Error("failed to delete FAILED_FINAL after a failed publish", "pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", err)
		}
		return
	}

	d.Logger.Info("sensor trigger window closed",
		"pipelineId", pipelineID, "schedule", scheduleID, "date", date,
		"cutoffAt", cutoffAt)
}
