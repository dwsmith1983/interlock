package watchdog

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
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
		if cfg.DryRun {
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
			if !lambda.CutoffPassed(cfg, key, now) {
				continue
			}
			tr, err := d.Store.GetTrigger(ctx, id, scheduleID, key)
			if err != nil {
				d.Logger.Error("trigger lookup failed in cutoff check", "pipelineId", id, "date", key, "error", err)
				continue
			}
			if tr != nil {
				continue
			}
			// An error is retried on the next pass.
			if _, err := lambda.CloseRunAtCutoff(ctx, d, cfg, id, scheduleID, key, "watchdog"); err != nil {
				d.Logger.Error("cutoff close failed", "pipelineId", id, "date", key, "error", err)
			}
		}
	}
	return nil
}
