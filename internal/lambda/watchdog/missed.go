package watchdog

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// detectMissedSchedules is the start-by check for cron pipelines: for each RUN#
// key whose start-by time (the cron time) passed before its job started, it
// publishes SCHEDULE_MISSED once (a SENSOR# marker per key keeps later passes quiet).
func detectMissedSchedules(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	from := now.UTC().AddDate(0, 0, -runKeysDaysBack).Format("2006-01-02")
	to := now.UTC().AddDate(0, 0, runKeysDaysAhead).Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.Schedule.Cron == "" || cfg.DryRun {
			continue
		}

		keys, err := d.Store.ListRunKeys(ctx, id, from, to)
		if err != nil {
			d.Logger.Error("run key lookup failed in start-by check", "pipelineId", id, "error", err)
			continue
		}
		if len(keys) == 0 {
			d.Logger.Debug("no RUN# rows in range, no start-by check", "pipelineId", id)
			continue
		}

		loc := lambda.ResolveTimezone(cfg.Schedule.Timezone)
		for _, key := range keys {
			if !lambda.IsValidExecutionDate(key) {
				d.Logger.Warn("skipping malformed RUN# key", "pipelineId", id, "runKey", key)
				continue
			}
			if lambda.IsExcludedDate(cfg, key) {
				continue
			}
			startBy, ok, err := lambda.StartByTime(cfg.Schedule.Cron, key, loc)
			if err != nil {
				d.Logger.Warn("start-by time failed", "pipelineId", id, "runKey", key, "error", err)
				continue
			}
			if !ok || now.Before(startBy) {
				continue
			}
			checkStartBy(ctx, d, id, cfg, key, startBy)
		}
	}
	return nil
}

// checkStartBy publishes SCHEDULE_MISSED for one run key past its start-by
// time, unless its job started, its run finished, or it was alerted already.
func checkStartBy(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, key string, startBy time.Time) {
	scheduleID := lambda.ResolveScheduleID(cfg)
	tr, err := d.Store.GetTrigger(ctx, id, scheduleID, key)
	if err != nil {
		d.Logger.Error("trigger lookup failed in start-by check", "pipelineId", id, "date", key, "error", err)
		return
	}
	// FAILED_FINAL has sent its own final alert.
	if tr != nil && (tr.StartedAt != "" || tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal) {
		return
	}

	dedupKey := "schedule-missed-check#" + key
	if marker, err := d.Store.GetSensorData(ctx, id, dedupKey); err != nil || marker != nil {
		if err != nil {
			d.Logger.Error("dedup marker lookup failed in start-by check", "pipelineId", id, "date", key, "error", err)
		}
		return
	}

	alertDetail := map[string]interface{}{"source": "watchdog", "cron": cfg.Schedule.Cron, "startBy": startBy.UTC().Format(time.RFC3339),
		"actionHint": "start-by time passed and the job has not started; the SLA is at risk"}
	if err := lambda.PublishEvent(ctx, d, string(types.EventScheduleMissed), id, scheduleID, key,
		fmt.Sprintf("job for %s on %s has not started by %s", id, key, alertDetail["startBy"]), alertDetail); err != nil {
		d.Logger.Warn("failed to publish missed schedule event", "error", err, "pipeline", id, "schedule", scheduleID, "date", key)
		return // no marker, so the next pass retries
	}
	if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{"alerted": "true"}); err != nil {
		d.Logger.Warn("failed to write start-by dedup marker", "error", err, "pipeline", id, "date", key)
	}
	d.Logger.Info("detected missed start-by time", "pipelineId", id, "schedule", scheduleID, "date", key, "startBy", alertDetail["startBy"])
}

// detectMissedInclusionSchedules checks pipelines with inclusion calendar config.
func detectMissedInclusionSchedules(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()

	for id, cfg := range configs {
		if cfg.Schedule.Include == nil || len(cfg.Schedule.Include.Dates) == 0 {
			continue
		}

		if cfg.DryRun {
			continue
		}

		if lambda.IsExcluded(cfg, now) {
			continue
		}

		pastDates := lambda.PastInclusionDates(cfg.Schedule.Include.Dates, now)
		if len(pastDates) == 0 {
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)

		tzLoc := lambda.ResolveTimezone(cfg.Schedule.Timezone)
		today := now.In(tzLoc).Format("2006-01-02")

		for _, date := range pastDates {
			if cfg.Schedule.Time != "" && date == today {
				localNow := now.In(tzLoc)
				expectedStart, err := time.ParseInLocation("2006-01-02 15:04", date+" "+cfg.Schedule.Time, tzLoc)
				if err == nil && localNow.Before(expectedStart) {
					continue
				}
			}

			found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, date)
			if err != nil {
				d.Logger.Error("failed to check trigger for inclusion schedule",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if found {
				continue
			}

			dedupKey := "irregular-missed-check#" + date
			dedupData, err := d.Store.GetSensorData(ctx, id, dedupKey)
			if err != nil {
				d.Logger.Error("dedup marker lookup failed for inclusion schedule",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if dedupData != nil {
				continue
			}

			alertDetail := map[string]interface{}{
				"source":     "watchdog",
				"actionHint": fmt.Sprintf("inclusion date %s expected to have a trigger — none found", date),
			}
			if err := lambda.PublishEvent(ctx, d, string(types.EventIrregularScheduleMissed), id, scheduleID, date,
				fmt.Sprintf("missed inclusion schedule for %s on %s", id, date), alertDetail); err != nil {
				d.Logger.Warn("failed to publish irregular schedule missed event", "error", err, "pipeline", id, "date", date)
			}

			if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{
				"alerted": "true",
			}); err != nil {
				d.Logger.Warn("failed to write inclusion dedup marker", "error", err, "pipeline", id, "date", date)
			}

			d.Logger.Info("detected missed inclusion schedule",
				"pipelineId", id, "schedule", scheduleID, "date", date)
		}
	}
	return nil
}
