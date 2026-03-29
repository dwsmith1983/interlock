package watchdog

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// lastCronFire returns the most recent expected fire time for a cron expression.
func lastCronFire(cron string, now time.Time, loc *time.Location) time.Time {
	fields := strings.Fields(cron)
	if len(fields) < 5 {
		return time.Time{}
	}
	minute, err := strconv.Atoi(fields[0])
	if err != nil {
		return time.Time{}
	}
	localNow := now.In(loc)

	if fields[1] == "*" {
		candidate := time.Date(localNow.Year(), localNow.Month(), localNow.Day(),
			localNow.Hour(), minute, 0, 0, loc)
		if candidate.After(localNow) {
			candidate = candidate.Add(-time.Hour)
		}
		return candidate
	}

	hour, err := strconv.Atoi(fields[1])
	if err != nil {
		return time.Time{}
	}
	candidate := time.Date(localNow.Year(), localNow.Month(), localNow.Day(),
		hour, minute, 0, 0, loc)
	if candidate.After(localNow) {
		candidate = candidate.Add(-24 * time.Hour)
	}
	return candidate
}

// detectMissedSchedules checks all cron-scheduled pipelines.
func detectMissedSchedules(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	today := now.Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.Schedule.Cron == "" {
			continue
		}

		if cfg.DryRun {
			continue
		}

		if lambda.IsExcluded(cfg, now) {
			continue
		}

		if !d.StartedAt.IsZero() {
			loc := lambda.ResolveTimezone(cfg.Schedule.Timezone)
			if lastFire := lastCronFire(cfg.Schedule.Cron, now, loc); !lastFire.IsZero() && lastFire.Before(d.StartedAt) {
				continue
			}
		}

		scheduleID := lambda.ResolveScheduleID(cfg)

		found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, today)
		if err != nil {
			d.Logger.Error("failed to check trigger for missed schedule",
				"pipelineId", id, "error", err)
			continue
		}
		if found {
			continue
		}

		if cfg.Schedule.Time != "" {
			loc := lambda.ResolveTimezone(cfg.Schedule.Timezone)
			localNow := now.In(loc)
			expectedStart, err := time.ParseInLocation("2006-01-02 15:04", today+" "+cfg.Schedule.Time, loc)
			if err == nil && localNow.Before(expectedStart) {
				continue
			}
		}

		alertDetail := map[string]interface{}{
			"source":     "watchdog",
			"cron":       cfg.Schedule.Cron,
			"actionHint": fmt.Sprintf("cron %s expected to fire — no trigger found", cfg.Schedule.Cron),
		}
		if cfg.Schedule.Time != "" {
			alertDetail["expectedTime"] = cfg.Schedule.Time
		}
		if err := lambda.PublishEvent(ctx, d, string(types.EventScheduleMissed), id, scheduleID, today,
			fmt.Sprintf("missed schedule for %s on %s", id, today), alertDetail); err != nil {
			d.Logger.Warn("failed to publish missed schedule event", "error", err, "pipeline", id, "schedule", scheduleID, "date", today)
		}

		d.Logger.Info("detected missed schedule",
			"pipelineId", id, "schedule", scheduleID, "date", today)
	}
	return nil
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
