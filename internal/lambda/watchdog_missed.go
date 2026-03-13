package lambda

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// lastCronFire returns the most recent expected fire time for a cron expression.
// Supports the minute-hour patterns used by this system: "MM * * * *" (hourly)
// and "MM HH * * *" (daily). Returns zero time for unsupported patterns.
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
		// Hourly: fires at :MM every hour.
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
	// Daily: fires at HH:MM every day.
	candidate := time.Date(localNow.Year(), localNow.Month(), localNow.Day(),
		hour, minute, 0, 0, loc)
	if candidate.After(localNow) {
		candidate = candidate.Add(-24 * time.Hour)
	}
	return candidate
}

// detectMissedSchedules checks all cron-scheduled pipelines to see if today's
// trigger is missing. If a pipeline should have started by now but has no
// TRIGGER# row, a SCHEDULE_MISSED event is published.
func detectMissedSchedules(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.now()
	today := now.Format("2006-01-02")

	for id, cfg := range configs {
		// Only check cron-scheduled pipelines.
		if cfg.Schedule.Cron == "" {
			continue
		}

		// Dry-run pipelines are observation-only — skip missed schedule detection.
		if cfg.DryRun {
			continue
		}

		// Skip calendar-excluded days.
		if isExcluded(cfg, now) {
			continue
		}

		// Only alert for schedules that should have fired after this Lambda
		// started. Prevents retroactive alerts after fresh deploys.
		if !d.StartedAt.IsZero() {
			loc := resolveTimezone(cfg.Schedule.Timezone)
			if lastFire := lastCronFire(cfg.Schedule.Cron, now, loc); !lastFire.IsZero() && lastFire.Before(d.StartedAt) {
				continue
			}
		}

		// Resolve schedule ID for cron pipelines.
		scheduleID := resolveScheduleID(cfg)

		// Check if any TRIGGER# row exists for today (covers both daily
		// and per-hour trigger rows, e.g. "2026-03-04" and "2026-03-04T00").
		found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, today)
		if err != nil {
			d.Logger.Error("failed to check trigger for missed schedule",
				"pipelineId", id, "error", err)
			continue
		}
		if found {
			continue
		}

		// Check if we are past the expected start time. If the pipeline
		// has a schedule time configured, only alert after that time.
		if cfg.Schedule.Time != "" {
			loc := resolveTimezone(cfg.Schedule.Timezone)
			localNow := now.In(loc)
			expectedStart, err := time.ParseInLocation("2006-01-02 15:04", today+" "+cfg.Schedule.Time, loc)
			if err == nil && localNow.Before(expectedStart) {
				continue // not yet past expected start time
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
		if err := publishEvent(ctx, d, string(types.EventScheduleMissed), id, scheduleID, today,
			fmt.Sprintf("missed schedule for %s on %s", id, today), alertDetail); err != nil {
			d.Logger.Warn("failed to publish missed schedule event", "error", err, "pipeline", id, "schedule", scheduleID, "date", today)
		}

		d.Logger.Info("detected missed schedule",
			"pipelineId", id,
			"schedule", scheduleID,
			"date", today,
		)
	}
	return nil
}

// detectMissedInclusionSchedules checks pipelines with inclusion calendar config
// for missed schedules on irregular dates. For each pipeline with an Include
// config, it finds all past inclusion dates (capped at maxInclusionLookback)
// and verifies that a trigger exists for each. If no trigger is found and no
// dedup marker exists, an IRREGULAR_SCHEDULE_MISSED event is published.
func detectMissedInclusionSchedules(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.now()

	for id, cfg := range configs {
		if cfg.Schedule.Include == nil || len(cfg.Schedule.Include.Dates) == 0 {
			continue
		}

		// Dry-run pipelines are observation-only — skip inclusion schedule detection.
		if cfg.DryRun {
			continue
		}

		// Skip calendar-excluded days.
		if isExcluded(cfg, now) {
			continue
		}

		pastDates := PastInclusionDates(cfg.Schedule.Include.Dates, now)
		if len(pastDates) == 0 {
			continue
		}

		scheduleID := resolveScheduleID(cfg)

		// Resolve today in the pipeline's timezone so the grace-period
		// guard fires correctly when UTC date != pipeline-local date.
		tzLoc := resolveTimezone(cfg.Schedule.Timezone)
		today := now.In(tzLoc).Format("2006-01-02")

		for _, date := range pastDates {
			// If the inclusion date is today and the pipeline has a
			// Schedule.Time, only alert after that time has passed.
			// This mirrors the same check in detectMissedSchedules for
			// cron pipelines to avoid false-positive alerts before the
			// expected start time. Past dates are not gated because
			// their Schedule.Time has necessarily already elapsed.
			if cfg.Schedule.Time != "" && date == today {
				localNow := now.In(tzLoc)
				expectedStart, err := time.ParseInLocation("2006-01-02 15:04", date+" "+cfg.Schedule.Time, tzLoc)
				if err == nil && localNow.Before(expectedStart) {
					continue // not yet past expected start time
				}
			}

			// Check if a trigger exists for this inclusion date.
			found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, date)
			if err != nil {
				d.Logger.Error("failed to check trigger for inclusion schedule",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if found {
				continue
			}

			// Check dedup marker to avoid re-alerting on subsequent watchdog runs.
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
			if err := publishEvent(ctx, d, string(types.EventIrregularScheduleMissed), id, scheduleID, date,
				fmt.Sprintf("missed inclusion schedule for %s on %s", id, date), alertDetail); err != nil {
				d.Logger.Warn("failed to publish irregular schedule missed event", "error", err, "pipeline", id, "date", date)
			}

			// Write dedup marker.
			if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{
				"alerted": "true",
			}); err != nil {
				d.Logger.Warn("failed to write inclusion dedup marker", "error", err, "pipeline", id, "date", date)
			}

			d.Logger.Info("detected missed inclusion schedule",
				"pipelineId", id,
				"schedule", scheduleID,
				"date", date,
			)
		}
	}
	return nil
}
