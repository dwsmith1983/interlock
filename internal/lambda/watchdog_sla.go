package lambda

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// scheduleSLAAlerts proactively creates EventBridge Scheduler entries for all
// pipelines with SLA configs. This ensures warnings/breaches fire even when
// pipelines never trigger (data never arrives, sensor fails, etc.).
// Idempotency: deterministic scheduler names; ConflictException = already exists.
func scheduleSLAAlerts(ctx context.Context, d *Deps) error {
	if d.Scheduler == nil {
		return nil
	}
	if SkipScheduler() {
		d.Logger.Info("SKIP_SCHEDULER set, no-op proactive SLA scheduling")
		return nil
	}

	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()

	for id, cfg := range configs {
		if cfg.SLA == nil {
			continue
		}

		// Dry-run pipelines are observation-only — skip SLA scheduling.
		if cfg.DryRun {
			continue
		}

		if IsExcluded(cfg, now) {
			continue
		}

		scheduleID := ResolveScheduleID(cfg)
		date := resolveWatchdogSLADate(cfg, now)

		// Sensor-triggered daily pipelines run T+1: data for today completes
		// tomorrow, so the SLA deadline is relative to tomorrow's date.
		// Only slaDate is shifted; the original date is kept for schedule
		// naming, trigger lookup, and fire-alert payload so cancellation
		// stays consistent with the SFN's view of the pipeline.
		slaDate := date
		if cfg.Schedule.Cron == "" && !strings.HasPrefix(cfg.SLA.Deadline, ":") {
			t, err := time.Parse("2006-01-02", date)
			if err == nil {
				slaDate = t.AddDate(0, 0, 1).Format("2006-01-02")
			}
		}

		// Skip if pipeline already completed or permanently failed for this date.
		tr, err := d.Store.GetTrigger(ctx, id, scheduleID, date)
		switch {
		case err != nil:
			d.Logger.Warn("trigger lookup failed in SLA scheduling", "pipelineId", id, "error", err)
			continue
		case tr != nil && (tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal):
			continue
		case IsJobTerminal(ctx, d, id, scheduleID, date):
			continue
		}

		calc, err := handleSLACalculate(SLAMonitorInput{
			Mode:             "calculate",
			PipelineID:       id,
			ScheduleID:       scheduleID,
			Date:             slaDate,
			Deadline:         cfg.SLA.Deadline,
			ExpectedDuration: cfg.SLA.ExpectedDuration,
			Timezone:         cfg.SLA.Timezone,
		}, now)
		if err != nil {
			d.Logger.Error("SLA calculate failed", "pipelineId", id, "error", err)
			continue
		}

		breachAt, _ := time.Parse(time.RFC3339, calc.BreachAt)
		if breachAt.IsZero() || breachAt.After(now) {
			// SLA breach is in the future — create schedules.
			scheduleErr := false
			if err := createSLASchedules(ctx, d, id, scheduleID, date, calc, true); err != nil {
				d.Logger.Error("create SLA schedule failed",
					"pipelineId", id, "error", err)
				scheduleErr = true
			}

			if !scheduleErr {
				d.Logger.Info("proactive SLA schedules ensured",
					"pipelineId", id,
					"date", date,
					"warningAt", calc.WarningAt,
					"breachAt", calc.BreachAt,
				)
			}
		}
	}
	return nil
}

// checkTriggerDeadlines evaluates trigger deadlines independently of SLA
// configuration. Pipelines with a Trigger.Deadline but no SLA config are
// checked here. For each pipeline, if the trigger deadline has passed and
// no trigger exists, the sensor trigger window is closed.
func checkTriggerDeadlines(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()

	for id, cfg := range configs {
		if cfg.Schedule.Trigger == nil || cfg.Schedule.Trigger.Deadline == "" {
			continue
		}

		// Dry-run pipelines are observation-only — skip trigger deadline checks.
		if cfg.DryRun {
			continue
		}

		if IsExcluded(cfg, now) {
			continue
		}

		scheduleID := ResolveScheduleID(cfg)
		triggerDate := resolveTriggerDeadlineDate(cfg, now)

		triggerRec, err := d.Store.GetTrigger(ctx, id, scheduleID, triggerDate)
		if err != nil {
			d.Logger.Warn("trigger lookup failed in deadline check", "pipelineId", id, "error", err)
			continue
		}
		if triggerRec != nil {
			continue
		}

		if IsJobTerminal(ctx, d, id, scheduleID, triggerDate) {
			continue
		}

		closeSensorTriggerWindow(ctx, d, id, scheduleID, triggerDate, cfg, now)
	}
	return nil
}

// resolveWatchdogSLADate determines the execution date for SLA scheduling.
//   - Hourly pipelines (relative deadline like ":30"): previous hour composite
//     date, e.g. "2026-03-05T13" when the clock is 14:xx.
//   - Daily pipelines (absolute deadline like "02:00"): today's date,
//     so handleSLACalculate rolls the deadline forward to the next occurrence.
func resolveWatchdogSLADate(cfg *types.PipelineConfig, now time.Time) string {
	if strings.HasPrefix(cfg.SLA.Deadline, ":") {
		prev := now.Add(-time.Hour)
		return prev.Format("2006-01-02") + "T" + fmt.Sprintf("%02d", prev.Hour())
	}
	return now.Format("2006-01-02")
}

// resolveTriggerDeadlineDate determines the execution date for trigger
// deadline evaluation. Uses the trigger deadline format (not SLA deadline)
// to decide between hourly composite date and daily date.
func resolveTriggerDeadlineDate(cfg *types.PipelineConfig, now time.Time) string {
	if strings.HasPrefix(cfg.Schedule.Trigger.Deadline, ":") {
		prev := now.Add(-time.Hour)
		return prev.Format("2006-01-02") + "T" + fmt.Sprintf("%02d", prev.Hour())
	}
	return now.Format("2006-01-02")
}

// resolveTriggerDeadlineTime computes the absolute time when the trigger
// window closes for the given deadline string and execution date.
//
// For relative (hourly) deadlines like ":45" with composite date "2026-03-09T13":
//   - Data for hour 13 is processed in hour 14
//   - The deadline resolves to 2026-03-09T14:45:00 in the configured timezone
//
// For absolute (daily) deadlines like "09:00" with date "2026-03-09":
//   - The deadline resolves to 2026-03-09T09:00:00 in the configured timezone
//
// Unlike handleSLACalculate, this does NOT roll forward when the time is past.
// Returns zero time on parse errors.
func resolveTriggerDeadlineTime(deadline, date, timezone string) time.Time {
	loc := ResolveTimezone(timezone)

	if strings.HasPrefix(deadline, ":") {
		// Relative (hourly): ":MM" — deadline is in the NEXT hour after the
		// composite date's hour, since data for hour H is processed in hour H+1.
		minute, err := strconv.Atoi(strings.TrimPrefix(deadline, ":"))
		if err != nil {
			return time.Time{}
		}
		// Parse composite date "YYYY-MM-DDThh".
		if len(date) < 13 || date[10] != 'T' {
			return time.Time{}
		}
		t, err := time.ParseInLocation("2006-01-02T15", date, loc)
		if err != nil {
			return time.Time{}
		}
		// Add 1 hour for the processing window, then set the minute.
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, minute, 0, 0, loc)
	}

	// Absolute (daily): "HH:MM".
	parts := strings.SplitN(deadline, ":", 2)
	if len(parts) != 2 {
		return time.Time{}
	}
	hour, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}
	}
	minute, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}
	}
	t, err := time.ParseInLocation("2006-01-02", date, loc)
	if err != nil {
		return time.Time{}
	}
	return time.Date(t.Year(), t.Month(), t.Day(), hour, minute, 0, 0, loc)
}

// closeSensorTriggerWindow checks whether the trigger deadline has passed for
// a sensor-triggered pipeline that never started. If expired, it writes a
// FAILED_FINAL trigger record (blocking future auto-triggers) and publishes
// a SENSOR_DEADLINE_EXPIRED event. A human can still restart via RERUN_REQUEST.
func closeSensorTriggerWindow(ctx context.Context, d *Deps, pipelineID, scheduleID, date string, cfg *types.PipelineConfig, now time.Time) {
	// Compute the absolute trigger deadline time directly — we do NOT use
	// handleSLACalculate here because it rolls daily deadlines forward 24h
	// when past, which defeats the purpose of checking for expiry.
	tz := cfg.Schedule.Timezone
	if tz == "" && cfg.SLA != nil {
		tz = cfg.SLA.Timezone
	}
	triggerDeadline := resolveTriggerDeadlineTime(cfg.Schedule.Trigger.Deadline, date, tz)
	if triggerDeadline.IsZero() || triggerDeadline.After(now) {
		return
	}

	// Use conditional put to avoid overwriting a trigger that was acquired
	// between the GetTrigger read and this write (TOCTOU protection).
	created, err := d.Store.CreateTriggerIfAbsent(ctx, pipelineID, scheduleID, date, types.TriggerStatusFailedFinal)
	if err != nil {
		d.Logger.Error("failed to write FAILED_FINAL for expired trigger deadline",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "error", err)
		return
	}
	if !created {
		// Trigger row appeared since the read — pipeline started, don't interfere.
		d.Logger.Info("trigger appeared during deadline check, skipping window close",
			"pipelineId", pipelineID, "schedule", scheduleID, "date", date)
		return
	}

	alertDetail := map[string]interface{}{
		"source":          "watchdog",
		"triggerDeadline": cfg.Schedule.Trigger.Deadline,
		"actionHint":      "auto-trigger window closed — use RERUN_REQUEST to restart",
	}
	if err := PublishEvent(ctx, d, string(types.EventSensorDeadlineExpired), pipelineID, scheduleID, date,
		fmt.Sprintf("trigger deadline expired for %s/%s/%s", pipelineID, scheduleID, date), alertDetail); err != nil {
		d.Logger.Warn("failed to publish sensor deadline expired event", "error", err, "pipeline", pipelineID)
	}

	d.Logger.Info("sensor trigger window closed",
		"pipelineId", pipelineID,
		"schedule", scheduleID,
		"date", date,
		"triggerDeadline", cfg.Schedule.Trigger.Deadline,
	)
}
