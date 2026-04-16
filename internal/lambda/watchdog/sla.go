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

// scheduleSLAAlerts proactively creates EventBridge Scheduler entries for all
// pipelines with SLA configs.
func scheduleSLAAlerts(ctx context.Context, d *lambda.Deps) error {
	if d.Scheduler == nil {
		return nil
	}
	if lambda.SkipScheduler() {
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

		if cfg.DryRun {
			continue
		}

		if lambda.IsExcluded(cfg, now) {
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)
		date := resolveWatchdogSLADate(cfg, now)

		slaDate := date
		if cfg.Schedule.Cron == "" && !strings.HasPrefix(cfg.SLA.Deadline, ":") {
			t, err := time.Parse("2006-01-02", date)
			if err == nil {
				slaDate = t.AddDate(0, 0, 1).Format("2006-01-02")
			}
		}

		tr, err := d.Store.GetTrigger(ctx, id, scheduleID, date)
		switch {
		case err != nil:
			d.Logger.Warn("trigger lookup failed in SLA scheduling", "pipelineId", id, "error", err)
			continue
		case tr != nil && (tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal):
			continue
		case lambda.IsJobTerminal(ctx, d, id, scheduleID, date):
			continue
		}

		calc, err := lambda.HandleSLACalculate(lambda.SLAMonitorInput{
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
			scheduleErr := false
			if err := lambda.CreateSLASchedules(ctx, d, id, scheduleID, date, calc, true); err != nil {
				d.Logger.Error("create SLA schedule failed",
					"pipelineId", id, "error", err)
				scheduleErr = true
			}

			if !scheduleErr {
				d.Logger.Info("proactive SLA schedules ensured",
					"pipelineId", id, "date", date,
					"warningAt", calc.WarningAt, "breachAt", calc.BreachAt)
			}
		}
	}
	return nil
}

// checkTriggerDeadlines evaluates trigger deadlines independently of SLA config.
func checkTriggerDeadlines(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()

	for id, cfg := range configs {
		if cfg.Schedule.Trigger == nil || cfg.Schedule.Trigger.Deadline == "" {
			continue
		}

		if cfg.DryRun {
			continue
		}

		if lambda.IsExcluded(cfg, now) {
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)
		triggerDate := resolveTriggerDeadlineDate(cfg, now)

		triggerRec, err := d.Store.GetTrigger(ctx, id, scheduleID, triggerDate)
		if err != nil {
			d.Logger.Warn("trigger lookup failed in deadline check", "pipelineId", id, "error", err)
			continue
		}
		if triggerRec != nil {
			continue
		}

		if lambda.IsJobTerminal(ctx, d, id, scheduleID, triggerDate) {
			continue
		}

		closeSensorTriggerWindow(ctx, d, id, scheduleID, triggerDate, cfg, now)
	}
	return nil
}

func resolveWatchdogSLADate(cfg *types.PipelineConfig, now time.Time) string {
	if strings.HasPrefix(cfg.SLA.Deadline, ":") {
		prev := now.Add(-time.Hour)
		return prev.Format("2006-01-02") + "T" + fmt.Sprintf("%02d", prev.Hour())
	}
	return now.Format("2006-01-02")
}

func resolveTriggerDeadlineDate(cfg *types.PipelineConfig, now time.Time) string {
	if strings.HasPrefix(cfg.Schedule.Trigger.Deadline, ":") {
		prev := now.Add(-time.Hour)
		return prev.Format("2006-01-02") + "T" + fmt.Sprintf("%02d", prev.Hour())
	}
	return now.Format("2006-01-02")
}

func resolveTriggerDeadlineTime(deadline, date, timezone string) time.Time {
	loc := lambda.ResolveTimezone(timezone)

	if strings.HasPrefix(deadline, ":") {
		minute, err := strconv.Atoi(strings.TrimPrefix(deadline, ":"))
		if err != nil {
			return time.Time{}
		}
		if len(date) < 13 || date[10] != 'T' {
			return time.Time{}
		}
		t, err := time.ParseInLocation("2006-01-02T15", date, loc)
		if err != nil {
			return time.Time{}
		}
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, minute, 0, 0, loc)
	}

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

func closeSensorTriggerWindow(ctx context.Context, d *lambda.Deps, pipelineID, scheduleID, date string, cfg *types.PipelineConfig, now time.Time) {
	tz := cfg.Schedule.Timezone
	if tz == "" && cfg.SLA != nil {
		tz = cfg.SLA.Timezone
	}
	triggerDeadline := resolveTriggerDeadlineTime(cfg.Schedule.Trigger.Deadline, date, tz)
	if triggerDeadline.IsZero() || triggerDeadline.After(now) {
		return
	}

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

	alertDetail := map[string]interface{}{
		"source":          "watchdog",
		"triggerDeadline": cfg.Schedule.Trigger.Deadline,
		"actionHint":      "auto-trigger window closed — use RERUN_REQUEST to restart",
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventSensorDeadlineExpired), pipelineID, scheduleID, date,
		fmt.Sprintf("trigger deadline expired for %s/%s/%s", pipelineID, scheduleID, date), alertDetail); err != nil {
		d.Logger.Warn("failed to publish sensor deadline expired event", "error", err, "pipeline", pipelineID)
	}

	d.Logger.Info("sensor trigger window closed",
		"pipelineId", pipelineID, "schedule", scheduleID, "date", date,
		"triggerDeadline", cfg.Schedule.Trigger.Deadline)
}
