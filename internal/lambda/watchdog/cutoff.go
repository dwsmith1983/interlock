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

// checkTriggerDeadlines evaluates trigger deadlines independently of SLA config.
func checkTriggerDeadlines(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()

	for id, cfg := range configs {
		if cfg.Schedule.Trigger == nil || cfg.Schedule.Trigger.Cutoff == "" {
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

func resolveTriggerDeadlineDate(cfg *types.PipelineConfig, now time.Time) string {
	if strings.HasPrefix(cfg.Schedule.Trigger.Cutoff, ":") {
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
	triggerDeadline := resolveTriggerDeadlineTime(cfg.Schedule.Trigger.Cutoff, date, tz)
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
		"triggerDeadline": cfg.Schedule.Trigger.Cutoff,
		"actionHint":      "auto-trigger window closed; use RERUN_REQUEST to restart",
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventSensorDeadlineExpired), pipelineID, scheduleID, date,
		fmt.Sprintf("trigger deadline expired for %s/%s/%s", pipelineID, scheduleID, date), alertDetail); err != nil {
		d.Logger.Warn("failed to publish sensor deadline expired event", "error", err, "pipeline", pipelineID)
	}

	d.Logger.Info("sensor trigger window closed",
		"pipelineId", pipelineID, "schedule", scheduleID, "date", date,
		"triggerDeadline", cfg.Schedule.Trigger.Cutoff)
}
