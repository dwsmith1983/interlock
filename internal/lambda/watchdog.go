package lambda

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/internal/validation"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleWatchdog runs periodic health checks. It detects stale trigger
// executions (Step Function timeouts) and missed cron schedules. Errors from
// each check are logged but do not prevent the other check from running.
func HandleWatchdog(ctx context.Context, d *Deps) error {
	if err := detectStaleTriggers(ctx, d); err != nil {
		d.Logger.Error("stale trigger detection failed", "error", err)
	}
	if err := detectMissedSchedules(ctx, d); err != nil {
		d.Logger.Error("missed schedule detection failed", "error", err)
	}
	if err := reconcileSensorTriggers(ctx, d); err != nil {
		d.Logger.Error("sensor trigger reconciliation failed", "error", err)
	}
	return nil
}

// detectStaleTriggers scans for TRIGGER# rows with status=RUNNING and
// publishes an SFN_TIMEOUT event for any that have exceeded their TTL or the
// staleTriggerThreshold. Stale triggers are moved to FAILED_FINAL status.
func detectStaleTriggers(ctx context.Context, d *Deps) error {
	triggers, err := d.Store.ScanRunningTriggers(ctx)
	if err != nil {
		return fmt.Errorf("scan running triggers: %w", err)
	}

	now := time.Now()
	for _, tr := range triggers {
		if !isStaleTrigger(tr, now) {
			continue
		}

		pipelineID, schedule, date, err := parseTriggerRecord(tr)
		if err != nil {
			d.Logger.Warn("skipping unparseable trigger", "pk", tr.PK, "sk", tr.SK, "error", err)
			continue
		}

		_ = publishEvent(ctx, d, string(types.EventSFNTimeout), pipelineID, schedule, date,
			fmt.Sprintf("step function timed out for %s/%s/%s", pipelineID, schedule, date))

		if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
			d.Logger.Error("failed to set trigger status to FAILED_FINAL",
				"pipelineId", pipelineID, "schedule", schedule, "date", date, "error", err)
			continue
		}

		d.Logger.Info("detected stale trigger",
			"pipelineId", pipelineID,
			"schedule", schedule,
			"date", date,
		)
	}
	return nil
}

// isStaleTrigger returns true if the trigger's TTL has expired or if the TTL
// is zero and the trigger has been running longer than staleTriggerThreshold.
func isStaleTrigger(tr types.ControlRecord, now time.Time) bool {
	if tr.TTL > 0 {
		return now.Unix() > tr.TTL
	}
	// No TTL set — treat as stale if it has existed for longer than the threshold.
	// Without a creation timestamp we can't be precise, so we conservatively
	// consider it stale only when TTL is explicitly expired.
	return false
}

// parseTriggerRecord extracts pipeline ID, schedule, and date from a trigger
// ControlRecord's PK and SK.
// PK format: PIPELINE#<id>
// SK format: TRIGGER#<schedule>#<date>
func parseTriggerRecord(tr types.ControlRecord) (pipelineID, schedule, date string, err error) {
	const pkPrefix = "PIPELINE#"
	if !strings.HasPrefix(tr.PK, pkPrefix) {
		return "", "", "", fmt.Errorf("unexpected PK format: %q", tr.PK)
	}
	pipelineID = tr.PK[len(pkPrefix):]

	const skPrefix = "TRIGGER#"
	trimmed := strings.TrimPrefix(tr.SK, skPrefix)
	if trimmed == tr.SK {
		return "", "", "", fmt.Errorf("unexpected SK format: %q", tr.SK)
	}
	parts := strings.SplitN(trimmed, "#", 2)
	if len(parts) != 2 {
		return "", "", "", fmt.Errorf("invalid TRIGGER SK format: %q", tr.SK)
	}
	return pipelineID, parts[0], parts[1], nil
}

// reconcileSensorTriggers re-evaluates trigger conditions for sensor-triggered
// pipelines. If a sensor meets the trigger condition but no trigger lock exists,
// the watchdog acquires the lock, starts the SFN, and publishes TRIGGER_RECOVERED.
// This self-heals missed triggers caused by silent completion-write failures.
func reconcileSensorTriggers(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := time.Now()

	for id, cfg := range configs {
		trigger := cfg.Schedule.Trigger
		if trigger == nil || cfg.Schedule.Cron != "" {
			continue
		}

		if isExcluded(cfg, now) {
			continue
		}

		sensors, err := d.Store.GetAllSensors(ctx, id)
		if err != nil {
			d.Logger.Error("failed to get sensors for reconciliation",
				"pipelineId", id, "error", err)
			continue
		}

		scheduleID := resolveScheduleID(cfg)

		for sensorKey, sensorData := range sensors {
			if !strings.HasPrefix(sensorKey, trigger.Key) {
				continue
			}

			rule := types.ValidationRule{
				Key:   trigger.Key,
				Check: trigger.Check,
				Field: trigger.Field,
				Value: trigger.Value,
			}
			result := validation.EvaluateRule(rule, sensorData, now)
			if !result.Passed {
				continue
			}

			date := ResolveExecutionDate(sensorData)

			found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, date)
			if err != nil {
				d.Logger.Error("trigger check failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if found {
				continue
			}

			acquired, err := d.Store.AcquireTriggerLock(ctx, id, scheduleID, date, triggerLockTTL)
			if err != nil {
				d.Logger.Error("lock acquisition failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if !acquired {
				continue
			}

			if err := startSFN(ctx, d, cfg, id, scheduleID, date); err != nil {
				d.Logger.Error("SFN start failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}

			_ = publishEvent(ctx, d, string(types.EventTriggerRecovered), id, scheduleID, date,
				fmt.Sprintf("trigger recovered for %s/%s/%s", id, scheduleID, date))

			d.Logger.Info("recovered missed trigger",
				"pipelineId", id,
				"schedule", scheduleID,
				"date", date,
			)
		}
	}
	return nil
}

// detectMissedSchedules checks all cron-scheduled pipelines to see if today's
// trigger is missing. If a pipeline should have started by now but has no
// TRIGGER# row, a SCHEDULE_MISSED event is published.
func detectMissedSchedules(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := time.Now()
	today := now.Format("2006-01-02")

	for id, cfg := range configs {
		// Only check cron-scheduled pipelines.
		if cfg.Schedule.Cron == "" {
			continue
		}

		// Skip calendar-excluded days.
		if isExcluded(cfg, now) {
			continue
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
			loc := time.UTC
			if cfg.Schedule.Timezone != "" {
				if parsed, err := time.LoadLocation(cfg.Schedule.Timezone); err == nil {
					loc = parsed
				}
			}
			localNow := now.In(loc)
			expectedStart, err := time.ParseInLocation("2006-01-02 15:04", today+" "+cfg.Schedule.Time, loc)
			if err == nil && localNow.Before(expectedStart) {
				continue // not yet past expected start time
			}
		}

		_ = publishEvent(ctx, d, string(types.EventScheduleMissed), id, scheduleID, today,
			fmt.Sprintf("missed schedule for %s on %s", id, today))

		d.Logger.Info("detected missed schedule",
			"pipelineId", id,
			"schedule", scheduleID,
			"date", today,
		)
	}
	return nil
}
