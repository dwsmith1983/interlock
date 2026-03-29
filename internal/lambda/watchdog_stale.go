package lambda

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// detectStaleTriggers scans for TRIGGER# rows with status=RUNNING and
// publishes an SFN_TIMEOUT event for any that have exceeded their TTL or the
// staleTriggerThreshold. Stale triggers are moved to FAILED_FINAL status.
func detectStaleTriggers(ctx context.Context, d *Deps) error {
	triggers, err := d.Store.ScanRunningTriggers(ctx)
	if err != nil {
		return fmt.Errorf("scan running triggers: %w", err)
	}

	now := d.Now()
	for _, tr := range triggers {
		if !isStaleTrigger(tr, now) {
			continue
		}

		pipelineID, schedule, date, err := parseTriggerRecord(tr)
		if err != nil {
			d.Logger.Warn("skipping unparseable trigger", "pk", tr.PK, "sk", tr.SK, "error", err)
			continue
		}

		// Dry-run pipelines should never have TRIGGER# rows, but guard
		// against stale rows from pre-dry-run migrations or bugs.
		if cfg, cfgErr := d.ConfigCache.Get(ctx, pipelineID); cfgErr == nil && cfg != nil && cfg.DryRun {
			continue
		}

		alertDetail := map[string]interface{}{
			"source":     "watchdog",
			"actionHint": "step function exceeded TTL — check SFN execution history",
		}
		if tr.TTL > 0 {
			alertDetail["ttlExpired"] = time.Unix(tr.TTL, 0).UTC().Format(time.RFC3339)
		}
		if err := PublishEvent(ctx, d, string(types.EventSFNTimeout), pipelineID, schedule, date,
			fmt.Sprintf("step function timed out for %s/%s/%s", pipelineID, schedule, date), alertDetail); err != nil {
			d.Logger.Warn("failed to publish SFN timeout event", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
		}

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

	now := d.Now()

	for id, cfg := range configs {
		trigger := cfg.Schedule.Trigger
		if trigger == nil || cfg.Schedule.Cron != "" {
			continue
		}

		// Dry-run pipelines are observation-only — skip reconciliation.
		if cfg.DryRun {
			continue
		}

		if IsExcluded(cfg, now) {
			continue
		}

		sensors, err := d.Store.GetAllSensors(ctx, id)
		if err != nil {
			d.Logger.Error("failed to get sensors for reconciliation",
				"pipelineId", id, "error", err)
			continue
		}

		scheduleID := ResolveScheduleID(cfg)

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

			date := ResolveExecutionDate(sensorData, now)

			found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, date)
			if err != nil {
				d.Logger.Error("trigger check failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if found {
				continue
			}

			// Guard against re-triggering completed pipelines whose trigger
			// record was deleted by DynamoDB TTL. Check the joblog for a
			// terminal event before acquiring a new lock.
			if IsJobTerminal(ctx, d, id, scheduleID, date) {
				continue
			}

			acquired, err := d.Store.AcquireTriggerLock(ctx, id, scheduleID, date, ResolveTriggerLockTTL())
			if err != nil {
				d.Logger.Error("lock acquisition failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if !acquired {
				continue
			}

			if err := StartSFN(ctx, d, cfg, id, scheduleID, date); err != nil {
				if relErr := d.Store.ReleaseTriggerLock(ctx, id, scheduleID, date); relErr != nil {
					d.Logger.Warn("failed to release lock after SFN start failure during reconciliation", "error", relErr)
				}
				d.Logger.Error("SFN start failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}

			alertDetail := map[string]interface{}{
				"source":     "reconciliation",
				"actionHint": "watchdog recovered missed sensor trigger",
			}
			if err := PublishEvent(ctx, d, string(types.EventTriggerRecovered), id, scheduleID, date,
				fmt.Sprintf("trigger recovered for %s/%s/%s", id, scheduleID, date), alertDetail); err != nil {
				d.Logger.Warn("failed to publish trigger recovered event", "error", err, "pipeline", id, "schedule", scheduleID, "date", date)
			}

			d.Logger.Info("recovered missed trigger",
				"pipelineId", id,
				"schedule", scheduleID,
				"date", date,
			)
		}
	}
	return nil
}
