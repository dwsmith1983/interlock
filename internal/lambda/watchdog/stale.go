package watchdog

import (
	"context"
	"fmt"
	"strings"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// detectStaleTriggers scans for TRIGGER# rows with status=RUNNING and
// publishes an SFN_TIMEOUT event for any past their staleAt, then sets them
// FAILED_FINAL. A row whose publish fails stays RUNNING for the next pass.
// A dry-run pipeline never takes a trigger lock, so a RUNNING row for one is
// left from before it switched to dry-run and is closed too.
func detectStaleTriggers(ctx context.Context, d *lambda.Deps) error {
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

		alertDetail := map[string]interface{}{
			"source":     "watchdog",
			"actionHint": "step function passed its stale deadline: check SFN execution history",
			"staleAt":    time.Unix(tr.StaleAt, 0).UTC().Format(time.RFC3339),
		}
		if err := lambda.PublishEvent(ctx, d, string(types.EventSFNTimeout), pipelineID, schedule, date,
			fmt.Sprintf("step function timed out for %s/%s/%s", pipelineID, schedule, date), alertDetail); err != nil {
			d.Logger.Warn("failed to publish SFN timeout event", "error", err, "pipeline", pipelineID, "schedule", schedule, "date", date)
			continue // still RUNNING with its staleAt, so the next pass retries
		}

		if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
			d.Logger.Error("failed to set trigger status to FAILED_FINAL",
				"pipelineId", pipelineID, "schedule", schedule, "date", date, "error", err)
			continue
		}

		d.Logger.Info("detected stale trigger",
			"pipelineId", pipelineID, "schedule", schedule, "date", date)
	}
	return nil
}

// isStaleTrigger reports whether a RUNNING trigger row is past its staleAt.
// A row with no staleAt is never stale.
func isStaleTrigger(tr types.ControlRecord, now time.Time) bool {
	return tr.StaleAt > 0 && now.Unix() > tr.StaleAt
}

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
// pipelines, with or without schedule.cron (cron is only the start-by time).
// Self-heals missed triggers caused by silent completion-write failures.
func reconcileSensorTriggers(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()

	for id, cfg := range configs {
		trigger := cfg.Schedule.Trigger
		if trigger == nil {
			continue
		}

		if cfg.DryRun {
			continue
		}

		if lambda.IsExcluded(cfg, now) {
			continue
		}

		sensors, err := d.Store.GetAllSensors(ctx, id)
		if err != nil {
			d.Logger.Error("failed to get sensors for reconciliation",
				"pipelineId", id, "error", err)
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)

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

			date := lambda.ResolveExecutionDate(sensorData, now)

			found, err := d.Store.HasTriggerForDate(ctx, id, scheduleID, date)
			if err != nil {
				d.Logger.Error("trigger check failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if found {
				continue
			}

			// After the cutoff nothing starts: close the date instead.
			if lambda.CutoffPassed(cfg, date, now) {
				if _, err := lambda.CloseRunAtCutoff(ctx, d, cfg, id, scheduleID, date, "reconciliation"); err != nil {
					d.Logger.Error("cutoff close failed during reconciliation",
						"pipelineId", id, "date", date, "error", err)
				}
				continue
			}

			if lambda.IsJobTerminal(ctx, d, id, scheduleID, date) {
				continue
			}
			released := lambda.EvaluationReleased(ctx, d, id, scheduleID, date)

			acquired, err := d.Store.AcquireTriggerLock(ctx, id, scheduleID, date, lambda.ResolveTriggerLockTTL())
			if err != nil {
				d.Logger.Error("lock acquisition failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}
			if !acquired {
				continue
			}

			if err := lambda.StartSFN(ctx, d, cfg, id, scheduleID, date); err != nil {
				if relErr := d.Store.ReleaseTriggerLock(ctx, id, scheduleID, date); relErr != nil {
					d.Logger.Warn("failed to release lock after SFN start failure during reconciliation", "error", relErr)
				}
				d.Logger.Error("SFN start failed during reconciliation",
					"pipelineId", id, "date", date, "error", err)
				continue
			}

			// A restart after a released evaluation is a normal start, not a
			// recovered trigger.
			eventType, msg, hint := types.EventTriggerRecovered, "trigger recovered for %s/%s/%s", "watchdog recovered missed sensor trigger"
			if released {
				eventType, msg, hint = types.EventJobTriggered, "evaluation restarted for %s/%s/%s", "evaluation restarted after its window closed"
			}
			alertDetail := map[string]interface{}{"source": "reconciliation", "actionHint": hint}
			if err := lambda.PublishEvent(ctx, d, string(eventType), id, scheduleID, date,
				fmt.Sprintf(msg, id, scheduleID, date), alertDetail); err != nil {
				d.Logger.Warn("failed to publish reconciliation event", "type", eventType, "error", err, "pipeline", id, "schedule", scheduleID, "date", date)
			}

			d.Logger.Info("recovered missed trigger",
				"pipelineId", id, "schedule", scheduleID, "date", date, "type", eventType)
		}
	}
	return nil
}
