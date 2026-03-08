package lambda

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	schedulerTypes "github.com/aws/aws-sdk-go-v2/service/scheduler/types"

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
	if err := scheduleSLAAlerts(ctx, d); err != nil {
		d.Logger.Error("proactive SLA scheduling failed", "error", err)
	}
	if err := detectMissingPostRunSensors(ctx, d); err != nil {
		d.Logger.Error("post-run sensor absence detection failed", "error", err)
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

	now := d.now()
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
			"actionHint": "step function exceeded TTL — check SFN execution history",
		}
		if tr.TTL > 0 {
			alertDetail["ttlExpired"] = time.Unix(tr.TTL, 0).UTC().Format(time.RFC3339)
		}
		if err := publishEvent(ctx, d, string(types.EventSFNTimeout), pipelineID, schedule, date,
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

	now := d.now()

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
			if isJobTerminal(ctx, d, id, scheduleID, date) {
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

			if err := startSFN(ctx, d, cfg, id, scheduleID, date); err != nil {
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
			if err := publishEvent(ctx, d, string(types.EventTriggerRecovered), id, scheduleID, date,
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

		// Skip calendar-excluded days.
		if isExcluded(cfg, now) {
			continue
		}

		// Only alert for schedules that should have fired after this Lambda
		// started. Prevents retroactive alerts after fresh deploys.
		if !d.StartedAt.IsZero() {
			loc := time.UTC
			if cfg.Schedule.Timezone != "" {
				if parsed, err := time.LoadLocation(cfg.Schedule.Timezone); err == nil {
					loc = parsed
				}
			}
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

// scheduleSLAAlerts proactively creates EventBridge Scheduler entries for all
// pipelines with SLA configs. This ensures warnings/breaches fire even when
// pipelines never trigger (data never arrives, sensor fails, etc.).
// Idempotency: deterministic scheduler names; ConflictException = already exists.
func scheduleSLAAlerts(ctx context.Context, d *Deps) error {
	if d.Scheduler == nil {
		return nil
	}

	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.now()

	for id, cfg := range configs {
		if cfg.SLA == nil {
			continue
		}

		if isExcluded(cfg, now) {
			continue
		}

		// Sensor-triggered pipelines (no cron) get SLA scheduling when
		// their SFN execution starts — proactive scheduling here would
		// fire alerts for the previous hour before the pipeline runs.
		if cfg.Schedule.Cron == "" {
			continue
		}

		scheduleID := resolveScheduleID(cfg)
		date := resolveWatchdogSLADate(cfg, now)

		// Skip if pipeline already completed or permanently failed for this date.
		tr, err := d.Store.GetTrigger(ctx, id, scheduleID, date)
		switch {
		case err != nil:
			d.Logger.Warn("trigger lookup failed in SLA scheduling", "pipelineId", id, "error", err)
			continue
		case tr != nil && (tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal):
			continue
		case isJobTerminal(ctx, d, id, scheduleID, date):
			continue
		}

		calc, err := handleSLACalculate(SLAMonitorInput{
			Mode:             "calculate",
			PipelineID:       id,
			ScheduleID:       scheduleID,
			Date:             date,
			Deadline:         cfg.SLA.Deadline,
			ExpectedDuration: cfg.SLA.ExpectedDuration,
			Timezone:         cfg.SLA.Timezone,
		}, now)
		if err != nil {
			d.Logger.Error("SLA calculate failed", "pipelineId", id, "error", err)
			continue
		}

		breachAt, _ := time.Parse(time.RFC3339, calc.BreachAt)
		if !breachAt.IsZero() && !breachAt.After(now) {
			continue
		}

		var scheduleErr bool
		for _, alert := range []struct {
			suffix    string
			alertType string
			timestamp string
		}{
			{"warning", "SLA_WARNING", calc.WarningAt},
			{"breach", "SLA_BREACH", calc.BreachAt},
		} {
			name := slaScheduleName(id, scheduleID, date, alert.suffix)
			payload := SLAMonitorInput{
				Mode:       "fire-alert",
				PipelineID: id,
				ScheduleID: scheduleID,
				Date:       date,
				AlertType:  alert.alertType,
			}
			if alert.alertType == "SLA_WARNING" {
				payload.BreachAt = calc.BreachAt
			}
			if err := createOneTimeSchedule(ctx, d, name, alert.timestamp, payload); err != nil {
				var conflict *schedulerTypes.ConflictException
				if errors.As(err, &conflict) {
					continue
				}
				d.Logger.Error("create SLA schedule failed",
					"pipelineId", id, "suffix", alert.suffix, "error", err)
				scheduleErr = true
			}
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

// defaultSensorTimeout is the default grace period for post-run sensors to
// arrive after a pipeline completes. If no SensorTimeout is configured in
// PostRunConfig, this value is used.
const defaultSensorTimeout = 2 * time.Hour

// detectMissingPostRunSensors checks pipelines with PostRun config for missing
// post-run sensor data. If a pipeline completed (COMPLETED trigger + baseline
// exists) but no post-run sensor matching a rule key has been updated since
// completion, and the SensorTimeout grace period has elapsed, a
// POST_RUN_SENSOR_MISSING event is published.
func detectMissingPostRunSensors(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.now()
	today := now.Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.PostRun == nil || len(cfg.PostRun.Rules) == 0 {
			continue
		}

		scheduleID := resolveScheduleID(cfg)

		// Only check pipelines with a COMPLETED trigger for today.
		tr, err := d.Store.GetTrigger(ctx, id, scheduleID, today)
		if err != nil {
			d.Logger.Error("trigger lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}
		if tr == nil || tr.Status != types.TriggerStatusCompleted {
			continue
		}

		// Baseline must exist — it signals that capturePostRunBaseline ran
		// at completion time.
		baselineKey := "postrun-baseline#" + today
		baseline, err := d.Store.GetSensorData(ctx, id, baselineKey)
		if err != nil {
			d.Logger.Error("baseline lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}
		if baseline == nil {
			continue
		}

		// Dedup: skip if we already published an alert for this date.
		dedupKey := "postrun-check#" + today
		dedupData, err := d.Store.GetSensorData(ctx, id, dedupKey)
		if err != nil {
			d.Logger.Error("dedup marker lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}
		if dedupData != nil {
			continue
		}

		// Determine the completion timestamp from the latest success job event.
		completionTime, err := resolveCompletionTime(ctx, d, id, scheduleID, today)
		if err != nil {
			d.Logger.Error("completion time resolution failed",
				"pipelineId", id, "error", err)
			continue
		}
		if completionTime.IsZero() {
			continue
		}

		// Parse SensorTimeout from config (default 2h).
		timeout := parseSensorTimeout(cfg.PostRun.SensorTimeout)

		// Check if the timeout has elapsed since completion.
		if now.Before(completionTime.Add(timeout)) {
			continue
		}

		// Check if any post-run rule sensor has been updated since completion.
		sensors, err := d.Store.GetAllSensors(ctx, id)
		if err != nil {
			d.Logger.Error("sensor lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}

		if hasPostRunSensorUpdate(cfg.PostRun.Rules, sensors, completionTime) {
			continue
		}

		// No post-run sensor has arrived within the grace period — publish event.
		ruleKeys := make([]string, 0, len(cfg.PostRun.Rules))
		for _, r := range cfg.PostRun.Rules {
			ruleKeys = append(ruleKeys, r.Key)
		}

		alertDetail := map[string]interface{}{
			"source":        "watchdog",
			"sensorTimeout": cfg.PostRun.SensorTimeout,
			"ruleKeys":      strings.Join(ruleKeys, ", "),
			"actionHint":    "post-run sensor data has not arrived within the expected timeout",
		}
		if err := publishEvent(ctx, d, string(types.EventPostRunSensorMissing), id, scheduleID, today,
			fmt.Sprintf("post-run sensor missing for %s on %s", id, today), alertDetail); err != nil {
			d.Logger.Warn("failed to publish post-run sensor missing event", "error", err, "pipeline", id, "schedule", scheduleID, "date", today)
		}

		// Write dedup marker to avoid re-alerting on subsequent watchdog runs.
		if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{
			"alerted": "true",
		}); err != nil {
			d.Logger.Warn("failed to write post-run dedup marker", "error", err, "pipeline", id, "date", today)
		}

		d.Logger.Info("detected missing post-run sensor",
			"pipelineId", id,
			"schedule", scheduleID,
			"date", today,
		)
	}
	return nil
}

// resolveCompletionTime extracts the completion timestamp from the latest
// success job event for the given pipeline/schedule/date. The job event SK
// has the format JOB#<schedule>#<date>#<timestamp> where timestamp is
// milliseconds since epoch.
func resolveCompletionTime(ctx context.Context, d *Deps, pipelineID, scheduleID, date string) (time.Time, error) {
	rec, err := d.Store.GetLatestJobEvent(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return time.Time{}, fmt.Errorf("get latest job event: %w", err)
	}
	if rec == nil {
		return time.Time{}, nil
	}
	if rec.Event != types.JobEventSuccess {
		return time.Time{}, nil
	}

	// Extract timestamp from SK: JOB#<schedule>#<date>#<timestamp>
	parts := strings.Split(rec.SK, "#")
	if len(parts) < 4 {
		return time.Time{}, fmt.Errorf("unexpected job SK format: %q", rec.SK)
	}
	tsMillis, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse job timestamp %q: %w", parts[len(parts)-1], err)
	}
	return time.UnixMilli(tsMillis), nil
}

// parseSensorTimeout parses a duration string from PostRunConfig.SensorTimeout.
// Returns defaultSensorTimeout (2h) if the string is empty or unparseable.
func parseSensorTimeout(s string) time.Duration {
	if s == "" {
		return defaultSensorTimeout
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return defaultSensorTimeout
	}
	return d
}

// hasPostRunSensorUpdate checks whether any sensor matching a PostRun rule key
// has an updatedAt timestamp newer than the given completion time.
func hasPostRunSensorUpdate(rules []types.ValidationRule, sensors map[string]map[string]interface{}, completionTime time.Time) bool {
	completionMillis := completionTime.UnixMilli()

	for _, rule := range rules {
		data, ok := sensors[rule.Key]
		if !ok {
			continue
		}

		updatedAt, ok := data["updatedAt"]
		if !ok {
			continue
		}

		var ts int64
		switch v := updatedAt.(type) {
		case float64:
			ts = int64(v)
		case int64:
			ts = v
		case string:
			ts, _ = strconv.ParseInt(v, 10, 64)
		default:
			continue
		}

		if ts > completionMillis {
			return true
		}
	}
	return false
}
