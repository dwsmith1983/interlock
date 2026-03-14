package lambda

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

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

		// Dry-run pipelines are observation-only — skip post-run sensor checks.
		if cfg.DryRun {
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

// detectRelativeSLABreaches checks pipelines with MaxDuration SLA config for
// breaches. This is a defense-in-depth fallback: if the EventBridge Scheduler
// fails to fire the relative SLA breach alert, the watchdog catches it.
//
// Both today and yesterday are checked because stream_router writes the
// first-sensor-arrival key using ResolveExecutionDate(), which for T+1
// sensor-triggered pipelines produces yesterday's date. Checking both dates
// covers the cross-day boundary.
func detectRelativeSLABreaches(ctx context.Context, d *Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.now()
	datesToCheck := []string{
		now.Format("2006-01-02"),
		now.AddDate(0, 0, -1).Format("2006-01-02"),
	}

	for id, cfg := range configs {
		if cfg.SLA == nil || cfg.SLA.MaxDuration == "" {
			continue
		}

		// Dry-run pipelines are observation-only — skip relative SLA checks.
		if cfg.DryRun {
			continue
		}

		maxDur, err := time.ParseDuration(cfg.SLA.MaxDuration)
		if err != nil {
			d.Logger.Warn("invalid maxDuration in SLA config",
				"pipelineId", id, "maxDuration", cfg.SLA.MaxDuration, "error", err)
			continue
		}

		scheduleID := resolveScheduleID(cfg)

		for _, checkDate := range datesToCheck {
			checkRelativeSLAForDate(ctx, d, id, cfg, scheduleID, checkDate, maxDur, now)
		}
	}
	return nil
}

// checkRelativeSLAForDate checks a single date for a relative SLA breach on
// the given pipeline. It looks up the first-sensor-arrival marker, verifies
// the breach window has elapsed, and publishes an alert if needed.
func checkRelativeSLAForDate(ctx context.Context, d *Deps, id string, cfg *types.PipelineConfig, scheduleID, checkDate string, maxDur time.Duration, now time.Time) {
	arrivalKey := "first-sensor-arrival#" + checkDate
	arrivalData, err := d.Store.GetSensorData(ctx, id, arrivalKey)
	if err != nil {
		d.Logger.Error("first-sensor-arrival lookup failed",
			"pipelineId", id, "date", checkDate, "error", err)
		return
	}
	if arrivalData == nil {
		return
	}

	arrivedAtStr, ok := arrivalData["arrivedAt"].(string)
	if !ok || arrivedAtStr == "" {
		return
	}
	arrivedAt, err := time.Parse(time.RFC3339, arrivedAtStr)
	if err != nil {
		d.Logger.Warn("invalid arrivedAt in first-sensor-arrival",
			"pipelineId", id, "arrivedAt", arrivedAtStr, "error", err)
		return
	}

	// Check if the relative SLA has been breached.
	breachAt := arrivedAt.Add(maxDur)
	if now.Before(breachAt) {
		return
	}

	// Skip if pipeline already completed or permanently failed.
	tr, err := d.Store.GetTrigger(ctx, id, scheduleID, checkDate)
	if err != nil {
		d.Logger.Warn("trigger lookup failed in relative SLA check",
			"pipelineId", id, "date", checkDate, "error", err)
		return
	}
	if tr != nil && (tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal) {
		return
	}
	if isJobTerminal(ctx, d, id, scheduleID, checkDate) {
		return
	}

	// Check dedup marker to avoid re-alerting on subsequent watchdog runs.
	// The dedup key includes checkDate to avoid cross-date collisions.
	dedupKey := "relative-sla-breach-check#" + checkDate
	dedupData, err := d.Store.GetSensorData(ctx, id, dedupKey)
	if err != nil {
		d.Logger.Error("dedup marker lookup failed for relative SLA breach",
			"pipelineId", id, "date", checkDate, "error", err)
		return
	}
	if dedupData != nil {
		return
	}

	alertDetail := map[string]interface{}{
		"source":          "watchdog",
		"maxDuration":     cfg.SLA.MaxDuration,
		"sensorArrivalAt": arrivedAtStr,
		"breachAt":        breachAt.UTC().Format(time.RFC3339),
		"actionHint":      "relative SLA breached — pipeline has exceeded maxDuration since first sensor arrival",
	}
	if err := publishEvent(ctx, d, string(types.EventRelativeSLABreach), id, scheduleID, checkDate,
		fmt.Sprintf("relative SLA breach for %s on %s", id, checkDate), alertDetail); err != nil {
		d.Logger.Warn("failed to publish relative SLA breach event",
			"error", err, "pipeline", id, "date", checkDate)
	}

	// Write dedup marker.
	if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{
		"alerted": "true",
	}); err != nil {
		d.Logger.Warn("failed to write relative SLA breach dedup marker",
			"error", err, "pipeline", id, "date", checkDate)
	}

	d.Logger.Info("detected relative SLA breach",
		"pipelineId", id,
		"schedule", scheduleID,
		"date", checkDate,
		"sensorArrivalAt", arrivedAtStr,
		"breachAt", breachAt.UTC().Format(time.RFC3339),
	)
}
