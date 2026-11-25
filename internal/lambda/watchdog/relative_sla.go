package watchdog

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	pkgsla "github.com/dwsmith1983/interlock/pkg/sla"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// relativeSLALookback is how long after its breach time a date's
// RELATIVE_SLA_BREACH may still be sent, which keeps each pass's reads to the
// dates near their breach.
const relativeSLALookback = 24 * time.Hour

// detectRelativeSLABreaches checks the first sensor arrivals of pipelines with
// MaxDuration SLA config, for run dates (hourly ones included) from
// runKeysDaysBack to runKeysDaysAhead in UTC: once per date,
// RELATIVE_SLA_WARNING between the warning and breach times and
// RELATIVE_SLA_BREACH from the breach time until relativeSLALookback after it,
// while the run has not finished.
func detectRelativeSLABreaches(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	const keyPrefix = "first-sensor-arrival#"
	from := keyPrefix + now.UTC().AddDate(0, 0, -runKeysDaysBack).Format("2006-01-02")
	to := keyPrefix + now.UTC().AddDate(0, 0, runKeysDaysAhead).Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.SLA == nil || cfg.SLA.MaxDuration == "" {
			continue
		}

		if cfg.DryRun {
			continue
		}

		arrivals, err := d.Store.GetSensorsBetween(ctx, id, from, to)
		if err != nil {
			d.Logger.Error("first-sensor-arrival lookup failed", "pipelineId", id, "error", err)
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)
		for _, key := range slices.Sorted(maps.Keys(arrivals)) {
			checkDate := strings.TrimPrefix(key, keyPrefix)
			if !lambda.IsValidExecutionDate(checkDate) {
				d.Logger.Warn("skipping malformed first-sensor-arrival key", "pipelineId", id, "key", key)
				continue
			}
			checkRelativeSLAForDate(ctx, d, id, cfg, scheduleID, checkDate, arrivals[key], now)
		}
	}
	return nil
}

func checkRelativeSLAForDate(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, scheduleID, checkDate string, arrivalData map[string]interface{}, now time.Time) {
	arrivedAtStr, ok := arrivalData["arrivedAt"].(string)
	if !ok || arrivedAtStr == "" {
		return
	}
	breachAt, warningAt, err := pkgsla.CalculateRelativeDeadline(arrivedAtStr, cfg.SLA.MaxDuration, cfg.SLA.ExpectedDuration)
	if err != nil {
		d.Logger.Warn("relative SLA deadline failed",
			"pipelineId", id, "date", checkDate, "error", err)
		return
	}
	if now.Before(warningAt) || now.Sub(breachAt) > relativeSLALookback {
		return
	}
	if now.Before(breachAt) {
		warnRelativeSLA(ctx, d, id, cfg, scheduleID, checkDate, arrivedAtStr, warningAt, breachAt)
		return
	}

	dedupKey := "relative-sla-breach-check#" + checkDate
	if !relativeSLAAlertDue(ctx, d, id, scheduleID, checkDate, dedupKey) {
		return
	}

	alertDetail := map[string]interface{}{
		"source":          "watchdog",
		"maxDuration":     cfg.SLA.MaxDuration,
		"sensorArrivalAt": arrivedAtStr,
		"breachAt":        breachAt.UTC().Format(time.RFC3339),
		"actionHint":      "relative SLA breached; pipeline has exceeded maxDuration since first sensor arrival",
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventRelativeSLABreach), id, scheduleID, checkDate,
		fmt.Sprintf("relative SLA breach for %s on %s", id, checkDate), alertDetail); err != nil {
		d.Logger.Warn("failed to publish relative SLA breach event",
			"error", err, "pipeline", id, "date", checkDate)
		return // no marker, so the next pass retries
	}

	if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{
		"alerted": "true",
	}); err != nil {
		d.Logger.Warn("failed to write relative SLA breach dedup marker",
			"error", err, "pipeline", id, "date", checkDate)
	}

	d.Logger.Info("detected relative SLA breach",
		"pipelineId", id, "schedule", scheduleID, "date", checkDate,
		"sensorArrivalAt", arrivedAtStr, "breachAt", breachAt.UTC().Format(time.RFC3339))
}

// warnRelativeSLA publishes RELATIVE_SLA_WARNING once for a date whose run has
// not finished. The marker follows a successful publish only, so a failed
// publish is retried on the next pass.
func warnRelativeSLA(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, scheduleID, checkDate, arrivedAtStr string, warningAt, breachAt time.Time) {
	dedupKey := "relative-sla-warning-check#" + checkDate
	if !relativeSLAAlertDue(ctx, d, id, scheduleID, checkDate, dedupKey) {
		return
	}
	breachStr := breachAt.UTC().Format(time.RFC3339)
	alertDetail := map[string]interface{}{
		"source":           "watchdog",
		"maxDuration":      cfg.SLA.MaxDuration,
		"expectedDuration": cfg.SLA.ExpectedDuration,
		"sensorArrivalAt":  arrivedAtStr,
		"warningAt":        warningAt.UTC().Format(time.RFC3339),
		"breachAt":         breachStr,
		"actionHint":       "relative SLA at risk; the run has not finished and the breach is at " + breachStr,
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventRelativeSLAWarning), id, scheduleID, checkDate,
		fmt.Sprintf("relative SLA warning for %s on %s", id, checkDate), alertDetail); err != nil {
		d.Logger.Warn("failed to publish relative SLA warning event",
			"error", err, "pipeline", id, "date", checkDate)
		return
	}
	if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{"alerted": "true"}); err != nil {
		d.Logger.Warn("failed to write relative SLA warning dedup marker",
			"error", err, "pipeline", id, "date", checkDate)
	}
	d.Logger.Info("detected relative SLA warning",
		"pipelineId", id, "schedule", scheduleID, "date", checkDate,
		"warningAt", alertDetail["warningAt"], "breachAt", breachStr)
}

// relativeSLAAlertDue reports whether a relative SLA alert is due: the run has
// not finished and the alert's dedup marker is absent. A lookup failure skips
// the alert for this pass.
func relativeSLAAlertDue(ctx context.Context, d *lambda.Deps, id, scheduleID, checkDate, dedupKey string) bool {
	tr, err := d.Store.GetTrigger(ctx, id, scheduleID, checkDate)
	if err != nil {
		d.Logger.Warn("trigger lookup failed in relative SLA check",
			"pipelineId", id, "date", checkDate, "error", err)
		return false
	}
	if finished, _ := lambda.RunFinished(ctx, d, tr, id, scheduleID, checkDate); finished {
		return false
	}
	dedupData, err := d.Store.GetSensorData(ctx, id, dedupKey)
	if err != nil {
		d.Logger.Error("dedup marker lookup failed for relative SLA check",
			"pipelineId", id, "date", checkDate, "dedupKey", dedupKey, "error", err)
		return false
	}
	return dedupData == nil
}
