package watchdog

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	pkgsla "github.com/dwsmith1983/interlock/pkg/sla"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// detectRelativeSLABreaches checks pipelines with MaxDuration SLA config: once
// per date, RELATIVE_SLA_WARNING between the warning and breach times and
// RELATIVE_SLA_BREACH from the breach time, while the run has not finished.
func detectRelativeSLABreaches(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	datesToCheck := []string{
		now.Format("2006-01-02"),
		now.AddDate(0, 0, -1).Format("2006-01-02"),
	}

	for id, cfg := range configs {
		if cfg.SLA == nil || cfg.SLA.MaxDuration == "" {
			continue
		}

		if cfg.DryRun {
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)

		for _, checkDate := range datesToCheck {
			checkRelativeSLAForDate(ctx, d, id, cfg, scheduleID, checkDate, now)
		}
	}
	return nil
}

func checkRelativeSLAForDate(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, scheduleID, checkDate string, now time.Time) {
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
	breachAt, warningAt, err := pkgsla.CalculateRelativeDeadline(arrivedAtStr, cfg.SLA.MaxDuration, cfg.SLA.ExpectedDuration)
	if err != nil {
		d.Logger.Warn("relative SLA deadline failed",
			"pipelineId", id, "date", checkDate, "error", err)
		return
	}
	if now.Before(warningAt) {
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
