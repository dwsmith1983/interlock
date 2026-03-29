package watchdog

import (
	"context"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// detectRelativeSLABreaches checks pipelines with MaxDuration SLA config.
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

		maxDur, err := time.ParseDuration(cfg.SLA.MaxDuration)
		if err != nil {
			d.Logger.Warn("invalid maxDuration in SLA config",
				"pipelineId", id, "maxDuration", cfg.SLA.MaxDuration, "error", err)
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)

		for _, checkDate := range datesToCheck {
			checkRelativeSLAForDate(ctx, d, id, cfg, scheduleID, checkDate, maxDur, now)
		}
	}
	return nil
}

func checkRelativeSLAForDate(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, scheduleID, checkDate string, maxDur time.Duration, now time.Time) {
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

	breachAt := arrivedAt.Add(maxDur)
	if now.Before(breachAt) {
		return
	}

	tr, err := d.Store.GetTrigger(ctx, id, scheduleID, checkDate)
	if err != nil {
		d.Logger.Warn("trigger lookup failed in relative SLA check",
			"pipelineId", id, "date", checkDate, "error", err)
		return
	}
	if tr != nil && (tr.Status == types.TriggerStatusCompleted || tr.Status == types.TriggerStatusFailedFinal) {
		return
	}
	if lambda.IsJobTerminal(ctx, d, id, scheduleID, checkDate) {
		return
	}

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
	if err := lambda.PublishEvent(ctx, d, string(types.EventRelativeSLABreach), id, scheduleID, checkDate,
		fmt.Sprintf("relative SLA breach for %s on %s", id, checkDate), alertDetail); err != nil {
		d.Logger.Warn("failed to publish relative SLA breach event",
			"error", err, "pipeline", id, "date", checkDate)
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
