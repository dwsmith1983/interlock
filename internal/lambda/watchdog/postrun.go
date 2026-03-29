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

const defaultSensorTimeout = 2 * time.Hour

// detectMissingPostRunSensors checks pipelines with PostRun config for missing
// post-run sensor data.
func detectMissingPostRunSensors(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	today := now.Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.PostRun == nil || len(cfg.PostRun.Rules) == 0 {
			continue
		}

		if cfg.DryRun {
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)

		tr, err := d.Store.GetTrigger(ctx, id, scheduleID, today)
		if err != nil {
			d.Logger.Error("trigger lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}
		if tr == nil || tr.Status != types.TriggerStatusCompleted {
			continue
		}

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

		completionTime, err := resolveCompletionTime(ctx, d, id, scheduleID, today)
		if err != nil {
			d.Logger.Error("completion time resolution failed",
				"pipelineId", id, "error", err)
			continue
		}
		if completionTime.IsZero() {
			continue
		}

		timeout := parseSensorTimeout(cfg.PostRun.SensorTimeout)

		if now.Before(completionTime.Add(timeout)) {
			continue
		}

		sensors, err := d.Store.GetAllSensors(ctx, id)
		if err != nil {
			d.Logger.Error("sensor lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}

		if hasPostRunSensorUpdate(cfg.PostRun.Rules, sensors, completionTime) {
			continue
		}

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
		if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunSensorMissing), id, scheduleID, today,
			fmt.Sprintf("post-run sensor missing for %s on %s", id, today), alertDetail); err != nil {
			d.Logger.Warn("failed to publish post-run sensor missing event", "error", err, "pipeline", id, "schedule", scheduleID, "date", today)
		}

		if err := d.Store.WriteSensor(ctx, id, dedupKey, map[string]interface{}{
			"alerted": "true",
		}); err != nil {
			d.Logger.Warn("failed to write post-run dedup marker", "error", err, "pipeline", id, "date", today)
		}

		d.Logger.Info("detected missing post-run sensor",
			"pipelineId", id, "schedule", scheduleID, "date", today)
	}
	return nil
}

func resolveCompletionTime(ctx context.Context, d *lambda.Deps, pipelineID, scheduleID, date string) (time.Time, error) {
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
