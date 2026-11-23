package watchdog

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

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

		// No baseline row means no post-run sensor existed at completion.
		baseline, err := d.Store.GetSensorData(ctx, id, "postrun-baseline#"+today)
		if err != nil {
			d.Logger.Error("baseline lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
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

		// The run's completion on the trigger row starts the sensor timeout.
		completionTime, err := time.Parse(time.RFC3339, tr.CompletedAt)
		if err != nil {
			if tr.CompletedAt != "" {
				d.Logger.Warn("unparseable completedAt in post-run sensor check",
					"pipelineId", id, "date", today, "completedAt", tr.CompletedAt)
			}
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
		lambda.RemapPerPeriodSensors(sensors, today)

		if postRunSensorArrived(d.Logger, id, cfg.PostRun.Rules, sensors, baseline) {
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
			continue // no marker, so the next pass retries
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

func parseSensorTimeout(s string) time.Duration {
	if s == "" {
		return types.DefaultPostRunSensorTimeout
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return types.DefaultPostRunSensorTimeout
	}
	return d
}

// postRunSensorArrived reports whether any post-run rule's sensor is present
// and differs from its entry in the baseline captured at completion, or has
// no entry there. The baseline entry was stored once more than the sensor,
// so the sensor is put through the same round trip before the comparison.
// A sensor that cannot be compared counts as arrived, so it raises no alert.
// A sensor rewritten with identical values has not arrived. An entry the
// stream-router seeded from a first write after completion carries
// lambda.PostRunSeededField, so it differs from the sensor and reads as arrived.
func postRunSensorArrived(logger *slog.Logger, pipelineID string, rules []types.ValidationRule, sensors map[string]map[string]interface{}, baseline map[string]interface{}) bool {
	for _, rule := range rules {
		data, ok := sensors[rule.Key]
		if !ok {
			continue
		}
		entry, ok := baseline[rule.Key]
		if !ok {
			return true
		}
		stored, err := storedForm(data)
		if err != nil {
			logger.Warn("post-run sensor could not be compared with its baseline",
				"pipelineId", pipelineID, "sensor", rule.Key, "error", err)
			return true
		}
		if !reflect.DeepEqual(entry, stored) {
			return true
		}
	}
	return false
}

// storedForm returns data as it reads back once stored as a baseline entry:
// sets come back as lists and an empty binary as null.
func storedForm(data map[string]interface{}) (interface{}, error) {
	item, err := attributevalue.MarshalMap(map[string]interface{}{"v": data})
	if err != nil {
		return nil, fmt.Errorf("encode sensor: %w", err)
	}
	var out map[string]interface{}
	if err := attributevalue.UnmarshalMap(item, &out); err != nil {
		return nil, fmt.Errorf("decode sensor: %w", err)
	}
	return out["v"], nil
}
