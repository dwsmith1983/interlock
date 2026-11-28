package watchdog

import (
	"context"
	"fmt"
	"strings"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// postRunMissingLookback is how long after a date's post-run sensor deadline
// the watchdog still reports the sensor missing. A watchdog outage longer
// than this skips the date rather than alerting days late.
const postRunMissingLookback = 24 * time.Hour

// detectMissingPostRunSensors publishes POST_RUN_SENSOR_MISSING for each
// completed run date (daily or hourly key) of a pipeline with post-run rules
// whose sensor deadline (completedAt plus postRun.sensorTimeout) passed at
// most postRunMissingLookback ago and that has no postrun-check marker. The
// trigger rows read are those on the pipeline's schedule from runKeysDaysBack
// days before now's UTC date through runKeysDaysAhead after it. The
// stream-router writes the marker with arrived when a post-run sensor is
// written for a completed date; this check writes it with alerted.
func detectMissingPostRunSensors(ctx context.Context, d *lambda.Deps) error {
	configs, err := d.ConfigCache.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("load configs: %w", err)
	}

	now := d.Now()
	from := now.UTC().AddDate(0, 0, -runKeysDaysBack).Format("2006-01-02")
	to := now.UTC().AddDate(0, 0, runKeysDaysAhead).Format("2006-01-02")

	for id, cfg := range configs {
		if cfg.PostRun == nil || len(cfg.PostRun.Rules) == 0 || cfg.DryRun {
			continue
		}

		scheduleID := lambda.ResolveScheduleID(cfg)
		triggers, err := d.Store.ListTriggers(ctx, id, scheduleID, from, to)
		if err != nil {
			d.Logger.Error("trigger lookup failed in post-run sensor check",
				"pipelineId", id, "error", err)
			continue
		}

		timeout := parseSensorTimeout(cfg.PostRun.SensorTimeout)
		prefix := types.TriggerSK(scheduleID, "")
		for i := range triggers {
			tr := &triggers[i]
			date := strings.TrimPrefix(tr.SK, prefix)
			if !lambda.IsValidExecutionDate(date) {
				d.Logger.Warn("skipping malformed trigger key in post-run sensor check", "pipelineId", id, "sk", tr.SK)
				continue
			}
			if tr.Status != types.TriggerStatusCompleted || lambda.IsExcludedDate(cfg, date) {
				continue
			}
			// The run's completion on the trigger row starts the sensor timeout.
			completedAt, err := time.Parse(time.RFC3339, tr.CompletedAt)
			if err != nil {
				if tr.CompletedAt != "" {
					d.Logger.Warn("unparseable completedAt in post-run sensor check",
						"pipelineId", id, "date", date, "completedAt", tr.CompletedAt)
				}
				continue
			}
			if deadline := completedAt.Add(timeout); now.Before(deadline) || now.Sub(deadline) > postRunMissingLookback {
				continue
			}
			checkPostRunSensorForDate(ctx, d, id, cfg, scheduleID, date, tr.CompletedAt)
		}
	}
	return nil
}

// checkPostRunSensorForDate publishes POST_RUN_SENSOR_MISSING for one
// completed date past its sensor deadline unless its postrun-check marker
// exists (the sensor arrived, or the date was alerted already). Only a
// successful publish writes the alerted marker, so a failed one retries on
// the next pass.
func checkPostRunSensorForDate(ctx context.Context, d *lambda.Deps, id string, cfg *types.PipelineConfig, scheduleID, date, completedAt string) {
	dedupKey := "postrun-check#" + date
	if marker, err := d.Store.GetSensorData(ctx, id, dedupKey); err != nil || marker != nil {
		if err != nil {
			d.Logger.Error("dedup marker lookup failed in post-run sensor check",
				"pipelineId", id, "date", date, "error", err)
		}
		return
	}

	ruleKeys := make([]string, 0, len(cfg.PostRun.Rules))
	for _, r := range cfg.PostRun.Rules {
		ruleKeys = append(ruleKeys, r.Key)
	}
	alertDetail := map[string]interface{}{
		"source":        "watchdog",
		"sensorTimeout": cfg.PostRun.SensorTimeout,
		"ruleKeys":      strings.Join(ruleKeys, ", "),
		"completedAt":   completedAt,
		"actionHint":    "post-run sensor data has not arrived within the expected timeout",
	}
	if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunSensorMissing), id, scheduleID, date,
		fmt.Sprintf("post-run sensor missing for %s on %s", id, date), alertDetail); err != nil {
		d.Logger.Warn("failed to publish post-run sensor missing event", "error", err, "pipeline", id, "schedule", scheduleID, "date", date)
		return // no marker, so the next pass retries
	}

	// An arrived marker the stream-router wrote since the read above is kept
	// (written false).
	written, err := d.Store.WriteSensorIfAbsent(ctx, id, dedupKey, map[string]interface{}{"alerted": "true"})
	if err != nil {
		d.Logger.Warn("failed to write post-run dedup marker", "error", err, "pipeline", id, "date", date)
		return
	}

	d.Logger.Info("detected missing post-run sensor",
		"pipelineId", id, "schedule", scheduleID, "date", date, "markerWritten", written)
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
