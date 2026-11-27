package stream

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// handlePostRunSensorEvent evaluates post-run rules reactively when a sensor
// arrives via DynamoDB Stream. A completed date is checked only inside its
// post-run watch window.
func handlePostRunSensorEvent(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}) error {
	if cfg.DryRun {
		return handleDryRunPostRunSensor(ctx, d, cfg, pipelineID, sensorKey, sensorData)
	}

	now := d.Now()
	scheduleID := lambda.ResolveScheduleID(cfg)
	date := lambda.ResolveExecutionDate(sensorData, now)

	trigger, err := d.Store.GetTrigger(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get trigger for post-run: %w", err)
	}
	if trigger == nil {
		return nil
	}

	switch trigger.Status {
	case types.TriggerStatusRunning:
		return handlePostRunInflight(ctx, d, cfg, pipelineID, scheduleID, date, sensorKey, sensorData)
	case types.TriggerStatusCompleted:
		if !lambda.PostRunWatched(cfg, trigger, now) {
			d.Logger.InfoContext(ctx, "post-run sensor ignored: post-run watch window has ended",
				"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "sensor", sensorKey,
				"completedAt", trigger.CompletedAt)
			return nil
		}
		return handlePostRunCompleted(ctx, d, cfg, trigger, now, pipelineID, scheduleID, date, sensorKey, sensorData)
	default:
		return nil
	}
}

// handlePostRunInflight evaluates post-run rules while the job is still running.
func handlePostRunInflight(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, sensorKey string, sensorData map[string]interface{}) error {
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for inflight check: %w", err)
	}
	if baseline == nil {
		return nil
	}

	var ruleBaseline map[string]interface{}
	for _, rule := range cfg.PostRun.Rules {
		if lambda.SensorKeyMatches(sensorKey, rule.Key) {
			if nested, ok := baseline[rule.Key].(map[string]interface{}); ok {
				ruleBaseline = nested
			}
			break
		}
	}
	if ruleBaseline == nil {
		return nil
	}

	driftField := lambda.ResolveDriftField(cfg.PostRun)
	threshold := 0.0
	if cfg.PostRun.DriftThreshold != nil {
		threshold = *cfg.PostRun.DriftThreshold
	}
	dr := lambda.DetectDrift(ruleBaseline, sensorData, driftField, threshold)
	if dr.Drifted {
		if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunDriftInflight), pipelineID, scheduleID, date,
			fmt.Sprintf("inflight drift detected for %s: %.0f → %.0f (informational)", pipelineID, dr.Previous, dr.Current),
			map[string]interface{}{
				"previousCount":  dr.Previous,
				"currentCount":   dr.Current,
				"delta":          dr.Delta,
				"driftThreshold": threshold,
				"driftField":     driftField,
				"sensorKey":      sensorKey,
				"source":         "post-run-stream",
			}); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunDriftInflight, "error", err)
		}
	}
	return nil
}

// handlePostRunCompleted evaluates post-run rules after the job has completed.
// Drift is always published; a data-drift rerun is requested only while the
// trigger row tr still allows automatic reruns at now, the time the caller
// judged the watch window by.
func handlePostRunCompleted(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, tr *types.ControlRecord, now time.Time, pipelineID, scheduleID, date, sensorKey string, sensorData map[string]interface{}) error {
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for post-run: %w", err)
	}

	ruleKey, ruleBaseline := postRunBaselineEntry(cfg.PostRun.Rules, sensorKey, baseline)
	if ruleKey != "" {
		if ruleBaseline == nil && seedsPostRunBaseline(cfg.PostRun, ruleKey, sensorKey, date, sensorData) {
			if ruleBaseline, err = seedPostRunBaseline(ctx, d, pipelineID, date, ruleKey, baseline != nil, sensorData); err != nil {
				return err
			}
		}

		if ruleBaseline != nil {
			driftField := lambda.ResolveDriftField(cfg.PostRun)
			threshold := 0.0
			if cfg.PostRun.DriftThreshold != nil {
				threshold = *cfg.PostRun.DriftThreshold
			}
			dr := lambda.DetectDrift(ruleBaseline, sensorData, driftField, threshold)
			if dr.Drifted {
				if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunDrift), pipelineID, scheduleID, date,
					fmt.Sprintf("post-run drift detected for %s: %.0f → %.0f records", pipelineID, dr.Previous, dr.Current),
					map[string]interface{}{
						"previousCount":  dr.Previous,
						"currentCount":   dr.Current,
						"delta":          dr.Delta,
						"driftThreshold": threshold,
						"driftField":     driftField,
						"sensorKey":      sensorKey,
						"source":         "post-run-stream",
					}); err != nil {
					d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunDrift, "error", err)
				}

				switch {
				case lambda.IsExcludedDate(cfg, date):
					if pubErr := lambda.PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleID, date,
						fmt.Sprintf("post-run drift rerun skipped for %s: execution date %s excluded by calendar", pipelineID, date)); pubErr != nil {
						d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
					}
					d.Logger.InfoContext(ctx, "post-run drift rerun skipped: execution date excluded by calendar",
						"pipelineId", pipelineID, "date", date)
				case !lambda.AutoRerunAllowed(cfg, tr, now):
					logAutoRerunEnded(ctx, d, cfg, tr, "post-run drift rerun not requested: automatic reruns have ended",
						pipelineID, scheduleID, date, "data-drift")
				default:
					if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, scheduleID, date, "data-drift"); writeErr != nil {
						d.Logger.WarnContext(ctx, "failed to write rerun request on post-run drift",
							"pipelineId", pipelineID, "error", writeErr)
					}
				}
				return nil
			}
		}
	}

	// Evaluate post-run validation rules.
	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors for post-run rules: %w", err)
	}
	lambda.RemapPerPeriodSensors(sensors, date)

	result := validation.EvaluateRules("ALL", cfg.PostRun.Rules, sensors, d.Now())

	if result.Passed {
		if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunPassed), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run validation passed for %s", pipelineID)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunPassed, "error", err)
		}
	} else {
		if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunFailed), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run validation failed for %s", pipelineID)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunFailed, "error", err)
		}
	}

	return nil
}

// postRunBaselineEntry returns the key of the first post-run rule sensorKey
// matches (lambda.SensorKeyMatches), and that rule's entry in baseline, nil
// when it has none. The key is empty when no rule matches.
func postRunBaselineEntry(rules []types.ValidationRule, sensorKey string, baseline map[string]interface{}) (ruleKey string, entry map[string]interface{}) {
	for _, rule := range rules {
		if lambda.SensorKeyMatches(sensorKey, rule.Key) {
			entry, _ = baseline[rule.Key].(map[string]interface{})
			return rule.Key, entry
		}
	}
	return "", nil
}

// seedsPostRunBaseline reports whether a write of sensorKey for date may
// seed ruleKey's baseline entry: the key is one the baseline capture stores
// under ruleKey (ruleKey itself, or ruleKey#<date> dashed or compact), and the
// write carries the drift field as a number.
func seedsPostRunBaseline(cfg *types.PostRunConfig, ruleKey, sensorKey, date string, sensorData map[string]interface{}) bool {
	if ruleKey == "" {
		return false
	}
	switch sensorKey {
	case ruleKey, ruleKey + "#" + date, ruleKey + "#" + strings.ReplaceAll(date, "-", ""):
	default:
		return false
	}
	_, ok := lambda.ExtractFloatOk(sensorData, lambda.ResolveDriftField(cfg))
	return ok
}

// seedPostRunBaseline stores sensorData, marked with lambda.PostRunSeededField,
// as ruleKey's entry in the date's baseline: the first value of a post-run
// sensor written after the run completed, which later writes are compared
// with. It never replaces data: it creates the row only while it is absent
// (rowExists false), and otherwise adds the entry only while the row lacks
// it. After its own write it returns nil. When another writer got there
// first it returns the entry now stored (nil if there is none) for the
// caller to compare with.
//
// Limits: the baseline capture at completion writes the whole row
// unconditionally, so it replaces a seed that landed between the run's
// completion and the capture. A refused seed whose re-read finds no entry
// (the row was deleted, the capture won without this rule's sensor, or
// another rule's first write on a different shard created the row first)
// is neither seeded nor compared; the sensor's next write that carries the
// drift field seeds it.
func seedPostRunBaseline(ctx context.Context, d *lambda.Deps, pipelineID, date, ruleKey string, rowExists bool, sensorData map[string]interface{}) (map[string]interface{}, error) {
	entry := make(map[string]interface{}, len(sensorData)+1)
	maps.Copy(entry, sensorData)
	entry[lambda.PostRunSeededField] = true
	key := "postrun-baseline#" + date
	var written bool
	var err error
	if rowExists {
		written, err = d.Store.WriteSensorFieldIfAbsent(ctx, pipelineID, key, ruleKey, entry)
	} else {
		written, err = d.Store.WriteSensorIfAbsent(ctx, pipelineID, key, map[string]interface{}{ruleKey: entry})
	}
	if err != nil {
		return nil, fmt.Errorf("seed post-run baseline for %q: %w", ruleKey, err)
	}
	if written {
		d.Logger.InfoContext(ctx, "post-run baseline entry seeded from the first write after completion",
			"pipelineId", pipelineID, "date", date, "rule", ruleKey)
		return nil, nil
	}

	baseline, err := d.Store.GetSensorData(ctx, pipelineID, key)
	if err != nil {
		return nil, fmt.Errorf("read post-run baseline for %q after a refused seed: %w", ruleKey, err)
	}
	stored, _ := baseline[ruleKey].(map[string]interface{})
	d.Logger.InfoContext(ctx, "post-run baseline entry was written first by another writer",
		"pipelineId", pipelineID, "date", date, "rule", ruleKey, "found", stored != nil)
	return stored, nil
}
