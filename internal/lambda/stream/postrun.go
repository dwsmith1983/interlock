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
// arrives via DynamoDB Stream; arrivedAt is its stream record's creation
// time, zero when unknown. A completed date is checked only inside its
// post-run watch window; after it, drift only asks for a manual rerun.
func handlePostRunSensorEvent(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}, arrivedAt time.Time) error {
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

	write := manualRerunInput{sensorKey: sensorKey, sensorData: sensorData, arrivedAt: arrivedAt, now: now}
	if arrivedAt.Unix() <= 0 {
		write.arrivedAt = now
	}
	switch trigger.Status {
	case types.TriggerStatusRunning:
		// A write while the job runs is not compared: the baseline is captured
		// when the run completes, and later writes are compared with it.
		return nil
	case types.TriggerStatusCompleted:
		if err := recordPostRunArrival(ctx, d, cfg, pipelineID, sensorKey, date); err != nil {
			return err
		}
		if !lambda.PostRunWatched(cfg, trigger, now) {
			d.Logger.InfoContext(ctx, "post-run sensor after the post-run watch window: no automatic rerun or post-run check",
				"pipelineId", pipelineID, "schedule", scheduleID, "date", date, "sensor", sensorKey,
				"completedAt", trigger.CompletedAt)
			return recordDriftAfterWindow(ctx, d, cfg, trigger, pipelineID, scheduleID, date, write)
		}
		return handlePostRunCompleted(ctx, d, cfg, trigger, pipelineID, scheduleID, date, write)
	default:
		return nil
	}
}

// recordPostRunArrival records that a post-run sensor was written for date
// after its run completed: postrun-check#<date> with arrived, the marker the
// watchdog's POST_RUN_SENSOR_MISSING check skips. A sensorKey naming another
// period than date (keyNamesDate) records nothing. It is written before any
// rerun request, so a drift rerun's later completion finds it, and never
// replaces a marker already there (the watchdog's alerted one). A failed
// write is returned, failing the record before anything else is done, so
// its redelivery retries the marker: without it the watchdog would report
// the sensor as missing.
func recordPostRunArrival(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, sensorKey, date string) error {
	if ruleKey, _ := postRunBaselineEntry(cfg.PostRun.Rules, sensorKey, nil); !keyNamesDate(ruleKey, sensorKey, date) {
		d.Logger.InfoContext(ctx, "post-run arrival not recorded: the sensor key names another period",
			"pipelineId", pipelineID, "date", date, "sensor", sensorKey)
		return nil
	}
	if _, err := d.Store.WriteSensorIfAbsent(ctx, pipelineID, "postrun-check#"+date, map[string]interface{}{"arrived": "true"}); err != nil {
		return fmt.Errorf("record post-run arrival for %q on %s: %w", pipelineID, date, err)
	}
	return nil
}

// handlePostRunCompleted evaluates post-run rules after the job has completed,
// for a write inside the date's watch window of the trigger row tr at
// write.now. Drift goes to handleCompletedDrift instead of the rules.
func handlePostRunCompleted(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, tr *types.ControlRecord, pipelineID, scheduleID, date string, write manualRerunInput) error {
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for post-run: %w", err)
	}

	ruleKey, ruleBaseline := postRunBaselineEntry(cfg.PostRun.Rules, write.sensorKey, baseline)
	if ruleBaseline == nil && seedsPostRunBaseline(cfg.PostRun, ruleKey, write.sensorKey, date, write.sensorData) {
		if ruleBaseline, err = seedPostRunBaseline(ctx, d, pipelineID, date, ruleKey, baseline != nil, write.sensorData); err != nil {
			return err
		}
	}
	if ruleBaseline != nil {
		if dr, drift := postRunDrift(cfg.PostRun, ruleBaseline, write.sensorData); dr.Drifted {
			write.drift, write.baseline = drift, ruleBaseline
			return handleCompletedDrift(ctx, d, cfg, tr, pipelineID, scheduleID, date, dr, write)
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

// handleCompletedDrift acts on drift (dr, and write.drift) of a write inside
// the watch window of tr. While automatic reruns are allowed at write.now it
// publishes POST_RUN_DRIFT and requests a data-drift rerun, or publishes
// PIPELINE_EXCLUDED for a calendar-excluded date. After that it asks for a
// manual rerun and publishes no POST_RUN_DRIFT: with no rerun the baseline
// stays as it is, so every later write would repeat it.
func handleCompletedDrift(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, tr *types.ControlRecord, pipelineID, scheduleID, date string,
	dr lambda.DriftResult, write manualRerunInput) error {
	if !lambda.AutoRerunAllowed(cfg, tr, write.now) {
		write.blockedBy = logAutoRerunEnded(ctx, d, cfg, tr, "post-run drift rerun not requested: automatic reruns have ended",
			pipelineID, scheduleID, date, "data-drift")
		return requireManualRerunOnDrift(ctx, d, cfg, pipelineID, scheduleID, date, tr, write)
	}
	detail := maps.Clone(write.drift)
	detail["sensorKey"], detail["source"] = write.sensorKey, "post-run-stream"
	if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunDrift), pipelineID, scheduleID, date,
		fmt.Sprintf("post-run drift detected for %s: %.0f → %.0f records", pipelineID, dr.Previous, dr.Current), detail); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunDrift, "error", err)
	}
	if lambda.IsExcludedDate(cfg, date) {
		if pubErr := lambda.PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run drift rerun skipped for %s: execution date %s excluded by calendar", pipelineID, date)); pubErr != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
		}
		d.Logger.InfoContext(ctx, "post-run drift rerun skipped: execution date excluded by calendar",
			"pipelineId", pipelineID, "date", date)
		return nil
	}
	if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, scheduleID, date, "data-drift"); writeErr != nil {
		d.Logger.WarnContext(ctx, "failed to write rerun request on post-run drift",
			"pipelineId", pipelineID, "error", writeErr)
	}
	return nil
}

// recordDriftAfterWindow handles a post-run write after the watch window of
// tr: drift from its rule's baseline entry asks for a manual rerun, blocked by
// the window. It never seeds or rewrites the baseline, publishes no POST_RUN_*
// event and evaluates no post-run rule. Without the baseline row, which
// expires with its ttl, nothing is recorded.
func recordDriftAfterWindow(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, tr *types.ControlRecord, pipelineID, scheduleID, date string,
	write manualRerunInput) error {
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, "postrun-baseline#"+date)
	if err != nil {
		return fmt.Errorf("get baseline after the post-run watch window: %w", err)
	}
	_, entry := postRunBaselineEntry(cfg.PostRun.Rules, write.sensorKey, baseline)
	if entry == nil {
		return nil
	}
	dr, drift := postRunDrift(cfg.PostRun, entry, write.sensorData)
	if !dr.Drifted {
		return nil
	}
	write.drift, write.baseline, write.blockedBy = drift, entry, endedByWatchWindow
	return requireManualRerunOnDrift(ctx, d, cfg, pipelineID, scheduleID, date, tr, write)
}

// postRunDrift compares a post-run write with its rule's baseline entry by
// postRun.driftField and driftThreshold, and gives the drift's numbers as
// event detail: driftField, previousCount, currentCount, delta and
// driftThreshold.
func postRunDrift(cfg *types.PostRunConfig, entry, sensorData map[string]interface{}) (dr lambda.DriftResult, detail map[string]interface{}) {
	field := lambda.ResolveDriftField(cfg)
	threshold := 0.0
	if cfg.DriftThreshold != nil {
		threshold = *cfg.DriftThreshold
	}
	dr = lambda.DetectDrift(entry, sensorData, field, threshold)
	return dr, map[string]interface{}{"previousCount": dr.Previous, "currentCount": dr.Current, "delta": dr.Delta,
		"driftThreshold": threshold, "driftField": field}
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
// seed ruleKey's baseline entry: keyNamesDate, and the write carries the
// drift field as a number.
func seedsPostRunBaseline(cfg *types.PostRunConfig, ruleKey, sensorKey, date string, sensorData map[string]interface{}) bool {
	if !keyNamesDate(ruleKey, sensorKey, date) {
		return false
	}
	_, ok := lambda.ExtractFloatOk(sensorData, lambda.ResolveDriftField(cfg))
	return ok
}

// keyNamesDate reports whether sensorKey is a key the baseline capture
// stores under ruleKey for date: ruleKey itself, or ruleKey#<date> dashed or
// compact. An empty ruleKey (no rule matches) is false.
func keyNamesDate(ruleKey, sensorKey, date string) bool {
	if ruleKey == "" {
		return false
	}
	switch sensorKey {
	case ruleKey, ruleKey + "#" + date, ruleKey + "#" + strings.ReplaceAll(date, "-", ""):
		return true
	}
	return false
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
