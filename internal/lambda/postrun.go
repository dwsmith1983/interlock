package lambda

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/dwsmith1983/interlock/internal/validation"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// defaultDriftField is the sensor field used for drift comparison when
// PostRunConfig.DriftField is not set.
const defaultDriftField = "sensor_count"

func resolveDriftField(cfg *types.PostRunConfig) string {
	if cfg.DriftField != "" {
		return cfg.DriftField
	}
	return defaultDriftField
}

// matchesPostRunRule returns true if the sensor key matches any post-run rule key
// (prefix match to support per-period sensor keys).
func matchesPostRunRule(sensorKey string, rules []types.ValidationRule) bool {
	for _, rule := range rules {
		if strings.HasPrefix(sensorKey, rule.Key) {
			return true
		}
	}
	return false
}

// handlePostRunSensorEvent evaluates post-run rules reactively when a sensor
// arrives via DynamoDB Stream. Compares current sensor values against the
// date-scoped baseline captured at trigger completion.
func handlePostRunSensorEvent(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}) error {
	scheduleID := resolveScheduleID(cfg)
	date := ResolveExecutionDate(sensorData, d.now())

	// Consistent read to handle race where sensor stream event arrives
	// before SFN sets trigger to COMPLETED.
	trigger, err := d.Store.GetTrigger(ctx, pipelineID, scheduleID, date)
	if err != nil {
		return fmt.Errorf("get trigger for post-run: %w", err)
	}
	if trigger == nil {
		return nil // No trigger for this date — not a post-run event.
	}

	switch trigger.Status {
	case types.TriggerStatusRunning:
		// Job still running — evaluate rules for informational drift detection.
		return handlePostRunInflight(ctx, d, cfg, pipelineID, scheduleID, date, sensorKey, sensorData)

	case types.TriggerStatusCompleted:
		// Job completed — full post-run evaluation with baseline comparison.
		return handlePostRunCompleted(ctx, d, cfg, pipelineID, scheduleID, date, sensorData)

	default:
		// FAILED_FINAL or unknown — skip.
		return nil
	}
}

// handlePostRunInflight evaluates post-run rules while the job is still running.
// If drift is detected, publishes an informational event but does NOT trigger a rerun.
func handlePostRunInflight(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, sensorKey string, sensorData map[string]interface{}) error {
	// Read baseline for comparison.
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for inflight check: %w", err)
	}
	if baseline == nil {
		return nil // No baseline yet — job hasn't completed once.
	}

	driftField := resolveDriftField(cfg.PostRun)
	prevCount := ExtractFloat(baseline, driftField)
	currCount := ExtractFloat(sensorData, driftField)
	threshold := 0.0
	if cfg.PostRun.DriftThreshold != nil {
		threshold = *cfg.PostRun.DriftThreshold
	}
	if prevCount > 0 && currCount > 0 && math.Abs(currCount-prevCount) > threshold {
		if err := publishEvent(ctx, d, string(types.EventPostRunDriftInflight), pipelineID, scheduleID, date,
			fmt.Sprintf("inflight drift detected for %s: %.0f → %.0f (informational)", pipelineID, prevCount, currCount),
			map[string]interface{}{
				"previousCount":  prevCount,
				"currentCount":   currCount,
				"delta":          currCount - prevCount,
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
// Compares sensor values against the date-scoped baseline and triggers a rerun
// if drift is detected.
func handlePostRunCompleted(ctx context.Context, d *Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date string, sensorData map[string]interface{}) error {
	// Read baseline captured at trigger completion.
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for post-run: %w", err)
	}

	// Check for data drift if baseline exists.
	if baseline != nil {
		driftField := resolveDriftField(cfg.PostRun)
		prevCount := ExtractFloat(baseline, driftField)
		currCount := ExtractFloat(sensorData, driftField)
		threshold := 0.0
		if cfg.PostRun.DriftThreshold != nil {
			threshold = *cfg.PostRun.DriftThreshold
		}
		if prevCount > 0 && currCount > 0 && math.Abs(currCount-prevCount) > threshold {
			delta := currCount - prevCount
			if err := publishEvent(ctx, d, string(types.EventPostRunDrift), pipelineID, scheduleID, date,
				fmt.Sprintf("post-run drift detected for %s: %.0f → %.0f records", pipelineID, prevCount, currCount),
				map[string]interface{}{
					"previousCount":  prevCount,
					"currentCount":   currCount,
					"delta":          delta,
					"driftThreshold": threshold,
					"driftField":     driftField,
					"source":         "post-run-stream",
				}); err != nil {
				d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunDrift, "error", err)
			}

			// Trigger rerun via the existing circuit breaker path only if the
			// execution date is not excluded by the pipeline's calendar config.
			if isExcludedDate(cfg, date) {
				if pubErr := publishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleID, date,
					fmt.Sprintf("post-run drift rerun skipped for %s: execution date %s excluded by calendar", pipelineID, date)); pubErr != nil {
					d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPipelineExcluded, "error", pubErr)
				}
				d.Logger.InfoContext(ctx, "post-run drift rerun skipped: execution date excluded by calendar",
					"pipelineId", pipelineID, "date", date)
			} else {
				if writeErr := d.Store.WriteRerunRequest(ctx, pipelineID, scheduleID, date, "data-drift"); writeErr != nil {
					d.Logger.WarnContext(ctx, "failed to write rerun request on post-run drift",
						"pipelineId", pipelineID, "error", writeErr)
				}
			}
			return nil
		}
	}

	// Evaluate post-run validation rules.
	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors for post-run rules: %w", err)
	}
	RemapPerPeriodSensors(sensors, date)

	result := validation.EvaluateRules("ALL", cfg.PostRun.Rules, sensors, d.now())

	if result.Passed {
		if err := publishEvent(ctx, d, string(types.EventPostRunPassed), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run validation passed for %s", pipelineID)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunPassed, "error", err)
		}
	} else {
		if err := publishEvent(ctx, d, string(types.EventPostRunFailed), pipelineID, scheduleID, date,
			fmt.Sprintf("post-run validation failed for %s", pipelineID)); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunFailed, "error", err)
		}
	}

	return nil
}
