package stream

import (
	"context"
	"fmt"
	"strings"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// handlePostRunSensorEvent evaluates post-run rules reactively when a sensor
// arrives via DynamoDB Stream.
func handlePostRunSensorEvent(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, sensorKey string, sensorData map[string]interface{}) error {
	if cfg.DryRun {
		return handleDryRunPostRunSensor(ctx, d, cfg, pipelineID, sensorKey, sensorData)
	}

	scheduleID := lambda.ResolveScheduleID(cfg)
	date := lambda.ResolveExecutionDate(sensorData, d.Now())

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
		return handlePostRunCompleted(ctx, d, cfg, pipelineID, scheduleID, date, sensorKey, sensorData)
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
		if strings.HasPrefix(sensorKey, rule.Key) {
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
func handlePostRunCompleted(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, scheduleID, date, sensorKey string, sensorData map[string]interface{}) error {
	baselineKey := "postrun-baseline#" + date
	baseline, err := d.Store.GetSensorData(ctx, pipelineID, baselineKey)
	if err != nil {
		return fmt.Errorf("get baseline for post-run: %w", err)
	}

	if baseline != nil {
		var ruleBaseline map[string]interface{}
		for _, rule := range cfg.PostRun.Rules {
			if strings.HasPrefix(sensorKey, rule.Key) {
				if nested, ok := baseline[rule.Key].(map[string]interface{}); ok {
					ruleBaseline = nested
				}
				break
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

				if lambda.IsExcludedDate(cfg, date) {
					if pubErr := lambda.PublishEvent(ctx, d, string(types.EventPipelineExcluded), pipelineID, scheduleID, date,
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
