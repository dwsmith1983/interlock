package orchestrator

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleCompleteTrigger sets the trigger row to its terminal status.
// Success -> COMPLETED; fail/timeout -> FAILED_FINAL.
// On success with PostRun configured, captures a date-scoped baseline snapshot
// of all sensors for later drift comparison by the stream-based post-run evaluator.
func handleCompleteTrigger(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	status := types.TriggerStatusCompleted
	if input.Event != types.JobEventSuccess {
		status = types.TriggerStatusFailedFinal
	}

	if err := d.Store.SetTriggerStatus(ctx, input.PipelineID, input.ScheduleID, input.Date, status); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("set trigger status: %w", err)
	}

	// On success, capture post-run baseline for drift detection.
	if input.Event == types.JobEventSuccess {
		if err := CapturePostRunBaseline(ctx, d, input.PipelineID, input.ScheduleID, input.Date); err != nil {
			d.Logger.WarnContext(ctx, "failed to capture post-run baseline",
				"pipeline", input.PipelineID, "error", err)
			if pubErr := lambda.PublishEvent(ctx, d, string(types.EventBaselineCaptureFailed), input.PipelineID, input.ScheduleID, input.Date,
				fmt.Sprintf("baseline capture failed for %s: %v", input.PipelineID, err)); pubErr != nil {
				d.Logger.WarnContext(ctx, "failed to publish baseline capture failure event", "error", pubErr)
			}
		}
	}

	return lambda.OrchestratorOutput{
		Mode:   "complete-trigger",
		Status: status,
	}, nil
}

// CapturePostRunBaseline reads all sensors and writes a date-scoped baseline
// snapshot if the pipeline has PostRun config. The baseline is stored as
// "postrun-baseline#<date>" so drift detection can compare against it.
func CapturePostRunBaseline(ctx context.Context, d *lambda.Deps, pipelineID, scheduleID, date string) error {
	cfg, err := d.Store.GetConfig(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get config: %w", err)
	}
	if cfg == nil || cfg.PostRun == nil || len(cfg.PostRun.Rules) == 0 {
		return nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, pipelineID)
	if err != nil {
		return fmt.Errorf("get sensors: %w", err)
	}

	lambda.RemapPerPeriodSensors(sensors, date)

	// Build baseline from post-run rule keys, namespaced by rule key
	// to prevent field name collisions between different sensors.
	baseline := make(map[string]interface{})
	for _, rule := range cfg.PostRun.Rules {
		if data, ok := sensors[rule.Key]; ok {
			baseline[rule.Key] = data
		}
	}

	if len(baseline) == 0 {
		return nil
	}

	baselineKey := "postrun-baseline#" + date
	if err := d.Store.WriteSensor(ctx, pipelineID, baselineKey, baseline); err != nil {
		return fmt.Errorf("write baseline: %w", err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventPostRunBaselineCaptured), pipelineID, scheduleID, date,
		fmt.Sprintf("post-run baseline captured for %s", pipelineID)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventPostRunBaselineCaptured, "error", err)
	}

	return nil
}
