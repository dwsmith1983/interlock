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
		if err := lambda.CapturePostRunBaseline(ctx, d, input.PipelineID, input.ScheduleID, input.Date); err != nil {
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
