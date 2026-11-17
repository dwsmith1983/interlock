package orchestrator

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleValidationExhausted ends an evaluation that did not launch the job.
// At or after the cutoff, for a date that is not a rerun, it closes the run
// (failAtCutoff). Otherwise the window closed without all rules passing: it
// writes the validation-exhausted joblog row and publishes VALIDATION_EXHAUSTED.
func handleValidationExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("validation-exhausted get config: %w", err)
	}
	if cfg != nil && lambda.CutoffPassed(cfg, input.Date, d.Now()) {
		rerun, err := lambda.IsRerun(ctx, d, input.PipelineID, input.ScheduleID, input.Date)
		if err != nil {
			return lambda.OrchestratorOutput{}, fmt.Errorf("validation-exhausted: %w", err)
		}
		if !rerun {
			if err := failAtCutoff(ctx, d, cfg, input); err != nil {
				return lambda.OrchestratorOutput{}, err
			}
			return lambda.OrchestratorOutput{Mode: "validation-exhausted", Status: statusCutoff}, nil
		}
	}

	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date, types.JobEventValidationExhausted, "", 0, "evaluation window exhausted without passing"); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write validation-exhausted joblog: %w", err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventValidationExhausted), input.PipelineID, input.ScheduleID, input.Date, "evaluation window exhausted without passing"); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventValidationExhausted, "error", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "validation-exhausted",
		Status: "exhausted",
	}, nil
}

// failAtCutoff publishes SENSOR_DEADLINE_EXPIRED, then sets the RUNNING
// trigger row FAILED_FINAL. Publishing first keeps a failed publish
// retryable: FAILED_FINAL removes the TTL, so a write to revert would leave a
// row RUNNING for good. A missing or finished row (a Task retry after
// success, or a run someone else ended) is left alone.
func failAtCutoff(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, input lambda.OrchestratorInput) error {
	pipelineID, schedule, date := input.PipelineID, input.ScheduleID, input.Date
	tr, err := d.Store.GetTrigger(ctx, pipelineID, schedule, date)
	if err != nil {
		return fmt.Errorf("validation-exhausted cutoff: %w", err)
	}
	if tr == nil || tr.Status != types.TriggerStatusRunning {
		status := "missing"
		if tr != nil {
			status = tr.Status
		}
		d.Logger.InfoContext(ctx, "cutoff passed, trigger row not RUNNING, leaving it",
			"pipelineId", pipelineID, "schedule", schedule, "date", date, "status", status)
		return nil
	}
	if err := lambda.PublishCutoffExpired(ctx, d, cfg, pipelineID, schedule, date, "orchestrator"); err != nil {
		return fmt.Errorf("validation-exhausted cutoff: %w", err)
	}
	if err := d.Store.SetTriggerStatus(ctx, pipelineID, schedule, date, types.TriggerStatusFailedFinal); err != nil {
		return fmt.Errorf("validation-exhausted cutoff: %w", err)
	}
	d.Logger.InfoContext(ctx, "run ended at its cutoff without starting",
		"pipelineId", pipelineID, "schedule", schedule, "date", date)
	return nil
}

// handleJobPollExhausted writes a timeout row for the run when the job poll
// window closes without a terminal result. The stream-router publishes
// JOB_FAILED from the row, and CompleteTrigger (fed event=timeout by
// InjectTimeoutEvent) decides whether to retry. The ASL treats this state as
// best effort, so that decision never depends on the row.
func handleJobPollExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventTimeout, input.RunID, 0, "job poll window exhausted"); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write job poll timeout joblog: %w", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "job-poll-exhausted",
		Status: "exhausted",
	}, nil
}

// handleTriggerExhausted publishes RETRY_EXHAUSTED when trigger retries are
// exhausted, writes a joblog entry for audit, and releases the trigger lock
// so the pipeline can be re-triggered.
func handleTriggerExhausted(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	errMsg := ""
	if cause, ok := input.ErrorInfo["Cause"].(string); ok {
		errMsg = cause
	}

	// Dual-write: joblog entry (audit) + EventBridge event (alerting).
	if err := d.Store.WriteJobEvent(ctx, input.PipelineID, input.ScheduleID, input.Date,
		types.JobEventInfraTriggerExhausted, "", 0, errMsg); err != nil {
		return lambda.OrchestratorOutput{}, fmt.Errorf("write trigger-exhausted joblog: %w", err)
	}

	if err := lambda.PublishEvent(ctx, d, string(types.EventRetryExhausted), input.PipelineID, input.ScheduleID, input.Date,
		fmt.Sprintf("trigger retries exhausted for %s: %s", input.PipelineID, errMsg)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventRetryExhausted, "error", err)
	}

	// Release lock so pipeline can be re-triggered.
	if err := d.Store.ReleaseTriggerLock(ctx, input.PipelineID, input.ScheduleID, input.Date); err != nil {
		d.Logger.WarnContext(ctx, "failed to release trigger lock after exhaustion",
			"pipeline", input.PipelineID, "error", err)
	}

	return lambda.OrchestratorOutput{
		Mode:   "trigger-exhausted",
		Status: "exhausted",
	}, nil
}
