package orchestrator

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// statusError is the evaluate-mode status emitted when evaluation could not be
// performed (storage failure, missing config). The Step Functions IsReady
// Choice state dereferences $.evaluateResult.status unconditionally, so the
// field must always be populated; "error" routes back into the wait/retry loop
// until the evaluation window closes.
const statusError = "error"

// statusCutoff is the status of an evaluation at or after the run date's
// cutoff that is not a rerun. IsReady sends it to ValidationExhausted, which
// ends the run without launching the job.
const statusCutoff = "cutoff"

// handleEvaluate fetches config and sensors, evaluates validation rules, and
// optionally publishes a VALIDATION_PASSED event.
func handleEvaluate(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		d.Logger.ErrorContext(ctx, "evaluate failed", "pipelineId", input.PipelineID, "error", err)
		return lambda.OrchestratorOutput{Mode: "evaluate", Status: statusError, Error: err.Error()}, nil
	}
	if cfg == nil {
		notFoundErr := fmt.Sprintf("config not found for pipeline %q", input.PipelineID)
		d.Logger.ErrorContext(ctx, "evaluate failed", "pipelineId", input.PipelineID, "error", notFoundErr)
		return lambda.OrchestratorOutput{Mode: "evaluate", Status: statusError, Error: notFoundErr}, nil
	}

	// The cutoff limits only the first start: a rerun evaluates as usual.
	if lambda.CutoffPassed(cfg, input.Date, d.Now()) {
		rerun, err := lambda.IsRerun(ctx, d, input.PipelineID, input.ScheduleID, input.Date)
		if err != nil {
			d.Logger.ErrorContext(ctx, "evaluate failed", "pipelineId", input.PipelineID, "error", err)
			return lambda.OrchestratorOutput{Mode: "evaluate", Status: statusError, Error: err.Error()}, nil
		}
		if !rerun {
			d.Logger.InfoContext(ctx, "cutoff passed during evaluation, not starting",
				"pipelineId", input.PipelineID, "schedule", input.ScheduleID, "date", input.Date)
			return lambda.OrchestratorOutput{Mode: "evaluate", Status: statusCutoff}, nil
		}
	}

	sensors, err := d.Store.GetAllSensors(ctx, input.PipelineID)
	if err != nil {
		d.Logger.ErrorContext(ctx, "evaluate failed", "pipelineId", input.PipelineID, "error", err)
		return lambda.OrchestratorOutput{Mode: "evaluate", Status: statusError, Error: err.Error()}, nil
	}

	lambda.RemapPerPeriodSensors(sensors, input.Date)

	result := validation.EvaluateRules(cfg.Validation.Trigger, cfg.Validation.Rules, sensors, d.Now())

	if result.Passed {
		if err := lambda.PublishEvent(ctx, d, string(types.EventValidationPassed), input.PipelineID, input.ScheduleID, input.Date, "all validation rules passed"); err != nil {
			d.Logger.WarnContext(ctx, "failed to publish event", "type", types.EventValidationPassed, "error", err)
		}
	}

	status := "not_ready"
	if result.Passed {
		status = "passed"
	}

	return lambda.OrchestratorOutput{
		Mode:    "evaluate",
		Status:  status,
		Results: result.Results,
	}, nil
}
