package orchestrator

import (
	"context"
	"fmt"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// handleEvaluate fetches config and sensors, evaluates validation rules, and
// optionally publishes a VALIDATION_PASSED event.
func handleEvaluate(ctx context.Context, d *lambda.Deps, input lambda.OrchestratorInput) (lambda.OrchestratorOutput, error) {
	cfg, err := d.Store.GetConfig(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{Mode: "evaluate", Error: err.Error()}, nil
	}
	if cfg == nil {
		return lambda.OrchestratorOutput{Mode: "evaluate", Error: fmt.Sprintf("config not found for pipeline %q", input.PipelineID)}, nil
	}

	sensors, err := d.Store.GetAllSensors(ctx, input.PipelineID)
	if err != nil {
		return lambda.OrchestratorOutput{Mode: "evaluate", Error: err.Error()}, nil
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
