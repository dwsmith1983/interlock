package main

import (
	"context"
	"fmt"
	"time"

	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func checkEvaluationSLA(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.SLA == nil || pipeline.SLA.EvaluationDeadline == "" {
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": false},
		}, nil
	}

	now := time.Now()
	refTime := extractExecutionStartTime(req)
	deadline, err := schedule.ParseSLADeadline(pipeline.SLA.EvaluationDeadline, pipeline.SLA.Timezone, now, refTime)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing SLA deadline: %v", err)), nil
	}

	if schedule.IsBreached(deadline, now) {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelWarning,
			Category:   "evaluation_sla_breach",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Evaluation SLA breached for %s (deadline: %s)", req.PipelineID, pipeline.SLA.EvaluationDeadline),
			Timestamp:  now,
		})
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": true},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"breached": false},
	}, nil
}

func checkCompletionSLA(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	// Resolve schedule for schedule-level deadline
	scheduleID := req.ScheduleID
	var sched types.ScheduleConfig
	for _, s := range types.ResolveSchedules(*pipeline) {
		if s.Name == scheduleID {
			sched = s
			break
		}
	}

	now := time.Now()
	refTime := extractExecutionStartTime(req)
	deadline, ok := schedule.ScheduleDeadline(sched, *pipeline, now, refTime)
	if !ok {
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": false},
		}, nil
	}

	if schedule.IsBreached(deadline, now) {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelWarning,
			Category:   "completion_sla_breach",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Completion SLA breached for %s schedule %s", req.PipelineID, scheduleID),
			Timestamp:  now,
		})
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": true},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"breached": false},
	}, nil
}

func checkValidationTimeout(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.SLA == nil || pipeline.SLA.ValidationTimeout == "" {
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"validationTimedOut": false},
		}, nil
	}

	now := time.Now()
	refTime := extractExecutionStartTime(req)
	deadline, err := schedule.ParseSLADeadline(pipeline.SLA.ValidationTimeout, pipeline.SLA.Timezone, now, refTime)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing validation timeout: %v", err)), nil
	}

	if schedule.IsBreached(deadline, now) {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelError,
			Category:   "validation_timeout",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Validation timeout for %s (deadline: %s)", req.PipelineID, pipeline.SLA.ValidationTimeout),
			Timestamp:  now,
		})
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"validationTimedOut": true},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"validationTimedOut": false},
	}, nil
}

// extractExecutionStartTime extracts the execution start time from the request
// payload. This is injected by the Step Functions ASL InitDefaults state as
// $$.Execution.StartTime. Returns zero time if not present or unparseable.
func extractExecutionStartTime(req intlambda.OrchestratorRequest) time.Time {
	if req.Payload == nil {
		return time.Time{}
	}
	v, ok := req.Payload["executionStartTime"]
	if !ok {
		return time.Time{}
	}
	s, ok := v.(string)
	if !ok {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}
	}
	return t
}
