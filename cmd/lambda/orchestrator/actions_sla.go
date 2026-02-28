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

	now := time.Now()
	refTime := extractExecutionStartTime(req)

	result, err := schedule.CheckEvaluationSLA(*pipeline, now, refTime)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing SLA deadline: %v", err)), nil
	}

	// No SLA configured — not breached.
	if result.Deadline.IsZero() {
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": false},
		}, nil
	}

	// At-risk alerting (advance warning before breach, deduped by daily lock).
	if result.AtRisk {
		lockKey := fmt.Sprintf("sla-at-risk:eval:%s:%s:%s", req.PipelineID, req.ScheduleID, now.UTC().Format("2006-01-02"))
		if token, _ := d.Provider.AcquireLock(ctx, lockKey, 24*time.Hour); token != "" {
			d.AlertFn(ctx, types.Alert{
				Level:      types.AlertLevelWarning,
				Category:   "evaluation_sla_at_risk",
				PipelineID: req.PipelineID,
				Message:    fmt.Sprintf("Evaluation SLA at risk for %s (deadline: %s, lead: %s)", req.PipelineID, pipeline.SLA.EvaluationDeadline, pipeline.SLA.AtRiskLeadTime),
				Timestamp:  now,
			})
		}
	}

	if result.Breached {
		d.AlertFn(ctx, types.Alert{
			Level:      types.AlertLevelError,
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

	now := time.Now()
	refTime := extractExecutionStartTime(req)

	result, err := schedule.CheckCompletionSLA(*pipeline, req.ScheduleID, now, refTime)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing completion SLA: %v", err)), nil
	}

	// No deadline configured — not breached.
	if result.Deadline.IsZero() {
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": false},
		}, nil
	}

	// At-risk alerting (advance warning before breach, deduped by daily lock).
	if result.AtRisk {
		lockKey := fmt.Sprintf("sla-at-risk:comp:%s:%s:%s", req.PipelineID, req.ScheduleID, now.UTC().Format("2006-01-02"))
		if token, _ := d.Provider.AcquireLock(ctx, lockKey, 24*time.Hour); token != "" {
			d.AlertFn(ctx, types.Alert{
				Level:      types.AlertLevelWarning,
				Category:   "completion_sla_at_risk",
				PipelineID: req.PipelineID,
				Message:    fmt.Sprintf("Completion SLA at risk for %s schedule %s", req.PipelineID, req.ScheduleID),
				Timestamp:  now,
			})
		}
	}

	if result.Breached {
		d.AlertFn(ctx, types.Alert{
			Level:      types.AlertLevelError,
			Category:   "completion_sla_breach",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Completion SLA breached for %s schedule %s", req.PipelineID, req.ScheduleID),
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

	now := time.Now()
	refTime := extractExecutionStartTime(req)

	timedOut, err := schedule.CheckValidationTimeout(*pipeline, now, refTime)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing validation timeout: %v", err)), nil
	}

	if timedOut {
		d.AlertFn(ctx, types.Alert{
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
