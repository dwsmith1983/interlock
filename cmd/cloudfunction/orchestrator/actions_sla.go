package main

import (
	"context"
	"fmt"
	"time"

	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func checkEvaluationSLA(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.SLA == nil || pipeline.SLA.EvaluationDeadline == "" {
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": false},
		}, nil
	}

	now := time.Now()
	deadline, err := schedule.ParseSLADeadline(pipeline.SLA.EvaluationDeadline, pipeline.SLA.Timezone, now)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing SLA deadline: %v", err)), nil
	}

	if schedule.IsBreached(deadline, now) {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelWarning,
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Evaluation SLA breached for %s (deadline: %s)", req.PipelineID, pipeline.SLA.EvaluationDeadline),
			Timestamp:  now,
		})
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": true},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"breached": false},
	}, nil
}

func checkCompletionSLA(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	scheduleID := req.ScheduleID
	var sched types.ScheduleConfig
	for _, s := range types.ResolveSchedules(*pipeline) {
		if s.Name == scheduleID {
			sched = s
			break
		}
	}

	now := time.Now()
	deadline, ok := schedule.ScheduleDeadline(sched, *pipeline, now)
	if !ok {
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": false},
		}, nil
	}

	if schedule.IsBreached(deadline, now) {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelWarning,
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Completion SLA breached for %s schedule %s", req.PipelineID, scheduleID),
			Timestamp:  now,
		})
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"breached": true},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"breached": false},
	}, nil
}

func checkValidationTimeout(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.SLA == nil || pipeline.SLA.ValidationTimeout == "" {
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"validationTimedOut": false},
		}, nil
	}

	now := time.Now()
	deadline, err := schedule.ParseSLADeadline(pipeline.SLA.ValidationTimeout, pipeline.SLA.Timezone, now)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing validation timeout: %v", err)), nil
	}

	if schedule.IsBreached(deadline, now) {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Validation timeout for %s (deadline: %s)", req.PipelineID, pipeline.SLA.ValidationTimeout),
			Timestamp:  now,
		})
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"validationTimedOut": true},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"validationTimedOut": false},
	}, nil
}
