package main

import (
	"context"
	"fmt"
	"time"

	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func notifyDownstream(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	pipelines, err := d.Provider.ListPipelines(ctx)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("listing pipelines: %v", err)), nil
	}

	date := time.Now().UTC().Format("2006-01-02")
	if req.Date != "" {
		date = req.Date
	}

	var notified []string
	for _, p := range pipelines {
		if p.Name == req.PipelineID {
			continue
		}
		for _, tc := range p.Traits {
			upstreamPipeline, _ := tc.Config["upstreamPipeline"].(string)
			if upstreamPipeline == req.PipelineID {
				if err := d.Provider.WriteCascadeMarker(ctx, p.Name, req.ScheduleID, date, req.PipelineID); err != nil {
					d.Logger.Error("failed to write cascade marker",
						"downstream", p.Name, "schedule", req.ScheduleID, "error", err)
				} else {
					notified = append(notified, p.Name+"/"+req.ScheduleID)
				}
				break
			}
		}
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"notified": notified,
		},
	}, nil
}

func checkDrift(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	traitResults, _ := req.Payload["traitResults"].([]interface{})
	runID, _ := req.Payload["runID"].(string)

	var drifted []string
	for _, tr := range traitResults {
		traitMap, ok := tr.(map[string]interface{})
		if !ok {
			continue
		}
		traitType, _ := traitMap["traitType"].(string)
		originalStatus, _ := traitMap["originalStatus"].(string)
		currentStatus, _ := traitMap["currentStatus"].(string)

		if originalStatus != currentStatus {
			drifted = append(drifted, traitType)
		}
	}

	if len(drifted) > 0 {
		d.AlertFn(types.Alert{
			Level:      types.AlertLevelWarning,
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Trait drift detected for %s: %v", req.PipelineID, drifted),
			Details:    map[string]interface{}{"drifted": drifted, "runID": runID},
			Timestamp:  time.Now(),
		})

		_ = d.Provider.AppendEvent(ctx, types.Event{
			Kind:       types.EventMonitoringDrift,
			PipelineID: req.PipelineID,
			RunID:      runID,
			Details:    map[string]interface{}{"drifted": drifted},
			Timestamp:  time.Now(),
		})

		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
			Payload: map[string]interface{}{
				"driftDetected": true,
				"drifted":       drifted,
			},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"driftDetected": false,
		},
	}, nil
}

func checkMonitoringExpired(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.Watch == nil || pipeline.Watch.Monitoring == nil {
		return intgcpfunc.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"expired": true},
		}, nil
	}

	duration, err := time.ParseDuration(pipeline.Watch.Monitoring.Duration)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing monitoring duration: %v", err)), nil
	}

	startedAtStr, _ := req.Payload["monitoringStartedAt"].(string)
	startedAt, err := time.Parse(time.RFC3339, startedAtStr)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing monitoringStartedAt: %v", err)), nil
	}

	expired := time.Since(startedAt) >= duration
	return intgcpfunc.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"expired": expired},
	}, nil
}

func handleLateArrival(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	drifted, _ := req.Payload["drifted"].([]interface{})
	runID, _ := req.Payload["runID"].(string)
	date := time.Now().UTC().Format("2006-01-02")

	for _, drift := range drifted {
		traitType, _ := drift.(string)
		if traitType == "" {
			continue
		}
		if err := d.Provider.PutLateArrival(ctx, types.LateArrival{
			PipelineID: req.PipelineID,
			Date:       date,
			ScheduleID: req.ScheduleID,
			DetectedAt: time.Now(),
			TraitType:  traitType,
		}); err != nil {
			d.Logger.Error("failed to store late arrival",
				"pipeline", req.PipelineID, "trait", traitType, "error", err)
		}
	}

	if err := d.Provider.PutRerun(ctx, types.RerunRecord{
		RerunID:      "rerun-" + runID,
		PipelineID:   req.PipelineID,
		OriginalDate: date,
		Reason:       "late arrival detected",
		Status:       types.RunPending,
		RequestedAt:  time.Now(),
	}); err != nil {
		d.Logger.Error("failed to create rerun record",
			"pipeline", req.PipelineID, "error", err)
	}

	if err := d.Provider.WriteCascadeMarker(ctx, req.PipelineID, req.ScheduleID, date, "late-arrival"); err != nil {
		d.Logger.Error("failed to write self-cascade for late arrival",
			"pipeline", req.PipelineID, "error", err)
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"lateArrivalHandled": true,
		},
	}, nil
}
