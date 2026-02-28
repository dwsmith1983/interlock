package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func notifyDownstream(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	date := time.Now().UTC().Format("2006-01-02")
	if req.Date != "" {
		date = req.Date
	}

	// Use dependency index for O(1) downstream lookup.
	dependents, err := d.Provider.ListDependents(ctx, req.PipelineID)
	if err != nil {
		d.Logger.Warn("ListDependents failed, falling back to scan", "pipeline", req.PipelineID, "error", err)
	}

	// Fallback to full scan if dependency index returns empty (bootstrap period).
	if len(dependents) == 0 {
		dependents, err = scanDependents(ctx, d, req.PipelineID)
		if err != nil {
			return errorResponse(req.Action, fmt.Sprintf("scanning dependents: %v", err)), nil
		}
	}

	// Resolve cascade backpressure delay.
	var cascadeDelay time.Duration
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err == nil && pipeline != nil && pipeline.Cascade != nil && pipeline.Cascade.DelayBetween != "" {
		if d, parseErr := time.ParseDuration(pipeline.Cascade.DelayBetween); parseErr == nil {
			cascadeDelay = d
		}
	}

	var notified []string
	for i, downstream := range dependents {
		// Apply backpressure delay between cascade writes.
		if cascadeDelay > 0 && i > 0 {
			time.Sleep(cascadeDelay)
		}
		if writeErr := d.Provider.WriteCascadeMarker(ctx, downstream, req.ScheduleID, date, req.PipelineID); writeErr != nil {
			d.Logger.Error("failed to write cascade marker",
				"downstream", downstream, "schedule", req.ScheduleID, "error", writeErr)
		} else {
			notified = append(notified, downstream+"/"+req.ScheduleID)
		}
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"notified": notified,
		},
	}, nil
}

// scanDependents performs a full pipeline scan to find downstreams (fallback path).
func scanDependents(ctx context.Context, d *intlambda.Deps, upstreamID string) ([]string, error) {
	pipelines, err := d.Provider.ListPipelines(ctx)
	if err != nil {
		return nil, err
	}
	var dependents []string
	for _, p := range pipelines {
		if p.Name == upstreamID {
			continue
		}
		for _, tc := range p.Traits {
			up, _ := tc.Config["upstreamPipeline"].(string)
			if up == upstreamID {
				dependents = append(dependents, p.Name)
				break
			}
		}
	}
	return dependents, nil
}

func checkDrift(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	// Compare current trait state vs post-completion snapshot
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
		d.AlertFn(ctx, types.Alert{
			Level:      types.AlertLevelWarning,
			Category:   "trait_drift",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Trait drift detected for %s: %v", req.PipelineID, drifted),
			Details:    map[string]interface{}{"drifted": drifted, "runID": runID},
			Timestamp:  time.Now(),
		})

		if err := d.Provider.AppendEvent(ctx, types.Event{
			Kind:       types.EventMonitoringDrift,
			PipelineID: req.PipelineID,
			RunID:      runID,
			Details:    map[string]interface{}{"drifted": drifted},
			Timestamp:  time.Now(),
		}); err != nil {
			d.Logger.Error("AppendEvent failed", "pipeline", req.PipelineID, "runID", runID, "error", err)
		}

		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
			Payload: map[string]interface{}{
				"driftDetected": true,
				"drifted":       drifted,
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"driftDetected": false,
		},
	}, nil
}

func checkMonitoringExpired(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.Watch == nil || pipeline.Watch.Monitoring == nil {
		return intlambda.OrchestratorResponse{
			Action:  req.Action,
			Result:  "proceed",
			Payload: map[string]interface{}{"expired": true},
		}, nil
	}

	duration, err := time.ParseDuration(pipeline.Watch.Monitoring.Duration)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing monitoring duration: %v", err)), nil
	}

	// startedAt is passed in payload from when monitoring began
	startedAtStr, _ := req.Payload["monitoringStartedAt"].(string)
	startedAt, err := time.Parse(time.RFC3339, startedAtStr)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("parsing monitoringStartedAt: %v", err)), nil
	}

	expired := time.Since(startedAt) >= duration
	return intlambda.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: map[string]interface{}{"expired": expired},
	}, nil
}

func handleLateArrival(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
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

	// Check rerun cap before creating a new rerun.
	maxReruns := 5
	if v := os.Getenv("MAX_RERUNS_PER_DAY"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxReruns = n
		}
	}

	reruns, err := d.Provider.ListReruns(ctx, req.PipelineID, 50)
	if err != nil {
		d.Logger.Error("failed to list reruns for cap check",
			"pipeline", req.PipelineID, "error", err)
	}

	todayRerunCount := 0
	for _, r := range reruns {
		if r.OriginalDate == date {
			todayRerunCount++
		}
	}

	if todayRerunCount >= maxReruns {
		d.Logger.Warn("rerun cap reached, skipping rerun creation",
			"pipeline", req.PipelineID, "date", date, "count", todayRerunCount, "max", maxReruns)

		d.AlertFn(ctx, types.Alert{
			Level:      types.AlertLevelWarning,
			Category:   "rerun_cap_reached",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Rerun cap reached: %d reruns today (max=%d)", todayRerunCount, maxReruns),
			Details: map[string]interface{}{
				"date":          date,
				"rerunCount":    todayRerunCount,
				"maxRerunsDay":  maxReruns,
				"driftedTraits": drifted,
			},
			Timestamp: time.Now(),
		})

		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
			Payload: map[string]interface{}{
				"lateArrivalHandled": false,
				"reason":             "rerun cap reached",
				"rerunCount":         todayRerunCount,
				"maxRerunsPerDay":    maxReruns,
			},
		}, nil
	}

	// Create a rerun record for the pipeline
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

	// Write cascade marker so the pipeline gets re-evaluated
	if err := d.Provider.WriteCascadeMarker(ctx, req.PipelineID, req.ScheduleID, date, "late-arrival"); err != nil {
		d.Logger.Error("failed to write self-cascade for late arrival",
			"pipeline", req.PipelineID, "error", err)
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"lateArrivalHandled": true,
		},
	}, nil
}
