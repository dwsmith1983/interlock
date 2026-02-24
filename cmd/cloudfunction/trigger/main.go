// trigger Cloud Function starts external jobs based on trigger configuration.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
	trigpkg "github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

var (
	deps     *intgcpfunc.Deps
	depsOnce sync.Once
	depsErr  error
)

func init() {
	functions.HTTP("Trigger", handleHTTP)
}

func getDeps() (*intgcpfunc.Deps, error) {
	depsOnce.Do(func() {
		deps, depsErr = intgcpfunc.Init(context.Background())
	})
	return deps, depsErr
}

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	d, err := getDeps()
	if err != nil {
		http.Error(w, fmt.Sprintf("init error: %v", err), http.StatusInternalServerError)
		return
	}

	var req intgcpfunc.TriggerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	resp, err := handleTrigger(r.Context(), d, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("handler error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleTrigger executes a pipeline trigger and manages run state transitions.
func handleTrigger(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.TriggerRequest) (intgcpfunc.TriggerResponse, error) {
	if req.Trigger == nil {
		return intgcpfunc.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  "no trigger configuration provided",
		}, nil
	}

	// Create initial run state as PENDING
	now := time.Now()
	runState := types.RunState{
		RunID:      req.RunID,
		PipelineID: req.PipelineID,
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := d.Provider.PutRunState(ctx, runState); err != nil {
		return intgcpfunc.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  fmt.Sprintf("creating run state: %v", err),
		}, nil
	}

	// CAS to TRIGGERING
	triggeringState := runState
	triggeringState.Status = types.RunTriggering
	triggeringState.UpdatedAt = time.Now()
	triggeringState.Version = 2
	ok, err := d.Provider.CompareAndSwapRunState(ctx, req.RunID, 1, triggeringState)
	if err != nil || !ok {
		return intgcpfunc.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  fmt.Sprintf("CAS to TRIGGERING failed: %v", err),
		}, nil
	}

	// Execute the trigger
	metadata, err := d.Runner.Execute(ctx, req.Trigger)
	if metadata == nil {
		metadata = map[string]interface{}{}
	}
	if err != nil {
		// CAS to FAILED
		failedState := triggeringState
		failedState.Status = types.RunFailed
		failedState.UpdatedAt = time.Now()
		failedState.Version = 3
		failedState.Metadata = map[string]interface{}{"error": err.Error()}
		_, _ = d.Provider.CompareAndSwapRunState(ctx, req.RunID, 2, failedState)

		fc := trigpkg.ClassifyFailure(err)

		_ = d.Provider.PutRunLog(ctx, types.RunLogEntry{
			PipelineID:      req.PipelineID,
			Date:            now.UTC().Format("2006-01-02"),
			ScheduleID:      req.ScheduleID,
			Status:          types.RunFailed,
			RunID:           req.RunID,
			FailureMessage:  err.Error(),
			FailureCategory: fc,
			StartedAt:       now,
			UpdatedAt:       time.Now(),
		})

		d.AlertFn(types.Alert{
			Level:      types.AlertLevelError,
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Trigger failed for %s: %v", req.PipelineID, err),
			Timestamp:  time.Now(),
		})

		return intgcpfunc.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  err.Error(),
		}, nil
	}

	// Synchronous triggers complete immediately; async ones need polling.
	isSynchronous := req.Trigger.Type == types.TriggerHTTP || req.Trigger.Type == types.TriggerCommand
	targetStatus := types.RunRunning
	responseStatus := "running"
	if isSynchronous {
		targetStatus = types.RunCompleted
		responseStatus = "completed"
	}

	// CAS to target status
	nextState := triggeringState
	nextState.Status = targetStatus
	nextState.UpdatedAt = time.Now()
	nextState.Version = 3
	nextState.Metadata = metadata
	_, _ = d.Provider.CompareAndSwapRunState(ctx, req.RunID, 2, nextState)

	_ = d.Provider.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: req.PipelineID,
		Date:       now.UTC().Format("2006-01-02"),
		ScheduleID: req.ScheduleID,
		Status:     targetStatus,
		RunID:      req.RunID,
		StartedAt:  now,
		UpdatedAt:  time.Now(),
	})

	_ = d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: req.PipelineID,
		RunID:      req.RunID,
		Status:     string(targetStatus),
		Details:    metadata,
		Timestamp:  time.Now(),
	})

	return intgcpfunc.TriggerResponse{
		RunID:    req.RunID,
		Status:   responseStatus,
		Metadata: metadata,
	}, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	if err := funcframework.Start(port); err != nil {
		slog.Error("failed to start functions framework", "error", err)
		os.Exit(1)
	}
}
