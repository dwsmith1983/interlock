// trigger Lambda starts external jobs based on trigger configuration.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	trigpkg "github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

var (
	deps     *intlambda.Deps
	depsOnce sync.Once
	depsErr  error
)

func getDeps() (*intlambda.Deps, error) {
	depsOnce.Do(func() {
		deps, depsErr = intlambda.Init(context.Background())
	})
	return deps, depsErr
}

// handleTrigger executes a pipeline trigger and manages run state transitions.
func handleTrigger(ctx context.Context, d *intlambda.Deps, req intlambda.TriggerRequest) (intlambda.TriggerResponse, error) {
	if req.Trigger == nil {
		return intlambda.TriggerResponse{
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
		return intlambda.TriggerResponse{
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
		return intlambda.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  fmt.Sprintf("CAS to TRIGGERING failed: %v", err),
		}, nil
	}

	// Execute the trigger
	metadata, err := d.Runner.Execute(ctx, req.Trigger)
	if err != nil {
		// CAS to FAILED
		failedState := triggeringState
		failedState.Status = types.RunFailed
		failedState.UpdatedAt = time.Now()
		failedState.Version = 3
		failedState.Metadata = map[string]interface{}{"error": err.Error()}
		_, _ = d.Provider.CompareAndSwapRunState(ctx, req.RunID, 2, failedState)

		fc := trigpkg.ClassifyFailure(err)

		// Update run log
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

		return intlambda.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  err.Error(),
		}, nil
	}

	// CAS to RUNNING
	runningState := triggeringState
	runningState.Status = types.RunRunning
	runningState.UpdatedAt = time.Now()
	runningState.Version = 3
	runningState.Metadata = metadata
	_, _ = d.Provider.CompareAndSwapRunState(ctx, req.RunID, 2, runningState)

	// Update run log
	_ = d.Provider.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: req.PipelineID,
		Date:       now.UTC().Format("2006-01-02"),
		ScheduleID: req.ScheduleID,
		Status:     types.RunRunning,
		RunID:      req.RunID,
		StartedAt:  now,
		UpdatedAt:  time.Now(),
	})

	// Append event
	_ = d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: req.PipelineID,
		RunID:      req.RunID,
		Status:     string(types.RunRunning),
		Details:    metadata,
		Timestamp:  time.Now(),
	})

	return intlambda.TriggerResponse{
		RunID:    req.RunID,
		Status:   "running",
		Metadata: metadata,
	}, nil
}

func handler(ctx context.Context, req intlambda.TriggerRequest) (intlambda.TriggerResponse, error) {
	d, err := getDeps()
	if err != nil {
		return intlambda.TriggerResponse{}, err
	}
	return handleTrigger(ctx, d, req)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
