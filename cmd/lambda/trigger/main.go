// trigger Lambda starts external jobs based on trigger configuration.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	trigpkg "github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleTrigger executes a pipeline trigger and manages run state transitions.
func handleTrigger(ctx context.Context, d *intlambda.Deps, req intlambda.TriggerRequest) (intlambda.TriggerResponse, error) {
	if req.Trigger == nil {
		return intlambda.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  "no trigger configuration provided",
		}, nil
	}

	now := time.Now()

	// Create initial run state and CAS to TRIGGERING.
	triggeringState, resp := casToTriggering(ctx, d, req, now)
	if resp != nil {
		return *resp, nil
	}

	// Execute the trigger.
	metadata, err := d.Runner.Execute(ctx, req.Trigger)
	if metadata == nil {
		metadata = map[string]interface{}{}
	}

	if err != nil {
		return handleLambdaTriggerFailure(ctx, d, req, triggeringState, now, err), nil
	}
	return handleLambdaTriggerSuccess(ctx, d, req, triggeringState, now, metadata), nil
}

// casToTriggering creates a PENDING run and transitions to TRIGGERING.
// Returns the triggering state on success, or a failure response.
func casToTriggering(ctx context.Context, d *intlambda.Deps, req intlambda.TriggerRequest, now time.Time) (types.RunState, *intlambda.TriggerResponse) {
	runState := types.RunState{
		RunID:      req.RunID,
		PipelineID: req.PipelineID,
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	if err := d.Provider.PutRunState(ctx, runState); err != nil {
		return types.RunState{}, &intlambda.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  fmt.Sprintf("creating run state: %v", err),
		}
	}

	triggeringState := runState
	triggeringState.Status = types.RunTriggering
	triggeringState.UpdatedAt = time.Now()
	triggeringState.Version = 2
	ok, err := d.Provider.CompareAndSwapRunState(ctx, req.RunID, 1, triggeringState)
	if err != nil || !ok {
		return types.RunState{}, &intlambda.TriggerResponse{
			RunID:  req.RunID,
			Status: "failed",
			Error:  fmt.Sprintf("CAS to TRIGGERING failed: %v", err),
		}
	}

	return triggeringState, nil
}

// handleLambdaTriggerFailure records the failure state and returns a failure response.
func handleLambdaTriggerFailure(ctx context.Context, d *intlambda.Deps, req intlambda.TriggerRequest, triggeringState types.RunState, now time.Time, trigErr error) intlambda.TriggerResponse {
	failedState := triggeringState
	failedState.Status = types.RunFailed
	failedState.UpdatedAt = time.Now()
	failedState.Version = 3
	failedState.Metadata = map[string]interface{}{"error": trigErr.Error()}
	if ok, casErr := d.Provider.CompareAndSwapRunState(ctx, req.RunID, 2, failedState); casErr != nil || !ok {
		d.Logger.Error("CAS to FAILED failed", "runID", req.RunID, "error", casErr)
	}

	fc := trigpkg.ClassifyFailure(trigErr)

	if logErr := d.Provider.PutRunLog(ctx, types.RunLogEntry{
		PipelineID:      req.PipelineID,
		Date:            now.UTC().Format("2006-01-02"),
		ScheduleID:      req.ScheduleID,
		Status:          types.RunFailed,
		RunID:           req.RunID,
		FailureMessage:  trigErr.Error(),
		FailureCategory: fc,
		StartedAt:       now,
		UpdatedAt:       time.Now(),
	}); logErr != nil {
		d.Logger.Error("PutRunLog failed", "runID", req.RunID, "error", logErr)
	}

	d.AlertFn(ctx, types.Alert{
		Level:      types.AlertLevelError,
		PipelineID: req.PipelineID,
		Message:    fmt.Sprintf("Trigger failed for %s: %v", req.PipelineID, trigErr),
		Timestamp:  time.Now(),
	})

	return intlambda.TriggerResponse{
		RunID:  req.RunID,
		Status: "failed",
		Error:  trigErr.Error(),
	}
}

// handleLambdaTriggerSuccess records the success state and returns the response.
func handleLambdaTriggerSuccess(ctx context.Context, d *intlambda.Deps, req intlambda.TriggerRequest, triggeringState types.RunState, now time.Time, metadata map[string]interface{}) intlambda.TriggerResponse {
	// Synchronous triggers (HTTP, Command) complete immediately — no polling needed.
	isSynchronous := req.Trigger.Type == types.TriggerHTTP || req.Trigger.Type == types.TriggerCommand
	targetStatus := types.RunRunning
	responseStatus := "running"
	if isSynchronous {
		targetStatus = types.RunCompleted
		responseStatus = "completed"
	}

	nextState := triggeringState
	nextState.Status = targetStatus
	nextState.UpdatedAt = time.Now()
	nextState.Version = 3
	nextState.Metadata = metadata
	if ok, casErr := d.Provider.CompareAndSwapRunState(ctx, req.RunID, 2, nextState); casErr != nil || !ok {
		d.Logger.Error("CAS to target status failed", "runID", req.RunID, "targetStatus", targetStatus, "error", casErr)
	}

	if err := d.Provider.PutRunLog(ctx, types.RunLogEntry{
		PipelineID: req.PipelineID,
		Date:       now.UTC().Format("2006-01-02"),
		ScheduleID: req.ScheduleID,
		Status:     targetStatus,
		RunID:      req.RunID,
		StartedAt:  now,
		UpdatedAt:  time.Now(),
	}); err != nil {
		d.Logger.Error("PutRunLog failed", "runID", req.RunID, "error", err)
	}

	if err := d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: req.PipelineID,
		RunID:      req.RunID,
		Status:     string(targetStatus),
		Details:    metadata,
		Timestamp:  time.Now(),
	}); err != nil {
		d.Logger.Error("AppendEvent failed", "runID", req.RunID, "error", err)
	}

	return intlambda.TriggerResponse{
		RunID:    req.RunID,
		Status:   responseStatus,
		Metadata: metadata,
	}
}

func handler(ctx context.Context, req intlambda.TriggerRequest) (intlambda.TriggerResponse, error) {
	d, err := intlambda.GetDeps()
	if err != nil {
		return intlambda.TriggerResponse{}, err
	}
	return handleTrigger(ctx, d, req)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
