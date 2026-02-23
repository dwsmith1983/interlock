// orchestrator Lambda is the multi-action "brain" of the Step Function workflow.
// It handles all orchestration logic delegated by the state machine.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/dwsmith1983/interlock/internal/archetype"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/schedule"
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

// handleOrchestrator dispatches to action-specific handlers.
func handleOrchestrator(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	switch req.Action {
	case "checkExclusion":
		return checkExclusion(ctx, d, req)
	case "acquireLock":
		return acquireLock(ctx, d, req)
	case "checkRunLog":
		return checkRunLog(ctx, d, req)
	case "resolvePipeline":
		return resolvePipeline(ctx, d, req)
	case "checkReadiness":
		return checkReadiness(ctx, d, req)
	case "checkEvaluationSLA":
		return checkEvaluationSLA(ctx, d, req)
	case "checkCompletionSLA":
		return checkCompletionSLA(ctx, d, req)
	case "logResult":
		return logResult(ctx, d, req)
	case "releaseLock":
		return releaseLock(ctx, d, req)
	case "checkDrift":
		return checkDrift(ctx, d, req)
	default:
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "error",
			Payload: map[string]interface{}{
				"error": fmt.Sprintf("unknown action: %s", req.Action),
			},
		}, nil
	}
}

func checkExclusion(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if schedule.IsExcluded(*pipeline, nil, time.Now()) {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "excluded day",
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func acquireLock(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	lockKey := fmt.Sprintf("eval:%s:%s", req.PipelineID, req.ScheduleID)
	acquired, err := d.Provider.AcquireLock(ctx, lockKey, 5*time.Minute)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("acquiring lock: %v", err)), nil
	}

	if !acquired {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "lock not acquired",
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func checkRunLog(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	date := time.Now().UTC().Format("2006-01-02")
	entry, err := d.Provider.GetRunLog(ctx, req.PipelineID, date, req.ScheduleID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("getting run log: %v", err)), nil
	}

	// No entry yet — proceed with first attempt
	if entry == nil {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
			Payload: map[string]interface{}{
				"attemptNumber": 1,
			},
		}, nil
	}

	// Already completed successfully
	if entry.Status == types.RunCompleted {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "already completed",
			},
		}, nil
	}

	// Check if failure is retryable
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	retryPolicy := schedule.DefaultRetryPolicy()
	if pipeline.Retry != nil {
		retryPolicy = *pipeline.Retry
	}

	// Non-retryable failure
	if entry.FailureCategory != "" && !schedule.IsRetryable(retryPolicy, entry.FailureCategory) {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "non-retryable failure",
			},
		}, nil
	}

	// Max attempts exceeded
	if entry.AttemptNumber >= retryPolicy.MaxAttempts {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "max attempts exceeded",
			},
		}, nil
	}

	// Check backoff
	backoff := schedule.CalculateBackoff(retryPolicy, entry.AttemptNumber)
	if time.Since(entry.UpdatedAt) < backoff {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason":  "backoff period",
				"backoff": backoff.String(),
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"attemptNumber": entry.AttemptNumber + 1,
		},
	}, nil
}

func resolvePipeline(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if pipeline.Archetype == "" {
		return errorResponse(req.Action, "pipeline has no archetype configured"), nil
	}

	arch, err := d.ArchetypeReg.Get(pipeline.Archetype)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("resolving archetype %q: %v", pipeline.Archetype, err)), nil
	}

	resolved := archetype.ResolveTraits(arch, pipeline)

	// Determine evaluation date: use explicit date from request if provided,
	// otherwise default to today's UTC date.
	evalDate := time.Now().UTC().Format("2006-01-02")
	if req.Date != "" {
		evalDate = req.Date
	}

	traits := make([]interface{}, 0, len(resolved))
	for _, rt := range resolved {
		// Inject scheduleID and date into config so evaluators can scope to
		// the right time partition.
		if rt.Config == nil {
			rt.Config = make(map[string]interface{})
		}
		rt.Config["scheduleID"] = req.ScheduleID
		rt.Config["date"] = evalDate

		traits = append(traits, map[string]interface{}{
			"pipelineID": req.PipelineID,
			"traitType":  rt.Type,
			"evaluator":  rt.Evaluator,
			"config":     rt.Config,
			"timeout":    rt.Timeout,
			"ttl":        rt.TTL,
			"required":   rt.Required,
		})
	}

	runID, err := generateRunID()
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("generating run ID: %v", err)), nil
	}

	payload := map[string]interface{}{
		"traits": traits,
		"runID":  runID,
	}
	if pipeline.Trigger != nil {
		payload["trigger"] = pipeline.Trigger
	}

	return intlambda.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: payload,
	}, nil
}

// generateRunID produces a UUID v4 using crypto/rand.
func generateRunID() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

func checkReadiness(_ context.Context, _ *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	// Trait results come via the Step Function payload
	traitResults, _ := req.Payload["traitResults"].([]interface{})

	var blocking []string
	allPass := true

	for _, tr := range traitResults {
		traitMap, ok := tr.(map[string]interface{})
		if !ok {
			continue
		}
		status, _ := traitMap["status"].(string)
		required, _ := traitMap["required"].(bool)
		traitType, _ := traitMap["traitType"].(string)

		if status != string(types.TraitPass) && required {
			blocking = append(blocking, traitType)
			allPass = false
		}
	}

	if !allPass {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason":   "not ready",
				"blocking": blocking,
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

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
	deadline, ok := schedule.ScheduleDeadline(sched, *pipeline, now)
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

func logResult(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	status, _ := req.Payload["status"].(string)
	runID, _ := req.Payload["runID"].(string)
	message, _ := req.Payload["message"].(string)

	date := time.Now().UTC().Format("2006-01-02")
	now := time.Now()

	entry := types.RunLogEntry{
		PipelineID: req.PipelineID,
		Date:       date,
		ScheduleID: req.ScheduleID,
		Status:     types.RunStatus(status),
		RunID:      runID,
		UpdatedAt:  now,
	}

	if status == string(types.RunFailed) {
		entry.FailureMessage = message
	}

	if err := d.Provider.PutRunLog(ctx, entry); err != nil {
		return errorResponse(req.Action, fmt.Sprintf("writing run log: %v", err)), nil
	}

	// Append event
	_ = d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: req.PipelineID,
		RunID:      runID,
		Status:     status,
		Message:    message,
		Timestamp:  now,
	})

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func releaseLock(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	lockKey := fmt.Sprintf("eval:%s:%s", req.PipelineID, req.ScheduleID)
	if err := d.Provider.ReleaseLock(ctx, lockKey); err != nil {
		d.Logger.Error("failed to release lock", "key", lockKey, "error", err)
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
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

func errorResponse(action, msg string) intlambda.OrchestratorResponse {
	return intlambda.OrchestratorResponse{
		Action:  action,
		Result:  "error",
		Payload: map[string]interface{}{"error": msg},
	}
}

func handler(ctx context.Context, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	d, err := getDeps()
	if err != nil {
		return intlambda.OrchestratorResponse{}, err
	}
	return handleOrchestrator(ctx, d, req)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
