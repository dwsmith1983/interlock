// orchestrator Cloud Function is the multi-action "brain" of the Cloud Workflows pipeline.
// It handles all orchestration logic delegated by the workflow.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/dwsmith1983/interlock/internal/archetype"
	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

var (
	deps     *intgcpfunc.Deps
	depsOnce sync.Once
	depsErr  error
)

func init() {
	functions.HTTP("Orchestrator", handleHTTP)
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

	var req intgcpfunc.OrchestratorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	resp, err := handleOrchestrator(r.Context(), d, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("handler error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleOrchestrator dispatches to action-specific handlers.
func handleOrchestrator(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
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
	case "notifyDownstream":
		return notifyDownstream(ctx, d, req)
	case "checkValidationTimeout":
		return checkValidationTimeout(ctx, d, req)
	case "checkMonitoringExpired":
		return checkMonitoringExpired(ctx, d, req)
	case "handleLateArrival":
		return handleLateArrival(ctx, d, req)
	default:
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "error",
			Payload: map[string]interface{}{
				"error": fmt.Sprintf("unknown action: %s", req.Action),
			},
		}, nil
	}
}

func checkExclusion(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	if schedule.IsExcluded(*pipeline, nil, time.Now()) {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "excluded day",
			},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func acquireLock(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	lockKey := schedule.LockKey(req.PipelineID, req.ScheduleID)
	acquired, err := d.Provider.AcquireLock(ctx, lockKey, 5*time.Minute)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("acquiring lock: %v", err)), nil
	}

	if !acquired {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "lock not acquired",
			},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func checkRunLog(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	date := time.Now().UTC().Format("2006-01-02")
	entry, err := d.Provider.GetRunLog(ctx, req.PipelineID, date, req.ScheduleID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("getting run log: %v", err)), nil
	}

	if entry == nil {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
			Payload: map[string]interface{}{
				"attemptNumber": 1,
			},
		}, nil
	}

	isReplay, _ := req.Payload["replay"].(bool)
	if entry.Status == types.RunCompleted && !isReplay {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "already completed",
			},
		}, nil
	}

	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("loading pipeline: %v", err)), nil
	}

	retryPolicy := schedule.DefaultRetryPolicy()
	if pipeline.Retry != nil {
		retryPolicy = *pipeline.Retry
	}

	if entry.FailureCategory != "" && !schedule.IsRetryable(retryPolicy, entry.FailureCategory) {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "non-retryable failure",
			},
		}, nil
	}

	if entry.AttemptNumber >= retryPolicy.MaxAttempts {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "max attempts exceeded",
			},
		}, nil
	}

	backoff := schedule.CalculateBackoff(retryPolicy, entry.AttemptNumber)
	if time.Since(entry.UpdatedAt) < backoff {
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason":  "backoff period",
				"backoff": backoff.String(),
			},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
		Payload: map[string]interface{}{
			"attemptNumber": entry.AttemptNumber + 1,
		},
	}, nil
}

func resolvePipeline(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
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

	evalDate := time.Now().UTC().Format("2006-01-02")
	if req.Date != "" {
		evalDate = req.Date
	}

	traits := make([]interface{}, 0, len(resolved))
	for _, rt := range resolved {
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

	if pipeline.Trigger != nil && len(pipeline.Trigger.Arguments) > 0 {
		hour := ""
		if strings.HasPrefix(req.ScheduleID, "h") {
			hour = req.ScheduleID[1:]
		}

		replacer := strings.NewReplacer(
			"${date}", evalDate,
			"${hour}", hour,
			"${scheduleID}", req.ScheduleID,
			"${pipelineID}", req.PipelineID,
		)
		for k, v := range pipeline.Trigger.Arguments {
			pipeline.Trigger.Arguments[k] = replacer.Replace(v)
		}
	}

	payload := map[string]interface{}{
		"traits": traits,
		"runID":  runID,
	}
	if pipeline.Trigger != nil {
		payload["trigger"] = pipeline.Trigger
	}
	if pipeline.Watch != nil && pipeline.Watch.Monitoring != nil && pipeline.Watch.Monitoring.Enabled {
		payload["monitoring"] = true
	} else {
		payload["monitoring"] = false
	}

	return intgcpfunc.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: payload,
	}, nil
}

func generateRunID() (string, error) {
	var uuid [16]byte
	if _, err := rand.Read(uuid[:]); err != nil {
		return "", err
	}
	uuid[6] = (uuid[6] & 0x0f) | 0x40
	uuid[8] = (uuid[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16]), nil
}

func checkReadiness(_ context.Context, _ *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
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
		return intgcpfunc.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason":   "not ready",
				"blocking": blocking,
			},
		}, nil
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func logResult(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	status, _ := req.Payload["status"].(string)
	runID, _ := req.Payload["runID"].(string)
	message, _ := req.Payload["message"].(string)
	failureCategory, _ := req.Payload["failureCategory"].(string)

	date := time.Now().UTC().Format("2006-01-02")
	now := time.Now()

	existing, _ := d.Provider.GetRunLog(ctx, req.PipelineID, date, req.ScheduleID)

	attemptNumber := 1
	var retryHistory []types.RetryAttempt
	if existing != nil {
		attemptNumber = existing.AttemptNumber + 1
		retryHistory = existing.RetryHistory
	}

	entry := types.RunLogEntry{
		PipelineID:    req.PipelineID,
		Date:          date,
		ScheduleID:    req.ScheduleID,
		Status:        types.RunStatus(status),
		AttemptNumber: attemptNumber,
		RunID:         runID,
		RetryHistory:  retryHistory,
		StartedAt:     now,
		UpdatedAt:     now,
	}

	if status == string(types.RunFailed) {
		entry.FailureMessage = message
		entry.FailureCategory = types.FailureCategory(failureCategory)

		entry.RetryHistory = append(entry.RetryHistory, types.RetryAttempt{
			Attempt:         attemptNumber,
			Status:          types.RunFailed,
			RunID:           runID,
			FailureMessage:  message,
			FailureCategory: types.FailureCategory(failureCategory),
			StartedAt:       now,
			CompletedAt:     &now,
		})
	}

	if status == string(types.RunCompleted) {
		entry.CompletedAt = &now
	}

	if err := d.Provider.PutRunLog(ctx, entry); err != nil {
		return errorResponse(req.Action, fmt.Sprintf("writing run log: %v", err)), nil
	}

	_ = d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: req.PipelineID,
		RunID:      runID,
		Status:     status,
		Message:    message,
		Timestamp:  now,
	})

	respPayload := map[string]interface{}{
		"attemptNumber": attemptNumber,
	}

	if status == string(types.RunFailed) {
		pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
		if err == nil && pipeline != nil {
			retryPolicy := schedule.DefaultRetryPolicy()
			if pipeline.Retry != nil {
				retryPolicy = *pipeline.Retry
			}
			retryable := schedule.IsRetryable(retryPolicy, types.FailureCategory(failureCategory))
			canRetry := retryable && attemptNumber < retryPolicy.MaxAttempts
			backoff := schedule.CalculateBackoff(retryPolicy, attemptNumber)

			respPayload["retryable"] = canRetry
			respPayload["retryBackoffSeconds"] = int(backoff.Seconds())
		}
	}

	return intgcpfunc.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: respPayload,
	}, nil
}

func releaseLock(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.OrchestratorRequest) (intgcpfunc.OrchestratorResponse, error) {
	lockKey := schedule.LockKey(req.PipelineID, req.ScheduleID)
	if err := d.Provider.ReleaseLock(ctx, lockKey); err != nil {
		d.Logger.Error("failed to release lock", "key", lockKey, "error", err)
	}

	return intgcpfunc.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func errorResponse(action, msg string) intgcpfunc.OrchestratorResponse {
	return intgcpfunc.OrchestratorResponse{
		Action:  action,
		Result:  "error",
		Payload: map[string]interface{}{"error": msg},
	}
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
