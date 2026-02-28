// orchestrator Lambda is the multi-action "brain" of the Step Function workflow.
// It handles all orchestration logic delegated by the state machine.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	awslambda "github.com/aws/aws-lambda-go/lambda"
	"github.com/dwsmith1983/interlock/internal/archetype"
	intlambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/internal/schedule"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// handleOrchestrator dispatches to action-specific handlers.
func handleOrchestrator(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	switch req.Action {
	case "checkExclusion":
		return checkExclusion(ctx, d, req)
	case "acquireLock":
		return acquireLock(ctx, d, req)
	case "checkRunLog":
		return checkRunLog(ctx, d, req)
	case "checkCircuitBreaker":
		return checkCircuitBreaker(ctx, d, req)
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
	case "checkQuarantine":
		return checkQuarantine(ctx, d, req)
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
	lockKey := schedule.LockKey(req.PipelineID, req.ScheduleID)
	token, err := d.Provider.AcquireLock(ctx, lockKey, 5*time.Minute)
	if err != nil {
		return errorResponse(req.Action, fmt.Sprintf("acquiring lock: %v", err)), nil
	}

	if token == "" {
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
		Payload: map[string]interface{}{
			"lockToken": token,
		},
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

	// Already completed successfully — unless this is a replay
	isReplay, _ := req.Payload["replay"].(bool)
	if entry.Status == types.RunCompleted && !isReplay {
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

func checkCircuitBreaker(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	threshold := 0
	if v := os.Getenv("CIRCUIT_BREAKER_THRESHOLD"); v != "" {
		threshold, _ = strconv.Atoi(v)
	}
	// Circuit breaker disabled if threshold is 0 or unset.
	if threshold <= 0 {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
		}, nil
	}

	record, err := d.Provider.GetControlStatus(ctx, req.PipelineID)
	if err != nil {
		d.Logger.Error("failed to read CONTROL record for circuit breaker",
			"pipeline", req.PipelineID, "error", err)
		// Fail open — don't block the pipeline on a read error.
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
		}, nil
	}

	if record == nil {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "proceed",
		}, nil
	}

	if !record.Enabled {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason": "pipeline disabled via CONTROL record",
			},
		}, nil
	}

	if record.ConsecutiveFailures >= threshold {
		d.AlertFn(ctx, types.Alert{
			Level:      types.AlertLevelError,
			Category:   "circuit_breaker_open",
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("Circuit breaker open: %d consecutive failures (threshold=%d)", record.ConsecutiveFailures, threshold),
			Details: map[string]interface{}{
				"consecutiveFailures": record.ConsecutiveFailures,
				"threshold":           threshold,
				"lastFailedRun":       record.LastFailedRun,
			},
			Timestamp: time.Now(),
		})

		if err := d.Provider.AppendEvent(ctx, types.Event{
			Kind:       types.EventCircuitBreakerTripped,
			PipelineID: req.PipelineID,
			Message:    fmt.Sprintf("circuit breaker open: %d failures >= threshold %d", record.ConsecutiveFailures, threshold),
			Timestamp:  time.Now(),
		}); err != nil {
			d.Logger.Error("AppendEvent failed for circuit breaker", "pipeline", req.PipelineID, "error", err)
		}

		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "skip",
			Payload: map[string]interface{}{
				"reason":              "circuit breaker open",
				"consecutiveFailures": record.ConsecutiveFailures,
				"threshold":           threshold,
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
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

	// Resolve template variables in trigger arguments. Pipeline configs can
	// reference execution context via ${var} placeholders (e.g. "--date": "${date}",
	// "--hour": "${hour}") so they declare exactly what their triggers need.
	if args := pipeline.Trigger.TriggerArguments(); len(args) > 0 {
		// Derive hour from scheduleID (e.g. "h15" → "15").
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
		for k, v := range args {
			args[k] = replacer.Replace(v)
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
	var errorTraits []string
	allPass := true
	hasErrors := false

	for _, tr := range traitResults {
		traitMap, ok := tr.(map[string]interface{})
		if !ok {
			continue
		}
		status, _ := traitMap["status"].(string)
		required, _ := traitMap["required"].(bool)
		traitType, _ := traitMap["traitType"].(string)
		reason, _ := traitMap["reason"].(string)

		if status == string(types.TraitError) {
			hasErrors = true
			errorTraits = append(errorTraits, fmt.Sprintf("%s: %s", traitType, reason))
		}

		if status != string(types.TraitPass) && required {
			blocking = append(blocking, traitType)
			allPass = false
		}
	}

	// Evaluator errors (HTTP failures, timeouts, crashes) are distinct from
	// "data not ready" — they indicate infrastructure or config problems
	// and should be alerted on rather than silently retried.
	if hasErrors {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "error",
			Payload: map[string]interface{}{
				"reason":      "evaluator errors",
				"errorTraits": errorTraits,
			},
		}, nil
	}

	if !allPass {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "not_ready",
			Payload: map[string]interface{}{
				"pollAdvised":  true,
				"failedTraits": blocking,
				"message":      fmt.Sprintf("blocked by %d traits", len(blocking)),
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func logResult(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	status, _ := req.Payload["status"].(string)
	runID, _ := req.Payload["runID"].(string)
	message, _ := req.Payload["message"].(string)
	failureCategory, _ := req.Payload["failureCategory"].(string)

	date := time.Now().UTC().Format("2006-01-02")
	now := time.Now()

	// Load existing entry to preserve/extend retry history
	existing, _ := d.Provider.GetRunLog(ctx, req.PipelineID, date, req.ScheduleID)

	attemptNumber := 1
	var retryHistory []types.RetryAttempt
	if existing != nil {
		retryHistory = existing.RetryHistory
		// Only increment attempt counter when retrying after a terminal state.
		// Transitioning within the same attempt (e.g., PENDING → COMPLETED)
		// preserves the counter.
		if lifecycle.IsTerminal(existing.Status) {
			attemptNumber = existing.AttemptNumber + 1
		} else {
			attemptNumber = existing.AttemptNumber
		}
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
		if failureCategory == "" {
			failureCategory = string(types.FailureTransient)
		}
		entry.FailureMessage = message
		entry.FailureCategory = types.FailureCategory(failureCategory)

		// Append to retry history
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

	// Append event
	if err := d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: req.PipelineID,
		RunID:      runID,
		Status:     status,
		Message:    message,
		Timestamp:  now,
	}); err != nil {
		d.Logger.Error("AppendEvent failed", "pipeline", req.PipelineID, "runID", runID, "error", err)
	}

	// Build response payload — include retry info for SFN to use
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

	return intlambda.OrchestratorResponse{
		Action:  req.Action,
		Result:  "proceed",
		Payload: respPayload,
	}, nil
}

func releaseLock(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	lockKey := schedule.LockKey(req.PipelineID, req.ScheduleID)
	token, _ := req.Payload["lockToken"].(string)
	if err := d.Provider.ReleaseLock(ctx, lockKey, token); err != nil {
		d.Logger.Error("failed to release lock", "key", lockKey, "error", err)
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "proceed",
	}, nil
}

func checkQuarantine(ctx context.Context, d *intlambda.Deps, req intlambda.OrchestratorRequest) (intlambda.OrchestratorResponse, error) {
	date := time.Now().UTC().Format("2006-01-02")
	if req.Date != "" {
		date = req.Date
	}

	// Derive par_day (YYYYMMDD) and hour from scheduleID/date
	parDay := strings.ReplaceAll(date, "-", "")
	hour := ""
	if strings.HasPrefix(req.ScheduleID, "h") {
		hour = req.ScheduleID[1:]
	}

	record, err := d.Provider.GetQuarantineRecord(ctx, req.PipelineID, parDay, hour)
	if err != nil {
		d.Logger.Error("failed to query quarantine record",
			"pipeline", req.PipelineID, "date", parDay, "hour", hour, "error", err)
		// Don't fail the pipeline on a quarantine check error
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "clean",
		}, nil
	}

	if record == nil {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "clean",
		}, nil
	}

	// Quarantine found — check blocking config
	pipeline, err := d.Provider.GetPipeline(ctx, req.PipelineID)
	if err != nil {
		d.Logger.Error("failed to load pipeline for quarantine check",
			"pipeline", req.PipelineID, "error", err)
	}

	blocking := false
	if pipeline != nil && pipeline.Quarantine != nil {
		blocking = pipeline.Quarantine.Blocking
	}

	// Fire alert with quarantine receipt
	d.AlertFn(ctx, types.Alert{
		Level:      types.AlertLevelWarning,
		Category:   "data_quarantined",
		PipelineID: req.PipelineID,
		Message:    fmt.Sprintf("%d records quarantined", record.Count),
		Details: map[string]interface{}{
			"count":          record.Count,
			"quarantinePath": record.QuarantinePath,
			"reasons":        record.Reasons,
			"date":           parDay,
			"hour":           hour,
		},
		Timestamp: time.Now(),
	})

	// Append event
	if err := d.Provider.AppendEvent(ctx, types.Event{
		Kind:       types.EventDataQuarantined,
		PipelineID: req.PipelineID,
		Status:     "QUARANTINED",
		Message:    fmt.Sprintf("%d records quarantined: %v", record.Count, record.Reasons),
		Details: map[string]interface{}{
			"count":          record.Count,
			"quarantinePath": record.QuarantinePath,
			"reasons":        record.Reasons,
		},
		Timestamp: time.Now(),
	}); err != nil {
		d.Logger.Error("AppendEvent failed for quarantine",
			"pipeline", req.PipelineID, "error", err)
	}

	if blocking {
		return intlambda.OrchestratorResponse{
			Action: req.Action,
			Result: "blocked",
			Payload: map[string]interface{}{
				"count":          record.Count,
				"quarantinePath": record.QuarantinePath,
				"reasons":        record.Reasons,
			},
		}, nil
	}

	return intlambda.OrchestratorResponse{
		Action: req.Action,
		Result: "quarantined",
		Payload: map[string]interface{}{
			"count":          record.Count,
			"quarantinePath": record.QuarantinePath,
			"reasons":        record.Reasons,
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
	d, err := intlambda.GetDeps()
	if err != nil {
		return intlambda.OrchestratorResponse{}, err
	}
	return handleOrchestrator(ctx, d, req)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	awslambda.Start(handler)
}
