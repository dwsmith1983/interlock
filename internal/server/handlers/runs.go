package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/interlock-systems/interlock/internal/lifecycle"
	"github.com/interlock-systems/interlock/pkg/types"
)

// RunPipeline evaluates readiness and triggers the pipeline if ready.
func (h *Handlers) RunPipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")

	result, err := h.engine.Evaluate(r.Context(), pipelineID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "evaluation failed", err)
		return
	}

	if result.Status != types.Ready {
		w.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "NOT_READY",
			"blocking": result.Blocking,
		})
		return
	}

	runID := fmt.Sprintf("%s-%d", pipelineID, time.Now().UnixNano())
	run := types.RunState{
		RunID:      runID,
		PipelineID: pipelineID,
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := h.provider.PutRunState(r.Context(), run); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to create run", err)
		return
	}

	if err := h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: pipelineID,
		RunID:      runID,
		Status:     string(types.RunPending),
		Message:    "run created",
		Timestamp:  time.Now(),
	}); err != nil {
		h.logger.Error("failed to append event", "pipeline", pipelineID, "event", "RUN_STATE_CHANGED", "error", err)
	}

	// CAS to TRIGGERING
	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := h.provider.CompareAndSwapRunState(r.Context(), runID, 1, run)
	if err != nil || !ok {
		h.writeError(w, http.StatusConflict, "failed to acquire trigger lock", err)
		return
	}

	if err := h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: pipelineID,
		RunID:      runID,
		Status:     string(types.RunTriggering),
		Message:    "trigger lock acquired",
		Timestamp:  time.Now(),
	}); err != nil {
		h.logger.Error("failed to append event", "pipeline", pipelineID, "event", "RUN_STATE_CHANGED", "error", err)
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(run)
}

// ListRuns returns recent runs for a pipeline.
func (h *Handlers) ListRuns(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	runs, err := h.provider.ListRuns(r.Context(), pipelineID, 20)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list runs", err)
		return
	}
	if runs == nil {
		runs = []types.RunState{}
	}
	_ = json.NewEncoder(w).Encode(runs)
}

// GetRun returns a single run state.
func (h *Handlers) GetRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := h.provider.GetRunState(r.Context(), runID)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "run not found", err)
		return
	}
	_ = json.NewEncoder(w).Encode(run)
}

// CompleteRun handles orchestrator completion callbacks.
func (h *Handlers) CompleteRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")

	var body struct {
		Status   string                 `json:"status"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON", err)
		return
	}

	run, err := h.provider.GetRunState(r.Context(), runID)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "run not found", err)
		return
	}

	var newStatus types.RunStatus
	switch body.Status {
	case "success":
		pipeline, pErr := h.provider.GetPipeline(r.Context(), run.PipelineID)
		if pErr == nil && pipeline.Watch != nil &&
			pipeline.Watch.Monitoring != nil && pipeline.Watch.Monitoring.Enabled {
			newStatus = types.RunCompletedMonitoring
		} else {
			newStatus = types.RunCompleted
		}
	case "failed":
		newStatus = types.RunFailed
	default:
		h.writeError(w, http.StatusBadRequest, "status must be 'success' or 'failed'", nil)
		return
	}

	if err := lifecycle.Transition(run.Status, newStatus); err != nil {
		h.writeError(w, http.StatusConflict, "invalid state transition", err)
		return
	}

	newRun := *run
	newRun.Status = newStatus
	newRun.Version = run.Version + 1
	newRun.UpdatedAt = time.Now()
	newRun.Metadata = body.Metadata
	if newStatus == types.RunCompletedMonitoring {
		if newRun.Metadata == nil {
			newRun.Metadata = make(map[string]interface{})
		}
		newRun.Metadata["monitoringStartedAt"] = time.Now().Format(time.RFC3339)
	}

	ok, err := h.provider.CompareAndSwapRunState(r.Context(), runID, run.Version, newRun)
	if err != nil || !ok {
		h.writeError(w, http.StatusConflict, "concurrent update conflict", err)
		return
	}

	if err := h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventCallbackReceived,
		PipelineID: run.PipelineID,
		RunID:      runID,
		Status:     string(newStatus),
		Message:    fmt.Sprintf("callback received: %s", body.Status),
		Details:    body.Metadata,
		Timestamp:  time.Now(),
	}); err != nil {
		h.logger.Error("failed to append event", "pipeline", run.PipelineID, "event", "CALLBACK_RECEIVED", "error", err)
	}

	if newStatus == types.RunCompletedMonitoring {
		if err := h.provider.AppendEvent(r.Context(), types.Event{
			Kind:       types.EventMonitoringStarted,
			PipelineID: run.PipelineID,
			RunID:      runID,
			Message:    "post-completion monitoring started via callback",
			Timestamp:  time.Now(),
		}); err != nil {
			h.logger.Error("failed to append monitoring event", "pipeline", run.PipelineID, "error", err)
		}
	}

	// Update RunLog to reflect completion
	today := time.Now().Format("2006-01-02")
	scheduleID := types.DefaultScheduleID
	if sid, ok := run.Metadata["scheduleId"].(string); ok && sid != "" {
		scheduleID = sid
	}
	if runLog, err := h.provider.GetRunLog(r.Context(), run.PipelineID, today, scheduleID); err == nil && runLog != nil && runLog.RunID == runID {
		now := time.Now()
		runLog.Status = newStatus
		if newStatus != types.RunCompletedMonitoring {
			runLog.CompletedAt = &now
		}
		runLog.UpdatedAt = now
		if newStatus == types.RunFailed {
			runLog.FailureMessage = "completed via callback with status: failed"
		}
		if err := h.provider.PutRunLog(r.Context(), *runLog); err != nil {
			h.logger.Error("failed to update run log", "pipeline", run.PipelineID, "runID", runID, "error", err)
		}
	}

	_ = json.NewEncoder(w).Encode(newRun)
}

// ListRunLogs returns recent run log entries for a pipeline.
func (h *Handlers) ListRunLogs(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	entries, err := h.provider.ListRunLogs(r.Context(), pipelineID, 20)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list run logs", err)
		return
	}
	if entries == nil {
		entries = []types.RunLogEntry{}
	}
	_ = json.NewEncoder(w).Encode(entries)
}

// GetRunLog returns a single run log entry for a pipeline and date.
func (h *Handlers) GetRunLog(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	date := chi.URLParam(r, "date")
	scheduleID := r.URL.Query().Get("schedule")
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	entry, err := h.provider.GetRunLog(r.Context(), pipelineID, date, scheduleID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get run log", err)
		return
	}
	if entry == nil {
		h.writeError(w, http.StatusNotFound, "run log not found", nil)
		return
	}
	_ = json.NewEncoder(w).Encode(entry)
}
