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
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	if result.Status != types.Ready {
		w.WriteHeader(http.StatusPreconditionFailed)
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "NOT_READY",
			"blocking": result.Blocking,
		}); err != nil {
			http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		}
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
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	_ = h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: pipelineID,
		RunID:      runID,
		Status:     string(types.RunPending),
		Message:    "run created",
		Timestamp:  time.Now(),
	})

	// CAS to TRIGGERING
	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := h.provider.CompareAndSwapRunState(r.Context(), runID, 1, run)
	if err != nil || !ok {
		http.Error(w, `{"error":"failed to acquire trigger lock"}`, http.StatusConflict)
		return
	}

	_ = h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: pipelineID,
		RunID:      runID,
		Status:     string(types.RunTriggering),
		Message:    "trigger lock acquired",
		Timestamp:  time.Now(),
	})

	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(run); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// ListRuns returns recent runs for a pipeline.
func (h *Handlers) ListRuns(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	runs, err := h.provider.ListRuns(r.Context(), pipelineID, 20)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if runs == nil {
		runs = []types.RunState{}
	}
	if err := json.NewEncoder(w).Encode(runs); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// GetRun returns a single run state.
func (h *Handlers) GetRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")
	run, err := h.provider.GetRunState(r.Context(), runID)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusNotFound)
		return
	}
	if err := json.NewEncoder(w).Encode(run); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// CompleteRun handles orchestrator completion callbacks.
func (h *Handlers) CompleteRun(w http.ResponseWriter, r *http.Request) {
	runID := chi.URLParam(r, "runID")

	var body struct {
		Status   string                 `json:"status"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}

	run, err := h.provider.GetRunState(r.Context(), runID)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusNotFound)
		return
	}

	var newStatus types.RunStatus
	switch body.Status {
	case "success":
		newStatus = types.RunCompleted
	case "failed":
		newStatus = types.RunFailed
	default:
		http.Error(w, `{"error":"status must be 'success' or 'failed'"}`, http.StatusBadRequest)
		return
	}

	if err := lifecycle.Transition(run.Status, newStatus); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusConflict)
		return
	}

	newRun := *run
	newRun.Status = newStatus
	newRun.Version = run.Version + 1
	newRun.UpdatedAt = time.Now()
	newRun.Metadata = body.Metadata

	ok, err := h.provider.CompareAndSwapRunState(r.Context(), runID, run.Version, newRun)
	if err != nil || !ok {
		http.Error(w, `{"error":"concurrent update conflict"}`, http.StatusConflict)
		return
	}

	_ = h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventCallbackReceived,
		PipelineID: run.PipelineID,
		RunID:      runID,
		Status:     string(newStatus),
		Message:    fmt.Sprintf("callback received: %s", body.Status),
		Details:    body.Metadata,
		Timestamp:  time.Now(),
	})

	if err := json.NewEncoder(w).Encode(newRun); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}
