package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// RequestRerun creates a rerun record and triggers the pipeline if ready.
func (h *Handlers) RequestRerun(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")

	// Validate pipeline exists
	if _, err := h.provider.GetPipeline(r.Context(), pipelineID); err != nil {
		h.writeError(w, http.StatusNotFound, "pipeline not found", err)
		return
	}

	var body struct {
		OriginalDate string                 `json:"originalDate"`
		Reason       string                 `json:"reason"`
		Description  string                 `json:"description,omitempty"`
		Metadata     map[string]interface{} `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON", err)
		return
	}
	if body.OriginalDate == "" {
		h.writeError(w, http.StatusBadRequest, "originalDate is required", nil)
		return
	}
	if body.Reason == "" {
		h.writeError(w, http.StatusBadRequest, "reason is required", nil)
		return
	}

	// Look up original run log for reference (optional — may not exist)
	scheduleID := types.DefaultScheduleID
	if sid, _ := body.Metadata["scheduleId"].(string); sid != "" {
		scheduleID = sid
	}
	var originalRunID string
	if runLog, err := h.provider.GetRunLog(r.Context(), pipelineID, body.OriginalDate, scheduleID); err == nil && runLog != nil {
		originalRunID = runLog.RunID
	}

	now := time.Now()
	rerunID := fmt.Sprintf("rerun-%s-%d", pipelineID, now.UnixNano())

	record := types.RerunRecord{
		RerunID:       rerunID,
		PipelineID:    pipelineID,
		OriginalDate:  body.OriginalDate,
		OriginalRunID: originalRunID,
		Reason:        body.Reason,
		Description:   body.Description,
		Status:        types.RunPending,
		RequestedAt:   now,
		Metadata:      body.Metadata,
	}

	// Evaluate readiness
	result, err := h.engine.Evaluate(r.Context(), pipelineID)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "evaluation failed", err)
		return
	}

	if result.Status != types.Ready {
		// Store the record as PENDING so it's visible in the ledger
		if err := h.provider.PutRerun(r.Context(), record); err != nil {
			h.writeError(w, http.StatusInternalServerError, "failed to store rerun record", err)
			return
		}
		w.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"rerun":    record,
			"status":   "NOT_READY",
			"blocking": result.Blocking,
		})
		return
	}

	// Create run
	runID := fmt.Sprintf("%s-rerun-%d", pipelineID, now.UnixNano())
	run := types.RunState{
		RunID:      runID,
		PipelineID: pipelineID,
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
		Metadata: map[string]interface{}{
			"rerunId":      rerunID,
			"originalDate": body.OriginalDate,
			"reason":       body.Reason,
		},
	}

	if err := h.provider.PutRunState(r.Context(), run); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to create run", err)
		return
	}

	// CAS to TRIGGERING
	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, casErr := h.provider.CompareAndSwapRunState(r.Context(), runID, 1, run)
	if casErr != nil || !ok {
		h.writeError(w, http.StatusConflict, "failed to acquire trigger lock", casErr)
		return
	}

	// Update rerun record
	record.RerunRunID = runID
	record.Status = types.RunTriggering
	if err := h.provider.PutRerun(r.Context(), record); err != nil {
		h.logger.Error("failed to store rerun record", "rerunId", rerunID, "error", err)
	}

	// Log the rerun request event
	if err := h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventRerunRequested,
		PipelineID: pipelineID,
		RunID:      runID,
		Message:    fmt.Sprintf("rerun requested for %s: %s", body.OriginalDate, body.Reason),
		Details: map[string]interface{}{
			"rerunId":       rerunID,
			"originalDate":  body.OriginalDate,
			"originalRunId": originalRunID,
			"reason":        body.Reason,
		},
		Timestamp: now,
	}); err != nil {
		h.logger.Error("failed to append event", "pipeline", pipelineID, "event", "RERUN_REQUESTED", "error", err)
	}

	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(record)
}

// CompleteRerun updates a rerun record when the associated run completes.
func (h *Handlers) CompleteRerun(w http.ResponseWriter, r *http.Request) {
	rerunID := chi.URLParam(r, "rerunID")

	record, err := h.provider.GetRerun(r.Context(), rerunID)
	if err != nil || record == nil {
		h.writeError(w, http.StatusNotFound, "rerun not found", err)
		return
	}

	var body struct {
		Status   string                 `json:"status"`
		Metadata map[string]interface{} `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON", err)
		return
	}

	var newStatus types.RunStatus
	switch body.Status {
	case "success":
		newStatus = types.RunCompleted
	case "failed":
		newStatus = types.RunFailed
	default:
		h.writeError(w, http.StatusBadRequest, "status must be 'success' or 'failed'", nil)
		return
	}

	if err := lifecycle.Transition(record.Status, newStatus); err != nil {
		h.writeError(w, http.StatusConflict, "invalid state transition", err)
		return
	}

	now := time.Now()
	record.Status = newStatus
	record.CompletedAt = &now
	if body.Metadata != nil {
		if record.Metadata == nil {
			record.Metadata = make(map[string]interface{})
		}
		for k, v := range body.Metadata {
			record.Metadata[k] = v
		}
	}

	if err := h.provider.PutRerun(r.Context(), *record); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to update rerun record", err)
		return
	}

	if err := h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventRerunCompleted,
		PipelineID: record.PipelineID,
		RunID:      record.RerunRunID,
		Status:     string(newStatus),
		Message:    fmt.Sprintf("rerun %s completed: %s", rerunID, body.Status),
		Details: map[string]interface{}{
			"rerunId":      rerunID,
			"originalDate": record.OriginalDate,
		},
		Timestamp: now,
	}); err != nil {
		h.logger.Error("failed to append event", "rerunId", rerunID, "event", "RERUN_COMPLETED", "error", err)
	}

	_ = json.NewEncoder(w).Encode(record)
}

// ListReruns returns rerun records for a pipeline.
func (h *Handlers) ListReruns(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")

	limit := 20
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	records, err := h.provider.ListReruns(r.Context(), pipelineID, limit)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list reruns", err)
		return
	}
	if records == nil {
		records = []types.RerunRecord{}
	}
	_ = json.NewEncoder(w).Encode(records)
}

// ListAllReruns returns recent rerun records across all pipelines.
func (h *Handlers) ListAllReruns(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}

	records, err := h.provider.ListAllReruns(r.Context(), limit)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list reruns", err)
		return
	}
	if records == nil {
		records = []types.RerunRecord{}
	}
	_ = json.NewEncoder(w).Encode(records)
}
