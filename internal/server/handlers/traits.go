package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/interlock-systems/interlock/pkg/types"
)

// GetTraits returns all trait evaluations for a pipeline.
func (h *Handlers) GetTraits(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	traits, err := h.provider.GetTraits(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get traits", err)
		return
	}
	if err := json.NewEncoder(w).Encode(traits); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to encode response", err)
		return
	}
}

// GetTrait returns a single trait evaluation for a pipeline.
func (h *Handlers) GetTrait(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	traitType := chi.URLParam(r, "traitType")

	trait, err := h.provider.GetTrait(r.Context(), pipelineID, traitType)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to get trait", err)
		return
	}
	if trait == nil {
		h.writeError(w, http.StatusNotFound, "trait not found or expired", nil)
		return
	}
	if err := json.NewEncoder(w).Encode(trait); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to encode response", err)
		return
	}
}

// PushTrait accepts an externally-produced trait evaluation.
func (h *Handlers) PushTrait(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	traitType := chi.URLParam(r, "traitType")

	// Validate pipeline exists
	if _, err := h.provider.GetPipeline(r.Context(), pipelineID); err != nil {
		h.writeError(w, http.StatusNotFound, "pipeline not found", err)
		return
	}

	var body struct {
		Status types.TraitStatus       `json:"status"`
		Value  map[string]interface{}  `json:"value,omitempty"`
		Reason string                  `json:"reason,omitempty"`
		TTL    int                     `json:"ttl,omitempty"` // seconds, 0 = no expiry
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON", err)
		return
	}

	if body.Status != types.TraitPass && body.Status != types.TraitFail {
		h.writeError(w, http.StatusBadRequest, "status must be PASS or FAIL", nil)
		return
	}

	now := time.Now()
	te := types.TraitEvaluation{
		PipelineID:  pipelineID,
		TraitType:   traitType,
		Status:      body.Status,
		Value:       body.Value,
		Reason:      body.Reason,
		EvaluatedAt: now,
	}

	ttl := time.Duration(body.TTL) * time.Second
	if ttl > 0 {
		exp := now.Add(ttl)
		te.ExpiresAt = &exp
	}

	if err := h.provider.PutTrait(r.Context(), pipelineID, te, ttl); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to store trait", err)
		return
	}

	if err := h.provider.AppendEvent(r.Context(), types.Event{
		Kind:       types.EventTraitPushed,
		PipelineID: pipelineID,
		TraitType:  traitType,
		Status:     string(body.Status),
		Message:    body.Reason,
		Details:    body.Value,
		Timestamp:  now,
	}); err != nil {
		h.logger.Error("failed to append event", "pipeline", pipelineID, "trait", traitType, "event", "TRAIT_PUSHED", "error", err)
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(te)
}
