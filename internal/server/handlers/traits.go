package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
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
