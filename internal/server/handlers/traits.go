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
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(traits); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// GetTrait returns a single trait evaluation for a pipeline.
func (h *Handlers) GetTrait(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")
	traitType := chi.URLParam(r, "traitType")

	trait, err := h.provider.GetTrait(r.Context(), pipelineID, traitType)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if trait == nil {
		http.Error(w, `{"error":"trait not found or expired"}`, http.StatusNotFound)
		return
	}
	if err := json.NewEncoder(w).Encode(trait); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}
