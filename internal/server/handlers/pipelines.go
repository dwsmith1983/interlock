package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/interlock-systems/interlock/pkg/types"
)

// ListPipelines returns all registered pipelines.
func (h *Handlers) ListPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := h.provider.ListPipelines(r.Context())
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if pipelines == nil {
		pipelines = []types.PipelineConfig{}
	}
	if err := json.NewEncoder(w).Encode(pipelines); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// RegisterPipeline creates or updates a pipeline registration.
func (h *Handlers) RegisterPipeline(w http.ResponseWriter, r *http.Request) {
	var config types.PipelineConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, `{"error":"invalid JSON: `+err.Error()+`"}`, http.StatusBadRequest)
		return
	}
	if config.Name == "" {
		http.Error(w, `{"error":"name is required"}`, http.StatusBadRequest)
		return
	}

	if err := h.provider.RegisterPipeline(r.Context(), config); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(config); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// GetPipeline returns a single pipeline configuration.
func (h *Handlers) GetPipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	pipeline, err := h.provider.GetPipeline(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusNotFound)
		return
	}
	if err := json.NewEncoder(w).Encode(pipeline); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// DeletePipeline removes a pipeline registration.
func (h *Handlers) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	if err := h.provider.DeletePipeline(r.Context(), id); err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// EvaluatePipeline triggers evaluation of all traits for a pipeline.
func (h *Handlers) EvaluatePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	result, err := h.engine.Evaluate(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}

// GetReadiness returns the cached readiness status for a pipeline.
func (h *Handlers) GetReadiness(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	result, err := h.engine.CheckReadiness(r.Context(), id)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}
