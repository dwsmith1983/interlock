package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/interlock-systems/interlock/pkg/types"
)

// ListPipelines returns all registered pipelines.
func (h *Handlers) ListPipelines(w http.ResponseWriter, r *http.Request) {
	pipelines, err := h.provider.ListPipelines(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list pipelines", err)
		return
	}
	if pipelines == nil {
		pipelines = []types.PipelineConfig{}
	}
	_ = json.NewEncoder(w).Encode(pipelines)
}

// RegisterPipeline creates or updates a pipeline registration.
func (h *Handlers) RegisterPipeline(w http.ResponseWriter, r *http.Request) {
	var config types.PipelineConfig
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid JSON", err)
		return
	}
	if config.Name == "" {
		h.writeError(w, http.StatusBadRequest, "name is required", nil)
		return
	}

	if config.Trigger != nil && config.Trigger.Type == types.TriggerCommand {
		h.writeError(w, http.StatusBadRequest, "command triggers cannot be registered via API", nil)
		return
	}

	if config.Archetype != "" && h.registry != nil {
		if _, err := h.registry.Get(config.Archetype); err != nil {
			h.writeError(w, http.StatusBadRequest, fmt.Sprintf("unknown archetype %q", config.Archetype), nil)
			return
		}
	}

	if err := h.provider.RegisterPipeline(r.Context(), config); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to register pipeline", err)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(config)
}

// GetPipeline returns a single pipeline configuration.
func (h *Handlers) GetPipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	pipeline, err := h.provider.GetPipeline(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusNotFound, "pipeline not found", err)
		return
	}
	_ = json.NewEncoder(w).Encode(pipeline)
}

// DeletePipeline removes a pipeline registration.
func (h *Handlers) DeletePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	if err := h.provider.DeletePipeline(r.Context(), id); err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to delete pipeline", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// EvaluatePipeline triggers evaluation of all traits for a pipeline.
func (h *Handlers) EvaluatePipeline(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	result, err := h.engine.Evaluate(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "evaluation failed", err)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}

// GetReadiness returns the cached readiness status for a pipeline.
func (h *Handlers) GetReadiness(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "pipelineID")
	result, err := h.engine.CheckReadiness(r.Context(), id)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "readiness check failed", err)
		return
	}
	_ = json.NewEncoder(w).Encode(result)
}
