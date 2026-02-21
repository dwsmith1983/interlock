package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/interlock-systems/interlock/pkg/types"
)

// ListEvents returns recent events for a pipeline.
func (h *Handlers) ListEvents(w http.ResponseWriter, r *http.Request) {
	pipelineID := chi.URLParam(r, "pipelineID")

	limit := 50
	if q := r.URL.Query().Get("limit"); q != "" {
		if n, err := strconv.Atoi(q); err == nil && n > 0 && n <= 500 {
			limit = n
		}
	}

	events, err := h.provider.ListEvents(r.Context(), pipelineID, limit)
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "failed to list events", err)
		return
	}
	if events == nil {
		events = []types.Event{}
	}
	_ = json.NewEncoder(w).Encode(events)
}
