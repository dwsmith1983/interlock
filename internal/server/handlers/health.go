package handlers

import (
	"encoding/json"
	"net/http"
)

// Health returns the server health status.
func (h *Handlers) Health(w http.ResponseWriter, r *http.Request) {
	status := "ok"
	if err := h.provider.Ping(r.Context()); err != nil {
		status = "degraded"
	}

	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": status,
	}); err != nil {
		http.Error(w, `{"error":"failed to encode response"}`, http.StatusInternalServerError)
		return
	}
}
