// Package handlers implements HTTP request handlers for the Interlock API.
package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/provider"
)

// Handlers contains all HTTP handler dependencies.
type Handlers struct {
	engine   *engine.Engine
	provider provider.Provider
	logger   *slog.Logger
}

// New creates a new Handlers instance.
func New(eng *engine.Engine, prov provider.Provider) *Handlers {
	return &Handlers{
		engine:   eng,
		provider: prov,
		logger:   slog.Default(),
	}
}

// SetLogger overrides the default logger.
func (h *Handlers) SetLogger(l *slog.Logger) {
	if l != nil {
		h.logger = l
	}
}

// writeError logs the internal error and returns a sanitized JSON error to the client.
func (h *Handlers) writeError(w http.ResponseWriter, status int, msg string, err error) {
	if err != nil {
		h.logger.Error(msg, "error", err, "status", status)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
