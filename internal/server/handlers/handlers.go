// Package handlers implements HTTP request handlers for the Interlock API.
package handlers

import (
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/provider"
)

// Handlers contains all HTTP handler dependencies.
type Handlers struct {
	engine   *engine.Engine
	provider provider.Provider
}

// New creates a new Handlers instance.
func New(eng *engine.Engine, prov provider.Provider) *Handlers {
	return &Handlers{
		engine:   eng,
		provider: prov,
	}
}
