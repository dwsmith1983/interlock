// Package server implements the Interlock HTTP API server.
package server

import (
	"context"
	"expvar"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/provider"
)

// HTTP server defaults.
const (
	defaultMaxRequestBody = 1 << 20 // 1MB
	httpReadTimeout       = 30 * time.Second
	httpWriteTimeout      = 60 * time.Second
	httpIdleTimeout       = 120 * time.Second
)

// Server is the Interlock HTTP API server.
type Server struct {
	engine   *engine.Engine
	provider provider.Provider
	registry *archetype.Registry
	router   chi.Router
	addr     string
	srv      *http.Server
}

// New creates a new HTTP server.
func New(addr string, eng *engine.Engine, prov provider.Provider, reg *archetype.Registry, apiKey string, maxBody int64) *Server {
	s := &Server{
		engine:   eng,
		provider: prov,
		registry: reg,
		addr:     addr,
	}

	if maxBody <= 0 {
		maxBody = defaultMaxRequestBody
	}

	r := chi.NewRouter()
	r.Use(MaxBodyMiddleware(maxBody))
	r.Use(APIKeyMiddleware(apiKey))
	r.Use(RequestIDMiddleware)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.SetHeader("Content-Type", "application/json"))

	// Expose expvar metrics at /debug/vars
	r.Handle("/debug/vars", expvar.Handler())

	s.router = r
	s.registerRoutes(r)
	return s
}

// Start begins serving HTTP requests.
func (s *Server) Start() error {
	s.srv = &http.Server{
		Addr:         s.addr,
		Handler:      s.router,
		ReadTimeout:  httpReadTimeout,
		WriteTimeout: httpWriteTimeout,
		IdleTimeout:  httpIdleTimeout,
	}
	fmt.Printf("Interlock server listening on %s\n", s.addr)
	return s.srv.ListenAndServe()
}

// Stop gracefully shuts down the server.
func (s *Server) Stop(ctx context.Context) error {
	if s.srv != nil {
		return s.srv.Shutdown(ctx)
	}
	return nil
}
