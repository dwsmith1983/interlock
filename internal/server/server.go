// Package server implements the Interlock HTTP API server.
package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/provider"
)

// Server is the Interlock HTTP API server.
type Server struct {
	engine   *engine.Engine
	provider provider.Provider
	router   chi.Router
	addr     string
	srv      *http.Server
}

// New creates a new HTTP server.
func New(addr string, eng *engine.Engine, prov provider.Provider) *Server {
	s := &Server{
		engine:   eng,
		provider: prov,
		addr:     addr,
	}

	r := chi.NewRouter()
	r.Use(RequestIDMiddleware)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.SetHeader("Content-Type", "application/json"))

	s.router = r
	s.registerRoutes(r)
	return s
}

// Start begins serving HTTP requests.
func (s *Server) Start() error {
	s.srv = &http.Server{
		Addr:         s.addr,
		Handler:      s.router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
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
