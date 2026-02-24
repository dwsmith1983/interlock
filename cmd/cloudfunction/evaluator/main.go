// evaluator Cloud Function evaluates a single trait for a pipeline.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/dwsmith1983/interlock/internal/archetype"
	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
)

var (
	deps     *intgcpfunc.Deps
	depsOnce sync.Once
	depsErr  error
)

func init() {
	functions.HTTP("Evaluator", handleHTTP)
}

func getDeps() (*intgcpfunc.Deps, error) {
	depsOnce.Do(func() {
		deps, depsErr = intgcpfunc.Init(context.Background())
	})
	return deps, depsErr
}

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	d, err := getDeps()
	if err != nil {
		http.Error(w, fmt.Sprintf("init error: %v", err), http.StatusInternalServerError)
		return
	}

	var req intgcpfunc.EvaluatorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	resp, err := handleEvaluate(r.Context(), d, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("handler error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleEvaluate evaluates a single trait and returns the result.
func handleEvaluate(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.EvaluatorRequest) (intgcpfunc.EvaluatorResponse, error) {
	trait := archetype.ResolvedTrait{
		Type:      req.TraitType,
		Evaluator: req.Evaluator,
		Config:    req.Config,
		Timeout:   req.Timeout,
		TTL:       req.TTL,
		Required:  req.Required,
	}

	result, err := d.Engine.EvaluateTrait(ctx, req.PipelineID, trait)
	if err != nil {
		d.Logger.Error("trait evaluation failed",
			"pipeline", req.PipelineID,
			"trait", req.TraitType,
			"error", err,
		)
		return intgcpfunc.EvaluatorResponse{
			TraitType: req.TraitType,
			Status:    "FAIL",
			Reason:    err.Error(),
			Required:  req.Required,
		}, nil
	}

	return intgcpfunc.EvaluatorResponse{
		TraitType: req.TraitType,
		Status:    result.Status,
		Value:     result.Value,
		Reason:    result.Reason,
		Required:  req.Required,
	}, nil
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	if err := funcframework.Start(port); err != nil {
		slog.Error("failed to start functions framework", "error", err)
		os.Exit(1)
	}
}
