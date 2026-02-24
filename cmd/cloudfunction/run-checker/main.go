// run-checker Cloud Function polls external systems for job status.
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
	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
	"github.com/dwsmith1983/interlock/internal/trigger"
)

var (
	deps     *intgcpfunc.Deps
	depsOnce sync.Once
	depsErr  error
)

func init() {
	functions.HTTP("RunChecker", handleHTTP)
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

	var req intgcpfunc.RunCheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request: %v", err), http.StatusBadRequest)
		return
	}

	resp, err := handleRunCheck(r.Context(), d, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("handler error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleRunCheck implements the core run-checker logic.
func handleRunCheck(ctx context.Context, d *intgcpfunc.Deps, req intgcpfunc.RunCheckRequest) (intgcpfunc.RunCheckResponse, error) {
	result, err := d.Runner.CheckStatus(ctx, req.TriggerType, req.Metadata, req.Headers)
	if err != nil {
		d.Logger.Error("status check failed",
			"pipeline", req.PipelineID,
			"runID", req.RunID,
			"triggerType", req.TriggerType,
			"error", err,
		)
		return intgcpfunc.RunCheckResponse{
			State:   trigger.RunCheckFailed,
			Message: err.Error(),
		}, nil
	}

	return intgcpfunc.RunCheckResponse{
		State:   result.State,
		Message: result.Message,
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
