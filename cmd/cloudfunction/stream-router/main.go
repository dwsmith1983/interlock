// stream-router Cloud Function receives Firestore document change events via Eventarc
// and starts Cloud Workflows executions for MARKER documents.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	workflows "cloud.google.com/go/workflows/executions/apiv1"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	intgcpfunc "github.com/dwsmith1983/interlock/internal/gcpfunc"
)

// WorkflowsAPI is the subset of the Cloud Workflows Executions client we use.
type WorkflowsAPI interface {
	CreateExecution(ctx context.Context, req *executionspb.CreateExecutionRequest, opts ...interface{}) (*executionspb.Execution, error)
}

// workflowsClientWrapper adapts the real client to WorkflowsAPI.
type workflowsClientWrapper struct {
	client *workflows.Client
}

func (w *workflowsClientWrapper) CreateExecution(ctx context.Context, req *executionspb.CreateExecutionRequest, _ ...interface{}) (*executionspb.Execution, error) {
	return w.client.CreateExecution(ctx, req)
}

var (
	wfClient     WorkflowsAPI
	wfClientOnce sync.Once
	workflowName string // full resource name: projects/{p}/locations/{r}/workflows/{w}
)

func init() {
	functions.HTTP("StreamRouter", handleHTTP)
}

func getWorkflowsClient() (WorkflowsAPI, error) {
	var err error
	wfClientOnce.Do(func() {
		workflowName = os.Getenv("WORKFLOW_NAME")
		if workflowName == "" {
			err = fmt.Errorf("WORKFLOW_NAME environment variable required")
			return
		}
		var c *workflows.Client
		c, err = workflows.NewClient(context.Background())
		if err != nil {
			err = fmt.Errorf("creating Cloud Workflows client: %w", err)
			return
		}
		wfClient = &workflowsClientWrapper{client: c}
	})
	return wfClient, err
}

func handleHTTP(w http.ResponseWriter, r *http.Request) {
	var event intgcpfunc.StreamEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, fmt.Sprintf("invalid event: %v", err), http.StatusBadRequest)
		return
	}

	client, err := getWorkflowsClient()
	if err != nil {
		http.Error(w, fmt.Sprintf("init error: %v", err), http.StatusInternalServerError)
		return
	}

	if err := handleStreamEvent(r.Context(), client, workflowName, event); err != nil {
		http.Error(w, fmt.Sprintf("handler error: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "ok")
}

// handleStreamEvent processes Firestore document events and starts Cloud Workflows executions.
func handleStreamEvent(ctx context.Context, client WorkflowsAPI, wfName string, event intgcpfunc.StreamEvent) error {
	logger := slog.Default()

	// Only process events with a value (new/updated documents)
	if event.Value == nil {
		return nil
	}

	// Extract document ID from the full resource path
	// Format: projects/{p}/databases/{d}/documents/{collection}/{docID}
	docName := event.Value.Name
	parts := strings.Split(docName, "/")
	if len(parts) < 1 {
		return nil
	}
	docID := parts[len(parts)-1]

	// Document ID format: {PK}|{SK}
	idParts := strings.SplitN(docID, "|", 2)
	if len(idParts) != 2 {
		return nil
	}

	pk := idParts[0]
	sk := idParts[1]

	// Only process MARKER# records
	if !strings.HasPrefix(sk, "MARKER#") {
		return nil
	}

	// PK format: PIPELINE#<pipelineID>
	pipelineID := strings.TrimPrefix(pk, "PIPELINE#")
	if pipelineID == pk {
		logger.Warn("unexpected PK format", "pk", pk)
		return nil
	}

	// Extract scheduleID from document fields
	scheduleID := "daily"
	if event.Value.Fields != nil {
		if schedField, ok := event.Value.Fields["scheduleID"]; ok {
			// Firestore event fields use typed value wrappers
			if sv, ok := schedField.(map[string]interface{}); ok {
				if s, ok := sv["stringValue"].(string); ok && s != "" {
					scheduleID = s
				}
			} else if s, ok := schedField.(string); ok && s != "" {
				scheduleID = s
			}
		}
	}

	markerParts := strings.SplitN(sk, "#", 3)
	markerSource := ""
	if len(markerParts) >= 2 {
		markerSource = markerParts[1]
	}

	date := time.Now().UTC().Format("2006-01-02")

	wfInput, _ := json.Marshal(map[string]interface{}{
		"pipelineID":   pipelineID,
		"scheduleID":   scheduleID,
		"markerSource": markerSource,
		"date":         date,
	})

	_, err := client.CreateExecution(ctx, &executionspb.CreateExecutionRequest{
		Parent: wfName,
		Execution: &executionspb.Execution{
			Argument: string(wfInput),
		},
	})
	if err != nil {
		// AlreadyExists is expected for dedup — not an error
		if strings.Contains(err.Error(), "AlreadyExists") || strings.Contains(err.Error(), "ALREADY_EXISTS") {
			logger.Info("execution already exists (dedup)",
				"pipeline", pipelineID, "date", date, "schedule", scheduleID)
			return nil
		}
		logger.Error("failed to start workflow execution",
			"pipeline", pipelineID,
			"error", err,
		)
		return fmt.Errorf("starting execution for %s: %w", pipelineID, err)
	}

	logger.Info("started workflow execution",
		"pipeline", pipelineID,
		"date", date,
		"schedule", scheduleID,
	)

	return nil
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
