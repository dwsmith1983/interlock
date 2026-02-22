package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	intlambda "github.com/interlock-systems/interlock/internal/lambda"
	"github.com/interlock-systems/interlock/internal/trigger"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDeps(httpClient *http.Client) *intlambda.Deps {
	opts := []trigger.RunnerOption{}
	if httpClient != nil {
		opts = append(opts, trigger.WithHTTPClient(httpClient))
	}
	return &intlambda.Deps{
		Runner: trigger.NewRunner(opts...),
		Logger: slog.Default(),
	}
}

func TestHandleRunCheck_NonPollingType(t *testing.T) {
	d := testDeps(nil)

	req := intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerHTTP,
		Metadata:    map[string]interface{}{},
	}

	resp, err := handleRunCheck(context.Background(), d, req)
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
	assert.Equal(t, "non-polling trigger type", resp.Message)
}

func TestHandleRunCheck_CommandType(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerCommand,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_AirflowSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"dag_run_id": "run_1", "state": "success"}`)
	}))
	defer srv.Close()

	d := testDeps(srv.Client())

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerAirflow,
		Metadata: map[string]interface{}{
			"airflow_url":        srv.URL,
			"airflow_dag_id":     "my_dag",
			"airflow_dag_run_id": "run_1",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckSucceeded, resp.State)
}

func TestHandleRunCheck_AirflowMissingMetadata(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: types.TriggerAirflow,
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	// Missing metadata falls through to "running" state in the runner
	assert.Equal(t, trigger.RunCheckRunning, resp.State)
}

func TestHandleRunCheck_UnknownType(t *testing.T) {
	d := testDeps(nil)

	resp, err := handleRunCheck(context.Background(), d, intlambda.RunCheckRequest{
		PipelineID:  "test-pipe",
		RunID:       "run-1",
		TriggerType: "unknown",
		Metadata:    map[string]interface{}{},
	})
	require.NoError(t, err)
	assert.Equal(t, trigger.RunCheckFailed, resp.State)
}
