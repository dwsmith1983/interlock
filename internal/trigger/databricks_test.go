package trigger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteDatabricks_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/api/2.1/jobs/runs/submit")

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"run_id": 12345,
		})
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type:         types.TriggerDatabricks,
		WorkspaceURL: srv.URL,
		JobName:      "my-notebook",
	}

	meta, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
	require.NoError(t, err)
	assert.Equal(t, srv.URL, meta["databricks_workspace_url"])
	assert.Equal(t, "12345", meta["databricks_run_id"])
}

func TestExecuteDatabricks_WithAuth(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"run_id": 99,
		})
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type:         types.TriggerDatabricks,
		WorkspaceURL: srv.URL,
		JobName:      "my-job",
		Headers:      map[string]string{"Authorization": "Bearer db-token"},
	}

	_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
	require.NoError(t, err)
	assert.Equal(t, "Bearer db-token", receivedAuth)
}

func TestExecuteDatabricks_MissingWorkspaceURL(t *testing.T) {
	cfg := &types.TriggerConfig{Type: types.TriggerDatabricks, JobName: "job"}
	_, err := ExecuteDatabricks(context.Background(), cfg, http.DefaultClient)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workspaceUrl is required")
}

func TestExecuteDatabricks_MissingJobName(t *testing.T) {
	cfg := &types.TriggerConfig{Type: types.TriggerDatabricks, WorkspaceURL: "https://example.com"}
	_, err := ExecuteDatabricks(context.Background(), cfg, http.DefaultClient)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jobName is required")
}

func TestExecuteDatabricks_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type:         types.TriggerDatabricks,
		WorkspaceURL: srv.URL,
		JobName:      "job",
	}

	_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestCheckDatabricksStatus(t *testing.T) {
	tests := []struct {
		name           string
		lifeCycleState string
		resultState    string
		expected       RunCheckState
	}{
		{"terminated_success", "TERMINATED", "SUCCESS", RunCheckSucceeded},
		{"terminated_failure", "TERMINATED", "FAILED", RunCheckFailed},
		{"terminated_timedout", "TERMINATED", "TIMEDOUT", RunCheckFailed},
		{"terminated_cancelled", "TERMINATED", "CANCELED", RunCheckFailed},
		{"internal_error", "INTERNAL_ERROR", "", RunCheckFailed},
		{"skipped", "SKIPPED", "", RunCheckFailed},
		{"running", "RUNNING", "", RunCheckRunning},
		{"pending", "PENDING", "", RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := map[string]interface{}{
				"life_cycle_state": tt.lifeCycleState,
			}
			if tt.resultState != "" {
				state["result_state"] = tt.resultState
			}

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "GET", r.Method)
				assert.Contains(t, r.URL.Path, "/api/2.1/jobs/runs/get")
				assert.Equal(t, "999", r.URL.Query().Get("run_id"))

				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"state": state,
				})
			}))
			defer srv.Close()

			r := NewRunner(WithHTTPClient(srv.Client()))
			result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
				"databricks_workspace_url": srv.URL,
				"databricks_run_id":        "999",
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
		})
	}
}

func TestCheckDatabricksStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
