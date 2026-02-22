package trigger

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteAirflow_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.URL.Path, "/api/v1/dags/my_dag/dagRuns")

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "manual__2025-01-01T00:00:00+00:00",
			"dag_id":     "my_dag",
			"state":      "queued",
		})
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type:  types.TriggerAirflow,
		URL:   srv.URL,
		DagID: "my_dag",
	}

	meta, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "manual__2025-01-01T00:00:00+00:00", meta["airflow_dag_run_id"])
	assert.Equal(t, "my_dag", meta["airflow_dag_id"])
	assert.Equal(t, srv.URL, meta["airflow_url"])
}

func TestExecuteAirflow_AuthHeader(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "run-123",
		})
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type:    types.TriggerAirflow,
		URL:     srv.URL,
		DagID:   "test_dag",
		Headers: map[string]string{"Authorization": "Bearer my-token"},
	}

	_, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "Bearer my-token", receivedAuth)
}

func TestExecuteAirflow_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	cfg := &types.TriggerConfig{
		Type:  types.TriggerAirflow,
		URL:   srv.URL,
		DagID: "my_dag",
	}

	meta, err := ExecuteAirflow(context.Background(), cfg)
	assert.Nil(t, meta)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestCheckAirflowStatus_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Contains(t, r.URL.Path, "/api/v1/dags/my_dag/dagRuns/run-123")

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "run-123",
			"state":      "success",
		})
	}))
	defer srv.Close()

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", nil)
	require.NoError(t, err)
	assert.Equal(t, "success", state)
}

func TestCheckAirflowStatus_Running(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "running",
		})
	}))
	defer srv.Close()

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", nil)
	require.NoError(t, err)
	assert.Equal(t, "running", state)
}

func TestCheckAirflowStatus_Failed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "failed",
		})
	}))
	defer srv.Close()

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", nil)
	require.NoError(t, err)
	assert.Equal(t, "failed", state)
}
