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

func TestRunCheckState_Constants(t *testing.T) {
	assert.Equal(t, RunCheckState("running"), RunCheckRunning)
	assert.Equal(t, RunCheckState("succeeded"), RunCheckSucceeded)
	assert.Equal(t, RunCheckState("failed"), RunCheckFailed)
}

func TestStatusResult_ConstructAndCompare(t *testing.T) {
	result := StatusResult{
		State:   RunCheckSucceeded,
		Message: "job completed",
	}
	assert.Equal(t, RunCheckSucceeded, result.State)
	assert.Equal(t, "job completed", result.Message)
	assert.Empty(t, result.FailureCategory)
}

// TestCheckAirflowStatus_SendsExpandedHeaders: the runner's airflow poll goes
// through the runner's client and sends the configured headers with their
// INTERLOCK_ variables expanded.
func TestCheckAirflowStatus_SendsExpandedHeaders(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-123")
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"state": "success"})
	}))
	t.Cleanup(srv.Close)

	r := NewRunner(WithHTTPClient(srv.Client()))
	res, err := r.CheckStatus(context.Background(), types.TriggerAirflow, map[string]interface{}{
		"airflow_url": srv.URL, "airflow_dag_id": "etl", "airflow_dag_run_id": "r1",
	}, map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"})

	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, res.State)
	assert.Equal(t, "Bearer tok-123", gotAuth)
}

// TestCheckAirflowStatus_MetadataAPIVersion: the runner polls the API that
// airflow_api_version names, as an int or as the float64 a JSON round trip
// gives back. A run with no version was started on the v1 API.
func TestCheckAirflowStatus_MetadataAPIVersion(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"state": "running"})
	}))
	t.Cleanup(srv.Close)
	r := NewRunner(WithHTTPClient(srv.Client()))
	metadata := func(version interface{}, set bool) map[string]interface{} {
		m := map[string]interface{}{"airflow_url": srv.URL, "airflow_dag_id": "etl", "airflow_dag_run_id": "r1"}
		if set {
			m["airflow_api_version"] = version
		}
		return m
	}

	for _, tt := range []struct {
		name     string
		version  interface{}
		set      bool
		wantPath string
	}{
		{"missing", nil, false, "/api/v1/dags/etl/dagRuns/r1"},
		{"int 2", 2, true, "/api/v1/dags/etl/dagRuns/r1"},
		{"float 2", float64(2), true, "/api/v1/dags/etl/dagRuns/r1"},
		{"int 3", 3, true, "/api/v2/dags/etl/dagRuns/r1"},
		{"float 3", float64(3), true, "/api/v2/dags/etl/dagRuns/r1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path = ""
			res, err := r.CheckStatus(context.Background(), types.TriggerAirflow, metadata(tt.version, tt.set), nil)
			require.NoError(t, err)
			assert.Equal(t, RunCheckRunning, res.State)
			assert.Equal(t, tt.wantPath, path)
		})
	}

	for version, wantErr := range map[interface{}]string{
		1:            "airflow status: unsupported apiVersion 1",
		4:            "airflow status: unsupported apiVersion 4",
		float64(4):   "airflow status: unsupported apiVersion 4",
		float64(2.5): "airflow status: airflow_api_version 2.5 is not a whole number",
		"3":          `airflow status: airflow_api_version "3" is not a whole number`,
		nil:          "airflow status: airflow_api_version <nil> is not a whole number",
	} {
		path = ""
		_, err := r.CheckStatus(context.Background(), types.TriggerAirflow, metadata(version, true), nil)
		require.EqualError(t, err, wantErr, "version %#v", version)
		assert.Empty(t, path, "version %#v sent a request", version)
	}
}
