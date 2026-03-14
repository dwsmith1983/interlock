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

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
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

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
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

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
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

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

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

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

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

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", nil)
	require.NoError(t, err)
	assert.Equal(t, "failed", state)
}

func TestExecuteAirflow_MissingURL(t *testing.T) {
	cfg := &types.AirflowTriggerConfig{DagID: "my_dag"}
	_, err := ExecuteAirflow(context.Background(), cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "url is required")
}

func TestExecuteAirflow_MissingDagID(t *testing.T) {
	cfg := &types.AirflowTriggerConfig{URL: "http://example.com"}
	_, err := ExecuteAirflow(context.Background(), cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dagID is required")
}

func TestExecuteAirflow_WithBody(t *testing.T) {
	var receivedConf interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		receivedConf = payload["conf"]
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "run-with-body",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
		URL:   srv.URL,
		DagID: "my_dag",
		Body:  `{"key": "value"}`,
	}

	meta, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "run-with-body", meta["airflow_dag_run_id"])
	assert.NotNil(t, receivedConf)
}

func TestExecuteAirflow_InvalidBodyJSON(t *testing.T) {
	cfg := &types.AirflowTriggerConfig{
		URL:   "http://example.com",
		DagID: "my_dag",
		Body:  `{invalid json`,
	}
	_, err := ExecuteAirflow(context.Background(), cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid body JSON")
}

func TestExecuteAirflow_MissingDagRunIDInResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "queued",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
		URL:   srv.URL,
		DagID: "my_dag",
	}
	_, err := ExecuteAirflow(context.Background(), cfg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "response missing dag_run_id")
}

func TestExecuteAirflow_CustomTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "run-timeout",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
		URL:     srv.URL,
		DagID:   "my_dag",
		Timeout: 60, // Different from defaultTriggerTimeout (30s)
	}
	meta, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)
	assert.Equal(t, "run-timeout", meta["airflow_dag_run_id"])
}

func TestCheckAirflowStatus_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	_, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestCheckAirflowStatus_MissingStateField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "run-no-state",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	_, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "response missing state field")
}

func TestExecuteAirflow_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TEST_VAR", "safe")
	t.Setenv("SECRET_VAR", "leaked")

	var receivedAuth string
	var receivedBody map[string]interface{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		var payload map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&payload)
		receivedBody = payload
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"dag_run_id": "run-env-test",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.AirflowTriggerConfig{
		URL:     srv.URL,
		DagID:   "test_dag",
		Headers: map[string]string{"Authorization": "Bearer ${INTERLOCK_TEST_VAR}/${SECRET_VAR}"},
		Body:    `{"safe":"${INTERLOCK_TEST_VAR}","secret":"${SECRET_VAR}"}`,
	}

	_, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)

	// INTERLOCK_ prefixed vars should resolve; others should not.
	assert.Equal(t, "Bearer safe/", receivedAuth)
	conf, _ := receivedBody["conf"].(map[string]interface{})
	assert.Equal(t, "safe", conf["safe"])
	assert.Equal(t, "", conf["secret"])
	assert.NotContains(t, receivedAuth, "leaked")
}

func TestCheckAirflowStatus_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TEST_VAR", "safe")
	t.Setenv("SECRET_VAR", "leaked")

	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "running",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	headers := map[string]string{"Authorization": "Bearer ${INTERLOCK_TEST_VAR}/${SECRET_VAR}"}
	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", headers)
	require.NoError(t, err)
	assert.Equal(t, "running", state)
	assert.Equal(t, "Bearer safe/", receivedAuth)
	assert.NotContains(t, receivedAuth, "leaked")
}

func TestCheckAirflowStatus_WithHeaders(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": "running",
		})
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", map[string]string{
		"Authorization": "Bearer test-token",
	})
	require.NoError(t, err)
	assert.Equal(t, "running", state)
	assert.Equal(t, "Bearer test-token", receivedAuth)
}
