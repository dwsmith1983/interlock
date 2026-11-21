package trigger

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/dwsmith1983/interlock/internal/lambda"
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

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", 2, nil, defaultHTTPClient)
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

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", 2, nil, defaultHTTPClient)
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

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-123", 2, nil, defaultHTTPClient)
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
	// The decoder would quote the 'z' that follows the injected quote.
	t.Setenv("INTERLOCK_X", `"zq9`)
	cfg := &types.AirflowTriggerConfig{
		URL:   "http://example.com",
		DagID: "my_dag",
		Body:  `{"t":"${INTERLOCK_X}"}`,
	}
	_, err := ExecuteAirflow(context.Background(), cfg)
	require.EqualError(t, err, "airflow trigger: body is not valid JSON after variable expansion")
	assert.NotContains(t, err.Error(), "z")
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

	_, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", 2, nil, defaultHTTPClient)
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

	_, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", 2, nil, defaultHTTPClient)
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
	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", 2, headers, defaultHTTPClient)
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

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "my_dag", "run-1", 2, map[string]string{
		"Authorization": "Bearer test-token",
	}, defaultHTTPClient)
	require.NoError(t, err)
	assert.Equal(t, "running", state)
	assert.Equal(t, "Bearer test-token", receivedAuth)
}

// postedConf runs ExecuteAirflow with cfg and returns the request body's conf
// (nil when absent) and the request headers.
func postedConf(t *testing.T, cfg *types.AirflowTriggerConfig) (interface{}, http.Header) {
	t.Helper()
	var payload map[string]interface{}
	var headers http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"dag_run_id": "run-1"})
	}))
	t.Cleanup(srv.Close)
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	t.Cleanup(func() { defaultHTTPClient = origClient })

	cfg.URL, cfg.DagID = srv.URL, "d"
	_, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)
	return payload["conf"], headers
}

// The DAG run's conf carries the run: a generated body as is, a configured
// one with the identity variables expanded (JSON-escaped, ahead of env). The
// headers keep env-only expansion, since the status poll re-sends them
// without a run identity.
func TestExecuteAirflow_RunIdentity(t *testing.T) {
	t.Setenv("INTERLOCK_DATE", "from-env")
	t.Setenv("INTERLOCK_X", "expanded")
	run := types.RunIdentity{PipelineID: "orders", ScheduleID: "stream", Date: "2026-03-01", RunID: `exec"4\2`}

	conf, headers := postedConf(t, &types.AirflowTriggerConfig{
		Body:    `{"p":"${INTERLOCK_PIPELINE_ID}","s":"${INTERLOCK_SCHEDULE_ID}","d":"${INTERLOCK_DATE}","r":"${INTERLOCK_RUN_ID}"}`,
		Headers: map[string]string{"X-Date": "${INTERLOCK_DATE}"},
		Run:     run,
	})
	assert.Equal(t, map[string]interface{}{"p": "orders", "s": "stream", "d": "2026-03-01", "r": `exec"4\2`}, conf)
	assert.Equal(t, "from-env", headers.Get("X-Date"))

	conf, _ = postedConf(t, &types.AirflowTriggerConfig{
		Body: `{"pipelineId":"a${INTERLOCK_X}","par_day":"20260301"}`, BodyGenerated: true, Run: run,
	})
	assert.Equal(t, map[string]interface{}{"pipelineId": "a${INTERLOCK_X}", "par_day": "20260301"}, conf)

	conf, _ = postedConf(t, &types.AirflowTriggerConfig{})
	assert.Nil(t, conf, "a direct caller with no body sends no conf")
}

// airflowPost runs ExecuteAirflow with cfg against a server that records the
// request path and raw body, and returns them with the trigger metadata.
func airflowPost(t *testing.T, cfg *types.AirflowTriggerConfig) (path, body string, meta map[string]interface{}) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		raw, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		body = string(raw)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"dag_run_id": "r1"})
	}))
	t.Cleanup(srv.Close)
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	t.Cleanup(func() { defaultHTTPClient = origClient })

	cfg.URL, cfg.DagID = srv.URL, "etl"
	meta, err := ExecuteAirflow(context.Background(), cfg)
	require.NoError(t, err)
	return path, body, meta
}

// Airflow 3 serves DAG runs under /api/v2, and its trigger body requires
// logical_date: null leaves the date to Airflow, as the v1 call does.
func TestExecuteAirflow_API3(t *testing.T) {
	path, body, meta := airflowPost(t, &types.AirflowTriggerConfig{APIVersion: 3, Body: `{"k":"v"}`})
	assert.Equal(t, "/api/v2/dags/etl/dagRuns", path)
	assert.JSONEq(t, `{"logical_date":null,"conf":{"k":"v"}}`, body)
	assert.Equal(t, 3, meta["airflow_api_version"])

	_, body, _ = airflowPost(t, &types.AirflowTriggerConfig{APIVersion: 3})
	assert.JSONEq(t, `{"logical_date":null}`, body)
}

// An unset apiVersion and apiVersion 2 send the Airflow 2 request unchanged
// and record version 2 for the status poll.
func TestExecuteAirflow_API2(t *testing.T) {
	for _, version := range []int{0, 2} {
		path, body, meta := airflowPost(t, &types.AirflowTriggerConfig{APIVersion: version, Body: `{"k":"v"}`})
		assert.Equal(t, "/api/v1/dags/etl/dagRuns", path, "apiVersion %d", version)
		assert.Equal(t, `{"conf":{"k":"v"}}`, body, "apiVersion %d", version)
		assert.Equal(t, 2, meta["airflow_api_version"], "apiVersion %d", version)

		_, body, _ = airflowPost(t, &types.AirflowTriggerConfig{APIVersion: version})
		assert.Equal(t, `{}`, body, "apiVersion %d", version)
	}
}

// TestExecuteAirflow_APIVersionFromJobConfig: a job.config apiVersion reaches
// the request through the decoder the orchestrator builds trigger configs with.
func TestExecuteAirflow_APIVersionFromJobConfig(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"dag_run_id": "r1"})
	}))
	t.Cleanup(srv.Close)
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	t.Cleanup(func() { defaultHTTPClient = origClient })

	data, err := json.Marshal(map[string]interface{}{"dagId": "etl", "url": srv.URL, "apiVersion": 3})
	require.NoError(t, err)
	var tc types.TriggerConfig
	require.NoError(t, lambda.TriggerUnmarshalers[types.TriggerAirflow](data, &tc))

	_, err = ExecuteAirflow(context.Background(), tc.Airflow)
	require.NoError(t, err)
	assert.Equal(t, "/api/v2/dags/etl/dagRuns", path)
}

func TestExecuteAirflow_UnsupportedAPIVersion(t *testing.T) {
	_, err := ExecuteAirflow(context.Background(), &types.AirflowTriggerConfig{URL: "http://example.com", DagID: "etl", APIVersion: 4})
	require.EqualError(t, err, "airflow trigger: unsupported apiVersion 4")
}

func TestCheckAirflowStatus_API3(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"state": "success"})
	}))
	t.Cleanup(srv.Close)

	state, err := CheckAirflowStatus(context.Background(), srv.URL, "etl", "r1", 3, nil, srv.Client())
	require.NoError(t, err)
	assert.Equal(t, "success", state)
	assert.Equal(t, "/api/v2/dags/etl/dagRuns/r1", path)

	_, err = CheckAirflowStatus(context.Background(), srv.URL, "etl", "r1", 1, nil, srv.Client())
	require.EqualError(t, err, "airflow status: unsupported apiVersion 1")
}

// A DAG ID holding /, ?, # and .. reaches Airflow as one escaped path
// segment of the trigger POST, with no query.
func TestExecuteAirflow_EscapesDagID(t *testing.T) {
	const dagID = "a/../b?x=1#y"
	var escapedPath, rawQuery, segment string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		escapedPath, rawQuery = r.URL.EscapedPath(), r.URL.RawQuery
		parts := strings.Split(escapedPath, "/")
		var err error
		segment, err = url.PathUnescape(parts[len(parts)-2])
		assert.NoError(t, err)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"dag_run_id": "r1"})
	}))
	t.Cleanup(srv.Close)
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	t.Cleanup(func() { defaultHTTPClient = origClient })

	_, err := ExecuteAirflow(context.Background(), &types.AirflowTriggerConfig{URL: srv.URL, DagID: dagID})
	require.NoError(t, err)
	assert.Equal(t, "/api/v1/dags/a%2F..%2Fb%3Fx=1%23y/dagRuns", escapedPath)
	assert.Empty(t, rawQuery)
	assert.Equal(t, dagID, segment)
}

// The status poll puts the DAG ID and the DAG run ID from the trigger
// metadata in one escaped path segment each, so neither can change the path
// or add a query. An ordinary run ID reaches Airflow unchanged.
func TestCheckAirflowStatus_EscapesIDs(t *testing.T) {
	tests := []struct {
		name, dagID, runID, wantPath string
	}{
		{"hostile ids", "a/../b?x#y", "../../variables?k=v#f",
			"/api/v2/dags/a%2F..%2Fb%3Fx%23y/dagRuns/..%2F..%2Fvariables%3Fk=v%23f"},
		{"manual run id", "etl", "manual__2026-11-21T00:00:00+00:00",
			"/api/v2/dags/etl/dagRuns/manual__2026-11-21T00:00:00+00:00"},
		{"percent, backslash and space", "etl", "%2e%2e\\x y",
			"/api/v2/dags/etl/dagRuns/%252e%252e%5Cx%20y"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var escapedPath, rawQuery, runSegment string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				escapedPath, rawQuery = r.URL.EscapedPath(), r.URL.RawQuery
				var err error
				runSegment, err = url.PathUnescape(escapedPath[strings.LastIndex(escapedPath, "/")+1:])
				assert.NoError(t, err)
				_ = json.NewEncoder(w).Encode(map[string]interface{}{"state": "success"})
			}))
			defer srv.Close()

			state, err := CheckAirflowStatus(context.Background(), srv.URL, tt.dagID, tt.runID, 3, nil, srv.Client())
			require.NoError(t, err)
			assert.Equal(t, "success", state)
			assert.Equal(t, tt.wantPath, escapedPath)
			assert.Empty(t, rawQuery)
			assert.Equal(t, tt.runID, runSegment)
		})
	}
}

// An ID of . or .. would still be a dot segment after escaping, which a
// server or proxy resolves against the path, so it is refused before any
// request.
func TestAirflow_RejectsDotSegmentIDs(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer srv.Close()
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	for _, id := range []string{".", ".."} {
		_, err := ExecuteAirflow(context.Background(), &types.AirflowTriggerConfig{URL: srv.URL, DagID: id})
		require.EqualError(t, err, `airflow trigger: dag id "`+id+`" is not a valid path segment`)

		_, err = CheckAirflowStatus(context.Background(), srv.URL, id, "run-1", 2, nil, srv.Client())
		require.EqualError(t, err, `airflow status: dag id "`+id+`" is not a valid path segment`)

		_, err = CheckAirflowStatus(context.Background(), srv.URL, "etl", id, 2, nil, srv.Client())
		require.EqualError(t, err, `airflow status: dag run id "`+id+`" is not a valid path segment`)
	}
	assert.False(t, called, "no request may be sent")
}
