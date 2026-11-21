package trigger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// databricksRun is the launch identity the orchestrator sets on every config.
var databricksRun = types.RunIdentity{PipelineID: "p", ScheduleID: "s", Date: "2026-03-11", RunID: "exec-1"}

// databricksToken is the idempotency token run-now gets for runID.
func databricksToken(runID string) string {
	sum := sha256.Sum256([]byte(runID))
	return hex.EncodeToString(sum[:])
}

// run-now starts the configured job with its arguments as job parameters.
// The job ID goes out as the exact JSON number, past 2^53 too, and the run ID
// comes back without passing through float64.
func TestExecuteDatabricks_RunNow(t *testing.T) {
	token := `"idempotency_token":"` + databricksToken(databricksRun.RunID) + `"`
	tests := []struct {
		name      string
		jobID     string
		args      map[string]string
		runID     string
		wantBody  string
		wantRunID string
	}{
		{name: "job parameters", jobID: "12345",
			args:  map[string]string{"par_day": "20260311", "par_hour": "05", "env": "prod"},
			runID: "77", wantRunID: "77",
			wantBody: `{"job_id":12345,` + token + `,"job_parameters":{"env":"prod","par_day":"20260311","par_hour":"05"}}`},
		{name: "no arguments", jobID: "12345", runID: "77", wantRunID: "77",
			wantBody: `{"job_id":12345,` + token + `}`},
		{name: "ids above 2^53", jobID: "9007199254740993", runID: "9007199254740995", wantRunID: "9007199254740995",
			wantBody: `{"job_id":9007199254740993,` + token + `}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var path, body string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				path = r.URL.Path
				b, _ := io.ReadAll(r.Body)
				body = string(b)
				_, _ = w.Write([]byte(`{"run_id": ` + tt.runID + `, "number_in_job": ` + tt.runID + `}`))
			}))
			defer srv.Close()

			cfg := &types.DatabricksTriggerConfig{WorkspaceURL: srv.URL, JobID: tt.jobID, Arguments: tt.args, Run: databricksRun}
			meta, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())

			require.NoError(t, err)
			assert.Equal(t, "/api/2.1/jobs/run-now", path)
			assert.Equal(t, tt.wantBody, body)
			assert.Equal(t, srv.URL, meta["databricks_workspace_url"])
			assert.Equal(t, tt.wantRunID, meta["databricks_run_id"])
		})
	}
}

// The idempotency token is the hex SHA-256 of the run ID, 64 characters (the
// run-now limit, while a run ID can be longer): a retried start of the same
// run sends the same token, so Databricks returns the run it already started,
// and another run sends its own.
func TestExecuteDatabricks_SendsIdempotencyToken(t *testing.T) {
	var tokens []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		token, _ := body["idempotency_token"].(string)
		tokens = append(tokens, token)
		_, _ = w.Write([]byte(`{"run_id": 77}`))
	}))
	defer srv.Close()

	longRun := databricksRun
	longRun.RunID = strings.Repeat("r", 80)
	other := databricksRun
	other.RunID = databricksRun.RunID + "-r1"
	for _, run := range []types.RunIdentity{databricksRun, databricksRun, other, longRun} {
		cfg := &types.DatabricksTriggerConfig{WorkspaceURL: srv.URL, JobID: "12345", Run: run}
		_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
		require.NoError(t, err)
	}

	require.Len(t, tokens, 4)
	assert.Equal(t, databricksToken(databricksRun.RunID), tokens[0])
	assert.Len(t, tokens[0], 64)
	assert.Equal(t, tokens[0], tokens[1], "a retried start must reuse the token")
	assert.NotEqual(t, tokens[0], tokens[2], "another run must get its own token")
	assert.Len(t, tokens[3], 64, "a long run ID still fits the 64-character limit")
}

// A launch with no run ID has nothing to make the idempotency token from, so
// it fails before any request.
func TestExecuteDatabricks_RequiresRunID(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
	defer srv.Close()

	cfg := &types.DatabricksTriggerConfig{WorkspaceURL: srv.URL, JobID: "12345"}
	_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())

	require.Error(t, err)
	assert.Equal(t, "databricks trigger: run ID is required for the idempotency token", err.Error())
	assert.False(t, called, "no request is sent without a run ID")
}

func TestExecuteDatabricks_JobIDNotAnInteger(t *testing.T) {
	for _, jobID := range []string{"abc", "12.5", "1e3", "0", "-5", "99999999999999999999"} {
		t.Run(jobID, func(t *testing.T) {
			called := false
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true }))
			defer srv.Close()

			cfg := &types.DatabricksTriggerConfig{WorkspaceURL: srv.URL, JobID: jobID, Run: databricksRun}
			_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())

			require.Error(t, err)
			assert.Contains(t, err.Error(), "jobId")
			assert.False(t, called, "no request is sent for an invalid jobId")
		})
	}
}

// A run-now response without a usable run ID fails with fixed text: the
// decoder's own message would quote the offending literal.
func TestExecuteDatabricks_BadRunID(t *testing.T) {
	tests := []struct {
		name, body, wantErr string
	}{
		{"missing", `{"number_in_job": 3}`, "databricks trigger: response missing run_id"},
		{"null", `{"run_id": null}`, "databricks trigger: response missing run_id"},
		{"zero", `{"run_id": 0}`, "databricks trigger: response run_id is not a positive integer"},
		{"negative", `{"run_id": -5}`, "databricks trigger: response run_id is not a positive integer"},
		{"out of float range", `{"run_id": 1e400}`, "databricks trigger: response run_id is not a positive integer"},
		{"out of int64 range", `{"run_id": ` + strings.Repeat("9", 2000) + `}`, "databricks trigger: response run_id is not a positive integer"},
		{"string", `{"run_id": "77"}`, "databricks trigger: response run_id is not a positive integer"},
		{"not an object", `[` + strings.Repeat("7", 2000) + `]`, "databricks trigger: response is not a JSON object"},
		{"not JSON", "<html>" + strings.Repeat("x", 2000), "databricks trigger: response is not a JSON object"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			cfg := &types.DatabricksTriggerConfig{WorkspaceURL: srv.URL, JobID: "12345", Run: databricksRun}
			_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())

			require.Error(t, err)
			assert.Equal(t, tt.wantErr, err.Error())
		})
	}
}

// A runs/get response that is not a JSON object fails with fixed text.
func TestCheckDatabricksStatus_BadResponse(t *testing.T) {
	for name, body := range map[string]string{
		"not JSON":      "<html>" + strings.Repeat("x", 2000),
		"not an object": `[` + strings.Repeat("7", 2000) + `]`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()

			r := NewRunner(WithHTTPClient(srv.Client()))
			_, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
				"databricks_workspace_url": srv.URL,
				"databricks_run_id":        "123",
			}, nil)

			require.Error(t, err)
			assert.Equal(t, "databricks status: response is not a JSON object", err.Error())
		})
	}
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

	cfg := &types.DatabricksTriggerConfig{
		WorkspaceURL: srv.URL,
		JobID:        "12345",
		Headers:      map[string]string{"Authorization": "Bearer db-token"},
		Run:          databricksRun,
	}

	_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
	require.NoError(t, err)
	assert.Equal(t, "Bearer db-token", receivedAuth)
}

func TestExecuteDatabricks_MissingWorkspaceURL(t *testing.T) {
	cfg := &types.DatabricksTriggerConfig{JobID: "12345"}
	_, err := ExecuteDatabricks(context.Background(), cfg, http.DefaultClient)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workspaceUrl is required")
}

func TestExecuteDatabricks_MissingJobID(t *testing.T) {
	cfg := &types.DatabricksTriggerConfig{WorkspaceURL: "https://example.com"}
	_, err := ExecuteDatabricks(context.Background(), cfg, http.DefaultClient)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jobId is required")
}

func TestExecuteDatabricks_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	cfg := &types.DatabricksTriggerConfig{
		WorkspaceURL: srv.URL,
		JobID:        "12345",
		Run:          databricksRun,
	}

	_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestCheckDatabricksStatus(t *testing.T) {
	tests := []struct {
		name            string
		lifeCycleState  string
		resultState     string
		expected        RunCheckState
		failureCategory types.FailureCategory
	}{
		{"terminated_success", "TERMINATED", "SUCCESS", RunCheckSucceeded, ""},
		{"terminated_failure", "TERMINATED", "FAILED", RunCheckFailed, types.FailureTransient},
		{"terminated_timedout", "TERMINATED", "TIMEDOUT", RunCheckFailed, types.FailureTransient},
		{"terminated_cancelled", "TERMINATED", "CANCELED", RunCheckFailed, types.FailureTransient},
		{"internal_error", "INTERNAL_ERROR", "", RunCheckFailed, types.FailureTransient},
		{"skipped", "SKIPPED", "", RunCheckFailed, types.FailurePermanent},
		{"running", "RUNNING", "", RunCheckRunning, ""},
		{"pending", "PENDING", "", RunCheckRunning, ""},
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
			assert.Equal(t, tt.failureCategory, result.FailureCategory)
		})
	}
}

func TestCheckDatabricksStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

func TestCheckDatabricksStatus_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	r := NewRunner(WithHTTPClient(srv.Client()))
	_, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
		"databricks_workspace_url": srv.URL,
		"databricks_run_id":        "123",
	}, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestCheckDatabricksStatus_MissingWorkspaceURL(t *testing.T) {
	r := NewRunner()
	result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
		"databricks_run_id": "123",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "missing databricks metadata", result.Message)
}

func TestCheckDatabricksStatus_MissingRunID(t *testing.T) {
	r := NewRunner()
	result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
		"databricks_workspace_url": "https://example.com",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "missing databricks metadata", result.Message)
}

func TestExecuteDatabricks_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TEST_VAR", "safe")
	t.Setenv("SECRET_VAR", "leaked")

	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"run_id": 42,
		})
	}))
	defer srv.Close()

	cfg := &types.DatabricksTriggerConfig{
		WorkspaceURL: srv.URL,
		JobID:        "12345",
		Headers:      map[string]string{"Authorization": "Bearer ${INTERLOCK_TEST_VAR}/${SECRET_VAR}"},
		Run:          databricksRun,
	}

	_, err := ExecuteDatabricks(context.Background(), cfg, srv.Client())
	require.NoError(t, err)
	assert.Equal(t, "Bearer safe/", receivedAuth)
	assert.NotContains(t, receivedAuth, "leaked")
}

func TestCheckDatabricksStatus_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TEST_VAR", "safe")
	t.Setenv("SECRET_VAR", "leaked")

	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": map[string]interface{}{
				"life_cycle_state": "RUNNING",
			},
		})
	}))
	defer srv.Close()

	r := NewRunner(WithHTTPClient(srv.Client()))
	_, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
		"databricks_workspace_url": srv.URL,
		"databricks_run_id":        "123",
	}, map[string]string{"Authorization": "Bearer ${INTERLOCK_TEST_VAR}/${SECRET_VAR}"})
	require.NoError(t, err)
	assert.Equal(t, "Bearer safe/", receivedAuth)
	assert.NotContains(t, receivedAuth, "leaked")
}

func TestCheckDatabricksStatus_WithHeaders(t *testing.T) {
	var receivedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"state": map[string]interface{}{
				"life_cycle_state": "RUNNING",
			},
		})
	}))
	defer srv.Close()

	r := NewRunner(WithHTTPClient(srv.Client()))
	result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
		"databricks_workspace_url": srv.URL,
		"databricks_run_id":        "123",
	}, map[string]string{"Authorization": "Bearer test-token"})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "Bearer test-token", receivedAuth)
}

// The poll's message goes to the job log: control characters are stripped,
// the header's INTERLOCK_ values redacted before the cut, and the length is
// bounded like an error body.
func TestCheckDatabricksStatus_SanitizesMessage(t *testing.T) {
	const token = "zq9-tok-secret" // every prefix of 3 or more bytes contains zq9
	t.Setenv("INTERLOCK_DATABRICKS_TOKEN", token)
	headers := map[string]string{"Authorization": "Bearer ${INTERLOCK_DATABRICKS_TOKEN}"}
	const prefix = "TERMINATED/FAILED: "
	// fill pads the message so the token starts 5 bytes before the cut.
	fill := strings.Repeat("x", maxErrorBodyBytes-len(prefix)-5)
	tests := []struct {
		name, stateMessage, want string
	}{
		{"control characters and length", "task failed\x1b[31m" + strings.Repeat("x", 600),
			prefix + "task failed[31m" + strings.Repeat("x", maxErrorBodyBytes-len(prefix+"task failed[31m"))},
		{"token mid-message", "failed for Bearer " + token + " on retry",
			prefix + "failed for Bearer [REDACTED] on retry"},
		{"token across the cut", fill + token + " tail",
			prefix + fill + "[REDA"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]interface{}{
					"state": map[string]interface{}{
						"life_cycle_state": "TERMINATED",
						"result_state":     "FAILED",
						"state_message":    tt.stateMessage,
					},
				})
			}))
			defer srv.Close()

			r := NewRunner(WithHTTPClient(srv.Client()))
			result, err := r.checkDatabricksStatus(context.Background(), map[string]interface{}{
				"databricks_workspace_url": srv.URL,
				"databricks_run_id":        "123",
			}, headers)

			require.NoError(t, err)
			assert.Equal(t, RunCheckFailed, result.State)
			assert.Equal(t, tt.want, result.Message)
			assert.LessOrEqual(t, len(result.Message), maxErrorBodyBytes)
			assert.NotContains(t, result.Message, "zq9")
		})
	}
}
