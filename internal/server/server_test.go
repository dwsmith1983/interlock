package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/internal/testutil"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestServer(t *testing.T) (*httptest.Server, *testutil.MockProvider) {
	t.Helper()
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 5},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	runner := evaluator.NewRunner(nil)
	eng := engine.New(prov, reg, runner, nil)
	srv := New(":0", eng, prov)

	ts := httptest.NewServer(srv.router)
	t.Cleanup(ts.Close)
	return ts, prov
}

func TestHealthEndpoint(t *testing.T) {
	ts, _ := setupTestServer(t)

	resp, err := http.Get(ts.URL + "/api/health")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "ok", body["status"])
}

func TestPipelineEndpoints(t *testing.T) {
	ts, _ := setupTestServer(t)

	// Register pipeline
	pipelineJSON := `{"name":"test","archetype":"batch-ingestion","tier":2}`
	resp, err := http.Post(ts.URL+"/api/pipelines", "application/json", strings.NewReader(pipelineJSON))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	// List pipelines
	resp, err = http.Get(ts.URL + "/api/pipelines")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var pipelines []types.PipelineConfig
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&pipelines))
	assert.Len(t, pipelines, 1)
	assert.Equal(t, "test", pipelines[0].Name)

	// Get single pipeline
	resp, err = http.Get(ts.URL + "/api/pipelines/test")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Delete pipeline
	req, _ := http.NewRequest(http.MethodDelete, ts.URL+"/api/pipelines/test", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestEvaluateEndpoint(t *testing.T) {
	ts, prov := setupTestServer(t)

	// Register pipeline with pass evaluator
	pipeline := types.PipelineConfig{
		Name:      "eval-test",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	resp, err := http.Post(ts.URL+"/api/pipelines/eval-test/evaluate", "application/json", nil)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result types.ReadinessResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.Equal(t, types.Ready, result.Status)
}

func TestRegisterPipelineValidation(t *testing.T) {
	ts, _ := setupTestServer(t)

	// Missing name
	resp, err := http.Post(ts.URL+"/api/pipelines", "application/json", strings.NewReader(`{"archetype":"batch-ingestion"}`))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCompleteRunEndpoint(t *testing.T) {
	ts, prov := setupTestServer(t)

	// Create a run in RUNNING state
	run := types.RunState{
		RunID:      "complete-test",
		PipelineID: "test",
		Status:     types.RunRunning,
		Version:    3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(context.Background(), run))

	// Complete it
	resp, err := http.Post(
		ts.URL+"/api/runs/complete-test/complete",
		"application/json",
		strings.NewReader(`{"status":"success","metadata":{"rows":1000}}`),
	)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var updated types.RunState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&updated))
	assert.Equal(t, types.RunCompleted, updated.Status)
	assert.Equal(t, 4, updated.Version)
}
