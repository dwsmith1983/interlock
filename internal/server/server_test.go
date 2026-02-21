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
	srv := New(":0", eng, prov, reg, "", 0)

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

func TestE2E_FullLifecycle(t *testing.T) {
	ts, prov := setupTestServer(t)

	// Mock orchestrator that the trigger would call
	mockOrchestrator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mockOrchestrator.Close()

	// Step 1: Register pipeline with pass evaluator and HTTP trigger
	resp, err := http.Post(
		ts.URL+"/api/pipelines",
		"application/json",
		strings.NewReader(`{"name":"e2e-pipeline","archetype":"batch-ingestion","traits":{"freshness":{"evaluator":"../../testdata/evaluators/pass","timeout":5}},"trigger":{"type":"http","url":"`+mockOrchestrator.URL+`"}}`),
	)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Step 2: Evaluate → READY
	resp, err = http.Post(ts.URL+"/api/pipelines/e2e-pipeline/evaluate", "application/json", nil)
	require.NoError(t, err)
	var evalResult types.ReadinessResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&evalResult))
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, types.Ready, evalResult.Status)
	for _, trait := range evalResult.Traits {
		assert.Equal(t, types.TraitPass, trait.Status)
	}

	// Step 3: Check readiness → READY
	resp, err = http.Get(ts.URL + "/api/pipelines/e2e-pipeline/readiness")
	require.NoError(t, err)
	var readiness types.ReadinessResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&readiness))
	resp.Body.Close()
	assert.Equal(t, types.Ready, readiness.Status)

	// Step 4: Run → 202, TRIGGERING
	resp, err = http.Post(ts.URL+"/api/pipelines/e2e-pipeline/run", "application/json", nil)
	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
	var run types.RunState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&run))
	resp.Body.Close()
	assert.Equal(t, types.RunTriggering, run.Status)
	runID := run.RunID
	require.NotEmpty(t, runID)

	// Step 5: Get run → TRIGGERING
	resp, err = http.Get(ts.URL + "/api/runs/" + runID)
	require.NoError(t, err)
	var fetchedRun types.RunState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&fetchedRun))
	resp.Body.Close()
	assert.Equal(t, types.RunTriggering, fetchedRun.Status)

	// Advance to RUNNING (simulating what the watcher does)
	advancedRun := fetchedRun
	advancedRun.Status = types.RunRunning
	advancedRun.Version = fetchedRun.Version + 1
	advancedRun.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(context.Background(), runID, fetchedRun.Version, advancedRun)
	require.NoError(t, err)
	require.True(t, ok)

	// Step 6: Complete → 200, COMPLETED
	resp, err = http.Post(
		ts.URL+"/api/runs/"+runID+"/complete",
		"application/json",
		strings.NewReader(`{"status":"success","metadata":{"rows":5000}}`),
	)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var completedRun types.RunState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&completedRun))
	resp.Body.Close()
	assert.Equal(t, types.RunCompleted, completedRun.Status)

	// Step 7: Verify run → COMPLETED with metadata
	resp, err = http.Get(ts.URL + "/api/runs/" + runID)
	require.NoError(t, err)
	var finalRun types.RunState
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&finalRun))
	resp.Body.Close()
	assert.Equal(t, types.RunCompleted, finalRun.Status)
	assert.Equal(t, float64(5000), finalRun.Metadata["rows"])

	// Step 8: Verify events contain expected kinds in order
	events := prov.Events()
	var eventKinds []types.EventKind
	for _, e := range events {
		if e.PipelineID == "e2e-pipeline" {
			eventKinds = append(eventKinds, e.Kind)
		}
	}
	assert.Contains(t, eventKinds, types.EventTraitEvaluated)
	assert.Contains(t, eventKinds, types.EventReadinessChecked)
	assert.Contains(t, eventKinds, types.EventRunStateChanged)
	assert.Contains(t, eventKinds, types.EventCallbackReceived)

	// Verify ordering: TRAIT_EVALUATED before READINESS_CHECKED before RUN_STATE_CHANGED before CALLBACK_RECEIVED
	idxTrait := indexOf(eventKinds, types.EventTraitEvaluated)
	idxReadiness := indexOf(eventKinds, types.EventReadinessChecked)
	idxRunState := indexOf(eventKinds, types.EventRunStateChanged)
	idxCallback := indexOf(eventKinds, types.EventCallbackReceived)
	assert.Less(t, idxTrait, idxReadiness)
	assert.Less(t, idxReadiness, idxRunState)
	assert.Less(t, idxRunState, idxCallback)

	// Step 9: Verify metrics — evaluations_total > 0
	resp, err = http.Get(ts.URL + "/debug/vars")
	require.NoError(t, err)
	var vars map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&vars))
	resp.Body.Close()
	evalTotal, ok := vars["evaluations_total"].(float64)
	assert.True(t, ok)
	assert.Greater(t, evalTotal, float64(0))
}

func indexOf(kinds []types.EventKind, target types.EventKind) int {
	for i, k := range kinds {
		if k == target {
			return i
		}
	}
	return -1
}

func TestE2E_NotReady_BlocksRun(t *testing.T) {
	ts, prov := setupTestServer(t)

	// Register pipeline with fail evaluator
	pipeline := types.PipelineConfig{
		Name:      "not-ready-pipeline",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type: types.TriggerHTTP,
			URL:  "http://localhost:9999",
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	// Run → 412 Precondition Failed
	resp, err := http.Post(ts.URL+"/api/pipelines/not-ready-pipeline/run", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)

	var body map[string]interface{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "NOT_READY", body["status"])
	assert.NotEmpty(t, body["blocking"])
}

func TestE2E_CommandTrigger_BlockedViaAPI(t *testing.T) {
	ts, _ := setupTestServer(t)

	// Try to register pipeline with command trigger via API
	resp, err := http.Post(
		ts.URL+"/api/pipelines",
		"application/json",
		strings.NewReader(`{"name":"cmd-pipeline","archetype":"batch-ingestion","trigger":{"type":"command","command":"echo hello"}}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Contains(t, body["error"], "command triggers cannot be registered via API")
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
