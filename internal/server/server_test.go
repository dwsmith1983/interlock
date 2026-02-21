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
	return setupTestServerWithOpts(t, "", 0)
}

func setupTestServerWithOpts(t *testing.T, apiKey string, maxBody int64) (*httptest.Server, *testutil.MockProvider) {
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

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)
	srv := New(":0", eng, prov, reg, apiKey, maxBody)

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

// --- Handler Tests (Phase 5c) ---

func TestPushTrait_Success(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "trait-test", Archetype: "batch-ingestion"}))

	resp, err := http.Post(
		ts.URL+"/api/pipelines/trait-test/traits/freshness",
		"application/json",
		strings.NewReader(`{"status":"PASS","value":{"age_hours":1},"reason":"fresh data"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	trait, err := prov.GetTrait(ctx, "trait-test", "freshness")
	require.NoError(t, err)
	require.NotNil(t, trait)
	assert.Equal(t, types.TraitPass, trait.Status)
}

func TestPushTrait_InvalidStatus(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "trait-bad", Archetype: "batch-ingestion"}))

	resp, err := http.Post(
		ts.URL+"/api/pipelines/trait-bad/traits/freshness",
		"application/json",
		strings.NewReader(`{"status":"INVALID"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPushTrait_PipelineNotFound(t *testing.T) {
	ts, _ := setupTestServer(t)

	resp, err := http.Post(
		ts.URL+"/api/pipelines/nonexistent/traits/freshness",
		"application/json",
		strings.NewReader(`{"status":"PASS"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestListEvents_Success(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	require.NoError(t, prov.AppendEvent(ctx, types.Event{
		Kind: types.EventTraitEvaluated, PipelineID: "events-pipe", Timestamp: time.Now(),
	}))
	require.NoError(t, prov.AppendEvent(ctx, types.Event{
		Kind: types.EventReadinessChecked, PipelineID: "events-pipe", Timestamp: time.Now(),
	}))

	resp, err := http.Get(ts.URL + "/api/pipelines/events-pipe/events")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var events []types.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	assert.Len(t, events, 2)
}

func TestListEvents_WithLimit(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		require.NoError(t, prov.AppendEvent(ctx, types.Event{
			Kind: types.EventTraitEvaluated, PipelineID: "limit-pipe", Timestamp: time.Now(),
		}))
	}

	resp, err := http.Get(ts.URL + "/api/pipelines/limit-pipe/events?limit=2")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var events []types.Event
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&events))
	assert.Len(t, events, 2)
}

func TestRequestRerun_Success(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	pipeline := types.PipelineConfig{
		Name:      "rerun-test",
		Archetype: "batch-ingestion",
		Traits:    map[string]types.TraitConfig{"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5}},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	resp, err := http.Post(
		ts.URL+"/api/pipelines/rerun-test/rerun",
		"application/json",
		strings.NewReader(`{"originalDate":"2026-02-20","reason":"late data"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestRequestRerun_MissingFields(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()
	require.NoError(t, prov.RegisterPipeline(ctx, types.PipelineConfig{Name: "rerun-missing", Archetype: "batch-ingestion"}))

	// Missing reason
	resp, err := http.Post(
		ts.URL+"/api/pipelines/rerun-missing/rerun",
		"application/json",
		strings.NewReader(`{"originalDate":"2026-02-20"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Missing originalDate
	resp2, err := http.Post(
		ts.URL+"/api/pipelines/rerun-missing/rerun",
		"application/json",
		strings.NewReader(`{"reason":"late data"}`),
	)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp2.StatusCode)
}

func TestRequestRerun_NotReady(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	pipeline := types.PipelineConfig{
		Name:      "rerun-not-ready",
		Archetype: "batch-ingestion",
		Traits:    map[string]types.TraitConfig{"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5}},
	}
	require.NoError(t, prov.RegisterPipeline(ctx, pipeline))

	resp, err := http.Post(
		ts.URL+"/api/pipelines/rerun-not-ready/rerun",
		"application/json",
		strings.NewReader(`{"originalDate":"2026-02-20","reason":"late data"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode)
}

func TestCompleteRerun_Success(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	require.NoError(t, prov.PutRerun(ctx, types.RerunRecord{
		RerunID:    "rerun-complete-1",
		PipelineID: "test",
		Status:     types.RunRunning,
		RerunRunID: "run-1",
	}))

	resp, err := http.Post(
		ts.URL+"/api/reruns/rerun-complete-1/complete",
		"application/json",
		strings.NewReader(`{"status":"success"}`),
	)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var record types.RerunRecord
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&record))
	assert.Equal(t, types.RunCompleted, record.Status)
}

func TestListReruns_Success(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	require.NoError(t, prov.PutRerun(ctx, types.RerunRecord{
		RerunID: "rr-1", PipelineID: "reruns-pipe", Status: types.RunPending,
	}))

	resp, err := http.Get(ts.URL + "/api/pipelines/reruns-pipe/reruns")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var records []types.RerunRecord
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&records))
	assert.Len(t, records, 1)
}

func TestListAllReruns_Success(t *testing.T) {
	ts, prov := setupTestServer(t)
	ctx := context.Background()

	require.NoError(t, prov.PutRerun(ctx, types.RerunRecord{
		RerunID: "rr-all-1", PipelineID: "pipe-a", Status: types.RunPending,
	}))
	require.NoError(t, prov.PutRerun(ctx, types.RerunRecord{
		RerunID: "rr-all-2", PipelineID: "pipe-b", Status: types.RunPending,
	}))

	resp, err := http.Get(ts.URL + "/api/reruns")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var records []types.RerunRecord
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&records))
	assert.Len(t, records, 2)
}

func TestPipelineNameValidation(t *testing.T) {
	ts, _ := setupTestServer(t)

	tests := []struct {
		name string
		code int
	}{
		{`{"name":"a","archetype":"batch-ingestion"}`, http.StatusBadRequest},            // too short (1 char)
		{`{"name":"ab","archetype":"batch-ingestion"}`, http.StatusCreated},               // minimum valid (2 chars)
		{`{"name":"UPPER","archetype":"batch-ingestion"}`, http.StatusBadRequest},          // uppercase
		{`{"name":"has space","archetype":"batch-ingestion"}`, http.StatusBadRequest},      // space
		{`{"name":"my.pipeline-v2","archetype":"batch-ingestion"}`, http.StatusCreated},    // dots and dashes
		{`{"name":"../../etc","archetype":"batch-ingestion"}`, http.StatusBadRequest},      // path traversal
		{`{"name":"key:injection","archetype":"batch-ingestion"}`, http.StatusBadRequest},  // colon
	}

	for _, tt := range tests {
		resp, err := http.Post(ts.URL+"/api/pipelines", "application/json", strings.NewReader(tt.name))
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, tt.code, resp.StatusCode, "for body: %s", tt.name)
	}
}

// --- Middleware Tests (Phase 5d) ---

func TestAPIKeyAuth_Valid(t *testing.T) {
	ts, _ := setupTestServerWithOpts(t, "test-secret", 0)

	req, _ := http.NewRequest("GET", ts.URL+"/api/pipelines", nil)
	req.Header.Set("X-API-Key", "test-secret")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAPIKeyAuth_Invalid(t *testing.T) {
	ts, _ := setupTestServerWithOpts(t, "test-secret", 0)

	req, _ := http.NewRequest("GET", ts.URL+"/api/pipelines", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAPIKeyAuth_Missing(t *testing.T) {
	ts, _ := setupTestServerWithOpts(t, "test-secret", 0)

	resp, err := http.Get(ts.URL + "/api/pipelines")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

func TestAPIKeyAuth_HealthBypass(t *testing.T) {
	ts, _ := setupTestServerWithOpts(t, "test-secret", 0)

	resp, err := http.Get(ts.URL + "/api/health")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMaxBody_Enforced(t *testing.T) {
	ts, _ := setupTestServerWithOpts(t, "", 50) // 50 bytes max

	bigBody := strings.Repeat("x", 200)
	resp, err := http.Post(ts.URL+"/api/pipelines", "application/json", strings.NewReader(bigBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
