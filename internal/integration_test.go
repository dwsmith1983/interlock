package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/alert"
	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/engine"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	"github.com/dwsmith1983/interlock/internal/lifecycle"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeEvaluator(t *testing.T, dir, name, script string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(script), 0o755))
	return path
}

func readAlertLog(t *testing.T, path string) []types.Alert {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var alerts []types.Alert
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		var a types.Alert
		if err := json.Unmarshal(line, &a); err != nil {
			continue
		}
		alerts = append(alerts, a)
	}
	return alerts
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

// ---------------------------------------------------------------------------
// Test 1: Happy path — evaluate READY, trigger command, verify COMPLETED
// ---------------------------------------------------------------------------

func TestIntegration_HappyPath_CommandTrigger(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))
	alertLog := filepath.Join(tmpDir, "alerts.log")

	// Create evaluators that all pass
	freshEval := writeEvaluator(t, evalDir, "check-freshness", `#!/bin/bash
echo '{"status":"PASS","value":{"lagSeconds":10,"threshold":300}}'
`)
	depEval := writeEvaluator(t, evalDir, "check-deps", `#!/bin/bash
echo '{"status":"PASS","value":{"upstream":"completed"}}'
`)
	resEval := writeEvaluator(t, evalDir, "check-resources", `#!/bin/bash
echo '{"status":"PASS","value":{"cpu_available":true}}'
`)

	// Set up archetype
	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "source-freshness", DefaultConfig: map[string]interface{}{"maxLagSeconds": 300}, DefaultTTL: 300, DefaultTimeout: 10},
			{Type: "upstream-dependency", DefaultTTL: 600, DefaultTimeout: 10},
			{Type: "resource-availability", DefaultTTL: 120, DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	// Set up provider and register pipeline
	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "daily-sales",
		Archetype: "batch-ingestion",
		Tier:      2,
		Traits: map[string]types.TraitConfig{
			"source-freshness":      {Evaluator: freshEval, Timeout: 5},
			"upstream-dependency":   {Evaluator: depEval, Timeout: 5},
			"resource-availability": {Evaluator: resEval, Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: fmt.Sprintf("echo triggered > %s/trigger-output.txt", tmpDir)},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	// Set up alerts (console + file)
	dispatcher, err := alert.NewDispatcher([]types.AlertConfig{
		{Type: types.AlertConsole},
		{Type: types.AlertFile, Path: alertLog},
	}, nil)
	require.NoError(t, err)

	// Build engine
	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())

	ctx := context.Background()

	// Step 1: Evaluate
	result, err := eng.Evaluate(ctx, "daily-sales")
	require.NoError(t, err)

	assert.Equal(t, types.Ready, result.Status, "pipeline should be READY")
	assert.Empty(t, result.Blocking, "no traits should be blocking")
	assert.Len(t, result.Traits, 3, "all 3 traits should be evaluated")

	for _, trait := range result.Traits {
		assert.Equal(t, types.TraitPass, trait.Status, "trait %s should PASS", trait.TraitType)
	}

	// Step 2: Create run, CAS to TRIGGERING
	runID := "daily-sales-test-run-1"
	run := types.RunState{
		RunID:      runID,
		PipelineID: "daily-sales",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(ctx, runID, 1, run)
	require.NoError(t, err)
	assert.True(t, ok, "CAS should succeed")

	// Step 3: Execute command trigger
	triggerOutput := filepath.Join(tmpDir, "trigger-output.txt")
	cmd := pipeline.Trigger.Command.Command
	require.NotEmpty(t, cmd)

	err = execCommand(ctx, cmd)
	require.NoError(t, err, "trigger command should succeed")

	// Verify trigger actually ran
	_, err = os.Stat(triggerOutput)
	assert.NoError(t, err, "trigger should have created output file")

	// Step 4: Transition to COMPLETED
	require.NoError(t, lifecycle.Transition(types.RunTriggering, types.RunRunning))
	require.NoError(t, lifecycle.Transition(types.RunRunning, types.RunCompleted))

	run.Status = types.RunCompleted
	run.Version = 3
	run.UpdatedAt = time.Now()
	ok, err = prov.CompareAndSwapRunState(ctx, runID, 2, run)
	require.NoError(t, err)
	assert.True(t, ok)

	// Verify final state
	finalRun, err := prov.GetRunState(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, types.RunCompleted, finalRun.Status)
	assert.Equal(t, 3, finalRun.Version)

	// No alerts for happy path (no failures)
	alerts := readAlertLog(t, alertLog)
	assert.Empty(t, alerts, "happy path should produce no alerts")
}

// ---------------------------------------------------------------------------
// Test 2: Blocked path — evaluator fails, NOT_READY, alerts fire
// ---------------------------------------------------------------------------

func TestIntegration_BlockedPath_AlertsFire(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))
	alertLog := filepath.Join(tmpDir, "alerts.log")

	// Source freshness FAILS, others pass
	freshEval := writeEvaluator(t, evalDir, "check-freshness-fail", `#!/bin/bash
echo '{"status":"FAIL","value":{"lagSeconds":9999,"threshold":300},"reason":"source is 9999s behind"}'
`)
	depEval := writeEvaluator(t, evalDir, "check-deps", `#!/bin/bash
echo '{"status":"PASS","value":{"upstream":"completed"}}'
`)
	// Resource evaluator hangs (timeout test)
	writeEvaluator(t, evalDir, "check-resources-hang", `#!/bin/bash
sleep 10
echo '{"status":"PASS","value":{}}'
`)

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "batch-ingestion",
		RequiredTraits: []types.TraitDefinition{
			{Type: "source-freshness", DefaultTimeout: 10},
			{Type: "upstream-dependency", DefaultTimeout: 10},
			{Type: "resource-availability", DefaultTimeout: 1}, // 1s timeout
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "blocked-pipeline",
		Archetype: "batch-ingestion",
		Traits: map[string]types.TraitConfig{
			"source-freshness":      {Evaluator: freshEval, Timeout: 5},
			"upstream-dependency":   {Evaluator: depEval, Timeout: 5},
			"resource-availability": {Evaluator: filepath.Join(evalDir, "check-resources-hang"), Timeout: 1},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	dispatcher, err := alert.NewDispatcher([]types.AlertConfig{
		{Type: types.AlertConsole},
		{Type: types.AlertFile, Path: alertLog},
	}, nil)
	require.NoError(t, err)

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())

	// Evaluate
	result, err := eng.Evaluate(context.Background(), "blocked-pipeline")
	require.NoError(t, err)

	// Verify NOT_READY
	assert.Equal(t, types.NotReady, result.Status, "pipeline should be NOT_READY")
	assert.NotEmpty(t, result.Blocking, "should have blocking traits")

	// Verify which traits blocked
	blockingSet := make(map[string]bool)
	for _, b := range result.Blocking {
		blockingSet[b] = true
	}
	assert.True(t, blockingSet["source-freshness"], "source-freshness should be blocking")
	assert.True(t, blockingSet["resource-availability"], "resource-availability should be blocking (timeout)")

	// Verify individual trait results
	traitMap := make(map[string]types.TraitEvaluation)
	for _, tr := range result.Traits {
		traitMap[tr.TraitType] = tr
	}

	assert.Equal(t, types.TraitFail, traitMap["source-freshness"].Status, "freshness should FAIL")
	assert.Contains(t, traitMap["source-freshness"].Reason, "source is 9999s behind")

	assert.Equal(t, types.TraitPass, traitMap["upstream-dependency"].Status, "deps should PASS")

	assert.Equal(t, types.TraitFail, traitMap["resource-availability"].Status, "resources should FAIL (timeout)")
	assert.Contains(t, traitMap["resource-availability"].Reason, "EVALUATOR_TIMEOUT")

	// Verify alerts were logged
	alerts := readAlertLog(t, alertLog)
	assert.NotEmpty(t, alerts, "alerts should have been written to file")

	// Should have alerts for: source-freshness fail, resource-availability timeout, overall NOT_READY
	var alertMessages []string
	for _, a := range alerts {
		alertMessages = append(alertMessages, a.Message)
	}

	hasSourceAlert := false
	hasTimeoutAlert := false
	hasBlockedAlert := false
	for _, msg := range alertMessages {
		if containsAll(msg, "source-freshness", "blocked-pipeline") {
			hasSourceAlert = true
		}
		if containsAll(msg, "resource-availability", "blocked-pipeline") {
			hasTimeoutAlert = true
		}
		if containsAll(msg, "blocked-pipeline", "blocked by") {
			hasBlockedAlert = true
		}
	}

	assert.True(t, hasSourceAlert, "should have alert for source-freshness failure, got: %v", alertMessages)
	assert.True(t, hasTimeoutAlert, "should have alert for resource-availability timeout, got: %v", alertMessages)
	assert.True(t, hasBlockedAlert, "should have overall NOT_READY alert, got: %v", alertMessages)

	// Verify readiness was stored
	cached, err := prov.GetReadiness(context.Background(), "blocked-pipeline")
	require.NoError(t, err)
	assert.NotNil(t, cached)
	assert.Equal(t, types.NotReady, cached.Status)
}

// ---------------------------------------------------------------------------
// Test 3: HTTP trigger with mock server + completion callback
// ---------------------------------------------------------------------------

func TestIntegration_HTTPTrigger_WithCallback(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))

	freshEval := writeEvaluator(t, evalDir, "check-pass", `#!/bin/bash
echo '{"status":"PASS","value":{"ok":true}}'
`)

	// Mock orchestrator HTTP server
	var triggerReceived sync.WaitGroup
	triggerReceived.Add(1)
	var receivedBody []byte

	mockOrchestrator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer triggerReceived.Done()

		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = body[:n]

		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"dag_run_id": "mock-123"}`))
	}))
	defer mockOrchestrator.Close()

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "simple",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "http-triggered",
		Archetype: "simple",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: freshEval, Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type: types.TriggerHTTP,
			HTTP: &types.HTTPTriggerConfig{
				Method: "POST",
				URL:    mockOrchestrator.URL,
				Headers: map[string]string{
					"Authorization": "Bearer test-token",
				},
				Body: `{"conf":{"triggered_by":"interlock"}}`,
			},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)

	ctx := context.Background()

	// Step 1: Evaluate — should be READY
	result, err := eng.Evaluate(ctx, "http-triggered")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)

	// Step 2: Create run, CAS to TRIGGERING
	runID := "http-triggered-run-1"
	run := types.RunState{
		RunID:      runID,
		PipelineID: "http-triggered",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(ctx, runID, 1, run)
	require.NoError(t, err)
	assert.True(t, ok)

	// Step 3: Fire HTTP trigger
	err = execHTTPTrigger(ctx, pipeline.Trigger.HTTP)
	require.NoError(t, err, "HTTP trigger should succeed")

	// Wait for mock server to receive the request
	triggerReceived.Wait()
	assert.Contains(t, string(receivedBody), "triggered_by")

	// Step 4: Transition to RUNNING (HTTP trigger = async, awaiting callback)
	run.Status = types.RunRunning
	run.Version = 3
	run.UpdatedAt = time.Now()
	ok, err = prov.CompareAndSwapRunState(ctx, runID, 2, run)
	require.NoError(t, err)
	assert.True(t, ok)

	// Step 5: Simulate completion callback
	require.NoError(t, lifecycle.Transition(types.RunRunning, types.RunCompleted))

	run.Status = types.RunCompleted
	run.Version = 4
	run.UpdatedAt = time.Now()
	run.Metadata = map[string]interface{}{"rows_processed": 150000}
	ok, err = prov.CompareAndSwapRunState(ctx, runID, 3, run)
	require.NoError(t, err)
	assert.True(t, ok)

	// Verify final state
	finalRun, err := prov.GetRunState(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, types.RunCompleted, finalRun.Status)
	assert.Equal(t, 4, finalRun.Version)
	assert.EqualValues(t, 150000, finalRun.Metadata["rows_processed"])
}

// ---------------------------------------------------------------------------
// Test 4: CAS race — 10 goroutines compete, exactly 1 wins
// ---------------------------------------------------------------------------

func TestIntegration_CASRace_OneTriggerWins(t *testing.T) {
	prov := testutil.NewMockProvider()

	run := types.RunState{
		RunID:      "race-run",
		PipelineID: "race-pipeline",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	require.NoError(t, prov.PutRunState(context.Background(), run))

	var wins int32
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			newRun := types.RunState{
				RunID:      "race-run",
				PipelineID: "race-pipeline",
				Status:     types.RunTriggering,
				Version:    2,
				UpdatedAt:  time.Now(),
				Metadata:   map[string]interface{}{"winner": id},
			}
			ok, err := prov.CompareAndSwapRunState(context.Background(), "race-run", 1, newRun)
			if err == nil && ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	assert.Equal(t, int32(1), wins, "exactly 1 goroutine should win the CAS")
}

// ---------------------------------------------------------------------------
// Test 5: Stale trait detection via CheckReadiness
// ---------------------------------------------------------------------------

func TestIntegration_StaleTrait_CheckReadiness(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))
	alertLog := filepath.Join(tmpDir, "alerts.log")

	passEval := writeEvaluator(t, evalDir, "check-pass", `#!/bin/bash
echo '{"status":"PASS","value":{"ok":true}}'
`)

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "two-trait",
		RequiredTraits: []types.TraitDefinition{
			{Type: "trait-a", DefaultTimeout: 10},
			{Type: "trait-b", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "stale-test",
		Archetype: "two-trait",
		Traits: map[string]types.TraitConfig{
			"trait-a": {Evaluator: passEval, Timeout: 5},
			"trait-b": {Evaluator: passEval, Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	dispatcher, err := alert.NewDispatcher([]types.AlertConfig{
		{Type: types.AlertFile, Path: alertLog},
	}, nil)
	require.NoError(t, err)

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())
	ctx := context.Background()

	// Evaluate — both pass
	result, err := eng.Evaluate(ctx, "stale-test")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)

	// Now manually delete trait-b to simulate TTL expiry
	prov.DeleteTrait("stale-test", "trait-b")

	// CheckReadiness (reads stored traits, doesn't re-evaluate)
	staleResult, err := eng.CheckReadiness(ctx, "stale-test")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, staleResult.Status, "should be NOT_READY with stale trait")

	// Verify blocking reason mentions STALE
	found := false
	for _, b := range staleResult.Blocking {
		if containsAll(b, "trait-b", "STALE") {
			found = true
		}
	}
	assert.True(t, found, "blocking should mention trait-b as STALE, got: %v", staleResult.Blocking)

	// Verify the stale trait shows correct status
	traitMap := make(map[string]types.TraitEvaluation)
	for _, tr := range staleResult.Traits {
		traitMap[tr.TraitType] = tr
	}
	assert.Equal(t, types.TraitPass, traitMap["trait-a"].Status)
	assert.Equal(t, types.TraitStale, traitMap["trait-b"].Status)
}

// ---------------------------------------------------------------------------
// Test 6: KV round-trip — verify all stored fields after evaluation
// ---------------------------------------------------------------------------

func TestIntegration_KVRoundTrip_FieldValidation(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))

	// Evaluator that returns specific values we can verify
	freshEval := writeEvaluator(t, evalDir, "check-freshness", `#!/bin/bash
echo '{"status":"PASS","value":{"lagSeconds":42,"threshold":300,"source":"sales_events"},"reason":"within threshold"}'
`)

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "kv-test-archetype",
		RequiredTraits: []types.TraitDefinition{
			{
				Type:           "source-freshness",
				DefaultConfig:  map[string]interface{}{"maxLagSeconds": 300},
				DefaultTTL:     600,
				DefaultTimeout: 10,
			},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "kv-roundtrip-test",
		Archetype: "kv-test-archetype",
		Traits: map[string]types.TraitConfig{
			"source-freshness": {Evaluator: freshEval, TTL: 120, Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)
	ctx := context.Background()
	beforeEval := time.Now()

	// Evaluate
	result, err := eng.Evaluate(ctx, "kv-roundtrip-test")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)

	afterEval := time.Now()

	// --- Verify stored trait round-trip ---
	storedTrait, err := prov.GetTrait(ctx, "kv-roundtrip-test", "source-freshness")
	require.NoError(t, err)
	require.NotNil(t, storedTrait)

	assert.Equal(t, "kv-roundtrip-test", storedTrait.PipelineID, "pipelineId must match")
	assert.Equal(t, "source-freshness", storedTrait.TraitType, "traitType must match")
	assert.Equal(t, types.TraitPass, storedTrait.Status, "status must be PASS")
	assert.Equal(t, "within threshold", storedTrait.Reason, "reason must round-trip")

	// Verify value map fields
	require.NotNil(t, storedTrait.Value)
	assert.EqualValues(t, 42, storedTrait.Value["lagSeconds"], "value.lagSeconds must round-trip")
	assert.EqualValues(t, 300, storedTrait.Value["threshold"], "value.threshold must round-trip")
	assert.Equal(t, "sales_events", storedTrait.Value["source"], "value.source must round-trip")

	// Verify evaluatedAt is within our time window
	assert.False(t, storedTrait.EvaluatedAt.Before(beforeEval), "evaluatedAt must be after test start")
	assert.False(t, storedTrait.EvaluatedAt.After(afterEval), "evaluatedAt must be before test end")

	// Verify expiresAt when TTL is set (TTL=120s configured on pipeline)
	require.NotNil(t, storedTrait.ExpiresAt, "expiresAt must be set when TTL > 0")
	expectedExpiry := storedTrait.EvaluatedAt.Add(120 * time.Second)
	assert.WithinDuration(t, expectedExpiry, *storedTrait.ExpiresAt, 2*time.Second, "expiresAt should be evaluatedAt + TTL")

	// --- Verify stored readiness round-trip ---
	storedReadiness, err := prov.GetReadiness(ctx, "kv-roundtrip-test")
	require.NoError(t, err)
	require.NotNil(t, storedReadiness)

	assert.Equal(t, "kv-roundtrip-test", storedReadiness.PipelineID, "readiness.pipelineId must match")
	assert.Equal(t, types.Ready, storedReadiness.Status, "readiness.status must be READY")
	assert.Empty(t, storedReadiness.Blocking, "readiness.blocking must be empty")
	assert.Len(t, storedReadiness.Traits, 1, "readiness.traits must have 1 entry")
	assert.False(t, storedReadiness.EvaluatedAt.Before(beforeEval))
	assert.False(t, storedReadiness.EvaluatedAt.After(afterEval))

	// Verify trait inside readiness matches stored trait
	assert.Equal(t, storedTrait.PipelineID, storedReadiness.Traits[0].PipelineID)
	assert.Equal(t, storedTrait.TraitType, storedReadiness.Traits[0].TraitType)
	assert.Equal(t, storedTrait.Status, storedReadiness.Traits[0].Status)

	// --- Verify pipeline registration round-trip ---
	storedPipeline, err := prov.GetPipeline(ctx, "kv-roundtrip-test")
	require.NoError(t, err)
	require.NotNil(t, storedPipeline)
	assert.Equal(t, "kv-roundtrip-test", storedPipeline.Name)
	assert.Equal(t, "kv-test-archetype", storedPipeline.Archetype)
	assert.Equal(t, freshEval, storedPipeline.Traits["source-freshness"].Evaluator)
	assert.Equal(t, 120, storedPipeline.Traits["source-freshness"].TTL)
	assert.Equal(t, 5, storedPipeline.Traits["source-freshness"].Timeout)

	// --- Verify run state round-trip ---
	runID := "kv-test-run-1"
	now := time.Now()
	run := types.RunState{
		RunID:      runID,
		PipelineID: "kv-roundtrip-test",
		Status:     types.RunPending,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
		Metadata:   map[string]interface{}{"env": "staging", "retries": 3},
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	storedRun, err := prov.GetRunState(ctx, runID)
	require.NoError(t, err)
	require.NotNil(t, storedRun)
	assert.Equal(t, runID, storedRun.RunID, "runId must round-trip")
	assert.Equal(t, "kv-roundtrip-test", storedRun.PipelineID, "pipelineId must round-trip")
	assert.Equal(t, types.RunPending, storedRun.Status, "status must round-trip")
	assert.Equal(t, 1, storedRun.Version, "version must round-trip")
	assert.Equal(t, "staging", storedRun.Metadata["env"], "metadata.env must round-trip")
	assert.EqualValues(t, 3, storedRun.Metadata["retries"], "metadata.retries must round-trip")

	// --- Verify ListRuns includes the run ---
	runs, err := prov.ListRuns(ctx, "kv-roundtrip-test", 10)
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, runID, runs[0].RunID)
}

// ---------------------------------------------------------------------------
// Test 7: Event audit trail — verify events logged in correct order
// ---------------------------------------------------------------------------

func TestIntegration_EventAuditTrail(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))

	freshEval := writeEvaluator(t, evalDir, "check-fresh", `#!/bin/bash
echo '{"status":"PASS","value":{"ok":true}}'
`)
	depEval := writeEvaluator(t, evalDir, "check-dep", `#!/bin/bash
echo '{"status":"PASS","value":{"upstream":"done"}}'
`)

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "event-archetype",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
			{Type: "dependency", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "event-test",
		Archetype: "event-archetype",
		Traits: map[string]types.TraitConfig{
			"freshness":  {Evaluator: freshEval, Timeout: 5},
			"dependency": {Evaluator: depEval, Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, nil)
	ctx := context.Background()

	// Step 1: Evaluate pipeline (should produce trait + readiness events)
	result, err := eng.Evaluate(ctx, "event-test")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)

	// Check events so far: should have 2 TRAIT_EVALUATED + 1 READINESS_CHECKED
	events, err := prov.ListEvents(ctx, "event-test", 100)
	require.NoError(t, err)

	traitEvents := filterEventsByKind(events, types.EventTraitEvaluated)
	readinessEvents := filterEventsByKind(events, types.EventReadinessChecked)

	assert.Len(t, traitEvents, 2, "should have 2 TRAIT_EVALUATED events")
	assert.Len(t, readinessEvents, 1, "should have 1 READINESS_CHECKED event")

	// Verify trait events have correct fields
	traitTypes := map[string]bool{}
	for _, te := range traitEvents {
		assert.Equal(t, "event-test", te.PipelineID)
		assert.Equal(t, "PASS", te.Status)
		assert.NotZero(t, te.Timestamp)
		traitTypes[te.TraitType] = true
	}
	assert.True(t, traitTypes["freshness"], "should have freshness trait event")
	assert.True(t, traitTypes["dependency"], "should have dependency trait event")

	// Verify readiness event
	re := readinessEvents[0]
	assert.Equal(t, "event-test", re.PipelineID)
	assert.Equal(t, "READY", re.Status)
	assert.NotNil(t, re.Details)

	// Readiness event should come after trait events (or at least last)
	lastEvent := events[len(events)-1]
	assert.Equal(t, types.EventReadinessChecked, lastEvent.Kind,
		"READINESS_CHECKED should be the last event after trait evaluations")

	// Step 2: Simulate run lifecycle and manually append events (as handlers do)
	runID := "event-test-run-1"
	_ = prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: "event-test",
		RunID:      runID,
		Status:     string(types.RunPending),
		Message:    "run created",
		Timestamp:  time.Now(),
	})
	_ = prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventRunStateChanged,
		PipelineID: "event-test",
		RunID:      runID,
		Status:     string(types.RunTriggering),
		Message:    "trigger lock acquired",
		Timestamp:  time.Now(),
	})
	_ = prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventTriggerFired,
		PipelineID: "event-test",
		RunID:      runID,
		Status:     string(types.RunRunning),
		Message:    "HTTP trigger sent",
		Timestamp:  time.Now(),
	})
	_ = prov.AppendEvent(ctx, types.Event{
		Kind:       types.EventCallbackReceived,
		PipelineID: "event-test",
		RunID:      runID,
		Status:     string(types.RunCompleted),
		Message:    "callback received: success",
		Details:    map[string]interface{}{"rows_processed": 50000},
		Timestamp:  time.Now(),
	})

	// Verify full event trail
	allEvents, err := prov.ListEvents(ctx, "event-test", 100)
	require.NoError(t, err)

	// Expected order: 2x TRAIT_EVALUATED, READINESS_CHECKED, RUN_STATE_CHANGED(PENDING),
	// RUN_STATE_CHANGED(TRIGGERING), TRIGGER_FIRED, CALLBACK_RECEIVED
	assert.Len(t, allEvents, 7, "should have 7 total events")

	runEvents := filterEventsByKind(allEvents, types.EventRunStateChanged)
	assert.Len(t, runEvents, 2, "should have 2 RUN_STATE_CHANGED events")
	assert.Equal(t, "PENDING", runEvents[0].Status)
	assert.Equal(t, "TRIGGERING", runEvents[1].Status)

	triggerEvents := filterEventsByKind(allEvents, types.EventTriggerFired)
	assert.Len(t, triggerEvents, 1, "should have 1 TRIGGER_FIRED event")
	assert.Equal(t, runID, triggerEvents[0].RunID)

	callbackEvents := filterEventsByKind(allEvents, types.EventCallbackReceived)
	assert.Len(t, callbackEvents, 1, "should have 1 CALLBACK_RECEIVED event")
	assert.Equal(t, "COMPLETED", callbackEvents[0].Status)
	assert.EqualValues(t, 50000, callbackEvents[0].Details["rows_processed"])

	// Verify all events have the right pipeline ID
	for _, ev := range allEvents {
		assert.Equal(t, "event-test", ev.PipelineID, "all events should reference the pipeline")
	}

	// Verify all run-related events reference the run ID
	for _, ev := range append(runEvents, append(triggerEvents, callbackEvents...)...) {
		assert.Equal(t, runID, ev.RunID, "run events should reference the run ID")
	}
}

// ---------------------------------------------------------------------------
// Test 8: Run failure scenarios — command fails, callback failure, invalid transition
// ---------------------------------------------------------------------------

func TestIntegration_RunFailure_CommandFails(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))
	alertLog := filepath.Join(tmpDir, "alerts.log")

	passEval := writeEvaluator(t, evalDir, "check-pass", `#!/bin/bash
echo '{"status":"PASS","value":{"ok":true}}'
`)

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "simple",
		RequiredTraits: []types.TraitDefinition{
			{Type: "check", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "fail-cmd-test",
		Archetype: "simple",
		Traits: map[string]types.TraitConfig{
			"check": {Evaluator: passEval, Timeout: 5},
		},
		Trigger: &types.TriggerConfig{
			Type:    types.TriggerCommand,
			Command: &types.CommandTriggerConfig{Command: "exit 1"},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	dispatcher, err := alert.NewDispatcher([]types.AlertConfig{
		{Type: types.AlertFile, Path: alertLog},
	}, nil)
	require.NoError(t, err)

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, dispatcher.AlertFunc())
	ctx := context.Background()

	// Evaluate — should be READY
	result, err := eng.Evaluate(ctx, "fail-cmd-test")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)

	// Create run
	runID := "fail-cmd-run-1"
	run := types.RunState{
		RunID: runID, PipelineID: "fail-cmd-test",
		Status: types.RunPending, Version: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))
	_ = prov.AppendEvent(ctx, types.Event{
		Kind: types.EventRunStateChanged, PipelineID: "fail-cmd-test",
		RunID: runID, Status: "PENDING", Message: "run created", Timestamp: time.Now(),
	})

	// CAS to TRIGGERING
	run.Status = types.RunTriggering
	run.Version = 2
	run.UpdatedAt = time.Now()
	ok, err := prov.CompareAndSwapRunState(ctx, runID, 1, run)
	require.NoError(t, err)
	assert.True(t, ok)

	// Execute trigger — this should FAIL (exit 1)
	triggerErr := execCommand(ctx, pipeline.Trigger.Command.Command)
	assert.Error(t, triggerErr, "trigger command should fail with exit 1")

	// Since trigger failed, transition to FAILED
	require.NoError(t, lifecycle.Transition(types.RunTriggering, types.RunFailed))
	run.Status = types.RunFailed
	run.Version = 3
	run.UpdatedAt = time.Now()
	ok, err = prov.CompareAndSwapRunState(ctx, runID, 2, run)
	require.NoError(t, err)
	assert.True(t, ok)

	_ = prov.AppendEvent(ctx, types.Event{
		Kind: types.EventTriggerFailed, PipelineID: "fail-cmd-test",
		RunID: runID, Status: "FAILED", Message: "trigger command failed: exit status 1",
		Timestamp: time.Now(),
	})

	// Verify run is FAILED
	finalRun, err := prov.GetRunState(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, types.RunFailed, finalRun.Status)
	assert.Equal(t, 3, finalRun.Version)

	// Verify event trail shows the failure
	events, err := prov.ListEvents(ctx, "fail-cmd-test", 100)
	require.NoError(t, err)
	failEvents := filterEventsByKind(events, types.EventTriggerFailed)
	assert.Len(t, failEvents, 1, "should have 1 TRIGGER_FAILED event")
	assert.Contains(t, failEvents[0].Message, "trigger command failed")
}

func TestIntegration_RunFailure_CallbackReportsFailure(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Create a run that's in RUNNING state (awaiting callback)
	runID := "callback-fail-run"
	run := types.RunState{
		RunID: runID, PipelineID: "callback-test",
		Status: types.RunRunning, Version: 3,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	// Simulate orchestrator calling back with failure
	require.NoError(t, lifecycle.Transition(types.RunRunning, types.RunFailed))

	newRun := run
	newRun.Status = types.RunFailed
	newRun.Version = 4
	newRun.UpdatedAt = time.Now()
	newRun.Metadata = map[string]interface{}{
		"error":          "OOM killed",
		"exit_code":      137,
		"rows_processed": 0,
	}
	ok, err := prov.CompareAndSwapRunState(ctx, runID, 3, newRun)
	require.NoError(t, err)
	assert.True(t, ok)

	_ = prov.AppendEvent(ctx, types.Event{
		Kind: types.EventCallbackReceived, PipelineID: "callback-test",
		RunID: runID, Status: "FAILED",
		Message:   "callback received: failed",
		Details:   newRun.Metadata,
		Timestamp: time.Now(),
	})

	// Verify final state
	final, err := prov.GetRunState(ctx, runID)
	require.NoError(t, err)
	assert.Equal(t, types.RunFailed, final.Status)
	assert.Equal(t, 4, final.Version)
	assert.Equal(t, "OOM killed", final.Metadata["error"])
	assert.EqualValues(t, 137, final.Metadata["exit_code"])
	assert.EqualValues(t, 0, final.Metadata["rows_processed"])

	// Verify event
	events, err := prov.ListEvents(ctx, "callback-test", 100)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, types.EventCallbackReceived, events[0].Kind)
	assert.Equal(t, "FAILED", events[0].Status)
	assert.Equal(t, "OOM killed", events[0].Details["error"])
}

func TestIntegration_RunFailure_InvalidTransition(t *testing.T) {
	// Verify that invalid state transitions are rejected
	invalidTransitions := []struct {
		from types.RunStatus
		to   types.RunStatus
	}{
		{types.RunPending, types.RunCompleted},    // can't skip TRIGGERING
		{types.RunPending, types.RunRunning},      // can't skip TRIGGERING
		{types.RunCompleted, types.RunRunning},    // can't go backwards
		{types.RunFailed, types.RunRunning},       // can't restart failed
		{types.RunCancelled, types.RunTriggering}, // can't restart cancelled
	}

	for _, tt := range invalidTransitions {
		t.Run(fmt.Sprintf("%s_to_%s", tt.from, tt.to), func(t *testing.T) {
			err := lifecycle.Transition(tt.from, tt.to)
			assert.Error(t, err, "transition from %s to %s should be invalid", tt.from, tt.to)
		})
	}

	// Verify valid transitions succeed
	validTransitions := []struct {
		from types.RunStatus
		to   types.RunStatus
	}{
		{types.RunPending, types.RunTriggering},
		{types.RunTriggering, types.RunRunning},
		{types.RunTriggering, types.RunFailed},
		{types.RunRunning, types.RunCompleted},
		{types.RunRunning, types.RunFailed},
		{types.RunRunning, types.RunCancelled},
	}

	for _, tt := range validTransitions {
		t.Run(fmt.Sprintf("%s_to_%s_valid", tt.from, tt.to), func(t *testing.T) {
			err := lifecycle.Transition(tt.from, tt.to)
			assert.NoError(t, err, "transition from %s to %s should be valid", tt.from, tt.to)
		})
	}
}

func TestIntegration_RunFailure_HTTPTriggerError(t *testing.T) {
	// Mock server that returns 500
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer failServer.Close()

	triggerCfg := &types.HTTPTriggerConfig{
		Method: "POST",
		URL:    failServer.URL,
		Body:   `{"conf":{}}`,
	}

	// HTTP trigger should return error for 500 response
	err := execHTTPTrigger(context.Background(), triggerCfg)
	assert.Error(t, err, "HTTP trigger should fail on 500")
	assert.Contains(t, err.Error(), "500")
}

func TestIntegration_RunFailure_CASRejection(t *testing.T) {
	prov := testutil.NewMockProvider()
	ctx := context.Background()

	// Create a run already in RUNNING state at version 3
	run := types.RunState{
		RunID: "cas-reject-run", PipelineID: "cas-test",
		Status: types.RunRunning, Version: 3,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	require.NoError(t, prov.PutRunState(ctx, run))

	// Try to CAS with wrong version (stale read)
	staleRun := run
	staleRun.Status = types.RunCompleted
	staleRun.Version = 4

	// Try version 2 (stale) — should fail
	ok, err := prov.CompareAndSwapRunState(ctx, "cas-reject-run", 2, staleRun)
	require.NoError(t, err)
	assert.False(t, ok, "CAS with wrong version should fail")

	// Verify state unchanged
	current, err := prov.GetRunState(ctx, "cas-reject-run")
	require.NoError(t, err)
	assert.Equal(t, types.RunRunning, current.Status, "state should be unchanged after failed CAS")
	assert.Equal(t, 3, current.Version)

	// Try correct version — should succeed
	ok, err = prov.CompareAndSwapRunState(ctx, "cas-reject-run", 3, staleRun)
	require.NoError(t, err)
	assert.True(t, ok, "CAS with correct version should succeed")

	current, err = prov.GetRunState(ctx, "cas-reject-run")
	require.NoError(t, err)
	assert.Equal(t, types.RunCompleted, current.Status)
	assert.Equal(t, 4, current.Version)
}

// ---------------------------------------------------------------------------
// Test 13: Event trail for evaluation failure — verify alert events logged
// ---------------------------------------------------------------------------

func TestIntegration_EventTrail_EvaluationFailure(t *testing.T) {
	tmpDir := t.TempDir()
	evalDir := filepath.Join(tmpDir, "evaluators")
	require.NoError(t, os.MkdirAll(evalDir, 0o755))

	failEval := writeEvaluator(t, evalDir, "check-fail", `#!/bin/bash
echo '{"status":"FAIL","value":{"lag":9999},"reason":"source is stale"}'
`)
	passEval := writeEvaluator(t, evalDir, "check-pass", `#!/bin/bash
echo '{"status":"PASS","value":{"ok":true}}'
`)

	reg := archetype.NewRegistry()
	require.NoError(t, reg.Register(&types.Archetype{
		Name: "fail-event-archetype",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
			{Type: "quality", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}))

	prov := testutil.NewMockProvider()
	pipeline := types.PipelineConfig{
		Name:      "fail-event-test",
		Archetype: "fail-event-archetype",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: failEval, Timeout: 5},
			"quality":   {Evaluator: passEval, Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	var capturedAlerts []types.Alert
	var alertMu sync.Mutex
	alertFn := func(a types.Alert) {
		alertMu.Lock()
		defer alertMu.Unlock()
		capturedAlerts = append(capturedAlerts, a)
	}

	runner := evaluator.NewRunner()
	eng := engine.New(prov, reg, runner, alertFn)
	ctx := context.Background()

	// Evaluate — should be NOT_READY
	result, err := eng.Evaluate(ctx, "fail-event-test")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)

	// Check events
	events, err := prov.ListEvents(ctx, "fail-event-test", 100)
	require.NoError(t, err)

	// Should have: 2 TRAIT_EVALUATED + 1 READINESS_CHECKED
	traitEvts := filterEventsByKind(events, types.EventTraitEvaluated)
	assert.Len(t, traitEvts, 2)

	// Find the failing trait event
	var failTraitEvt *types.Event
	for i, te := range traitEvts {
		if te.TraitType == "freshness" {
			failTraitEvt = &traitEvts[i]
		}
	}
	require.NotNil(t, failTraitEvt)
	assert.Equal(t, "FAIL", failTraitEvt.Status)
	assert.Contains(t, failTraitEvt.Message, "source is stale")

	readinessEvts := filterEventsByKind(events, types.EventReadinessChecked)
	assert.Len(t, readinessEvts, 1)
	assert.Equal(t, "NOT_READY", readinessEvts[0].Status)

	// Verify alerts fired
	alertMu.Lock()
	defer alertMu.Unlock()
	assert.NotEmpty(t, capturedAlerts, "alerts should have fired for failing trait")

	// Should have warning for failing trait + overall NOT_READY
	hasTraitAlert := false
	hasReadinessAlert := false
	for _, a := range capturedAlerts {
		if containsAll(a.Message, "freshness", "fail-event-test") {
			hasTraitAlert = true
		}
		if containsAll(a.Message, "fail-event-test", "blocked by") {
			hasReadinessAlert = true
		}
	}
	assert.True(t, hasTraitAlert, "should have alert for freshness failure")
	assert.True(t, hasReadinessAlert, "should have alert for NOT_READY pipeline")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func filterEventsByKind(events []types.Event, kind types.EventKind) []types.Event {
	var result []types.Event
	for _, e := range events {
		if e.Kind == kind {
			result = append(result, e)
		}
	}
	return result
}

func containsAll(s string, substrs ...string) bool {
	for _, sub := range substrs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func execCommand(ctx context.Context, command string) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	return cmd.Run()
}

func execHTTPTrigger(ctx context.Context, trigger *types.HTTPTriggerConfig) error {
	method := trigger.Method
	if method == "" {
		method = http.MethodPost
	}

	var bodyReader *os.File
	if trigger.Body != "" {
		tmp, err := os.CreateTemp("", "trigger-body-*")
		if err != nil {
			return err
		}
		defer func() {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}()
		if _, err := tmp.WriteString(trigger.Body); err != nil {
			return err
		}
		if _, err := tmp.Seek(0, 0); err != nil {
			return err
		}
		bodyReader = tmp
	}

	req, err := http.NewRequestWithContext(ctx, method, trigger.URL, bodyReader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range trigger.Headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("trigger returned %d", resp.StatusCode)
	}
	return nil
}
