package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/engine"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProvider for server tests (same as engine test but in this package)
type mockProvider struct {
	mu        sync.Mutex
	pipelines map[string]types.PipelineConfig
	traits    map[string]types.TraitEvaluation
	runs      map[string]types.RunState
	readiness map[string]types.ReadinessResult
	events    []types.Event
}

func newMockProvider() *mockProvider {
	return &mockProvider{
		pipelines: make(map[string]types.PipelineConfig),
		traits:    make(map[string]types.TraitEvaluation),
		runs:      make(map[string]types.RunState),
		readiness: make(map[string]types.ReadinessResult),
	}
}

func (m *mockProvider) RegisterPipeline(_ context.Context, config types.PipelineConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pipelines[config.Name] = config
	return nil
}

func (m *mockProvider) GetPipeline(_ context.Context, id string) (*types.PipelineConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pipelines[id]
	if !ok {
		return nil, assert.AnError
	}
	return &p, nil
}

func (m *mockProvider) ListPipelines(_ context.Context) ([]types.PipelineConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.PipelineConfig
	for _, p := range m.pipelines {
		result = append(result, p)
	}
	return result, nil
}

func (m *mockProvider) DeletePipeline(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pipelines, id)
	return nil
}

func (m *mockProvider) PutTrait(_ context.Context, pipelineID string, trait types.TraitEvaluation, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traits[pipelineID+":"+trait.TraitType] = trait
	return nil
}

func (m *mockProvider) GetTraits(_ context.Context, pipelineID string) ([]types.TraitEvaluation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.TraitEvaluation
	for k, v := range m.traits {
		if len(k) > len(pipelineID) && k[:len(pipelineID)] == pipelineID {
			result = append(result, v)
		}
	}
	return result, nil
}

func (m *mockProvider) GetTrait(_ context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.traits[pipelineID+":"+traitType]
	if !ok {
		return nil, nil
	}
	return &t, nil
}

func (m *mockProvider) PutRunState(_ context.Context, run types.RunState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.RunID] = run
	return nil
}

func (m *mockProvider) GetRunState(_ context.Context, runID string) (*types.RunState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[runID]
	if !ok {
		return nil, assert.AnError
	}
	return &r, nil
}

func (m *mockProvider) ListRuns(_ context.Context, _ string, _ int) ([]types.RunState, error) {
	return nil, nil
}

func (m *mockProvider) CompareAndSwapRunState(_ context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current, ok := m.runs[runID]
	if !ok {
		return false, nil
	}
	if current.Version != expectedVersion {
		return false, nil
	}
	m.runs[runID] = newState
	return true, nil
}

func (m *mockProvider) PutReadiness(_ context.Context, result types.ReadinessResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readiness[result.PipelineID] = result
	return nil
}

func (m *mockProvider) GetReadiness(_ context.Context, pipelineID string) (*types.ReadinessResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.readiness[pipelineID]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *mockProvider) AppendEvent(_ context.Context, event types.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *mockProvider) ListEvents(_ context.Context, pipelineID string, limit int) ([]types.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.Event
	for _, e := range m.events {
		if e.PipelineID == pipelineID {
			result = append(result, e)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *mockProvider) Start(_ context.Context) error { return nil }
func (m *mockProvider) Stop(_ context.Context) error  { return nil }
func (m *mockProvider) Ping(_ context.Context) error  { return nil }

func setupTestServer(t *testing.T) (*httptest.Server, *mockProvider) {
	t.Helper()
	prov := newMockProvider()
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
