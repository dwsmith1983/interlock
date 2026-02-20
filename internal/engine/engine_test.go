package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/interlock-systems/interlock/internal/archetype"
	"github.com/interlock-systems/interlock/internal/evaluator"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProvider implements the Provider interface for testing.
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

func TestEngine_EvaluateAllPass(t *testing.T) {
	prov := newMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultConfig: map[string]interface{}{"maxLagSeconds": 60}, DefaultTTL: 120, DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner(nil)
	var alerts []types.Alert
	eng := New(prov, reg, runner, func(a types.Alert) { alerts = append(alerts, a) })

	result, err := eng.Evaluate(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)
	assert.Empty(t, result.Blocking)
	assert.Len(t, result.Traits, 1)
	assert.Equal(t, types.TraitPass, result.Traits[0].Status)
}

func TestEngine_EvaluateWithFailure(t *testing.T) {
	prov := newMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner(nil)
	eng := New(prov, reg, runner, nil)

	result, err := eng.Evaluate(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Contains(t, result.Blocking, "freshness")
}

func TestEngine_EvaluateNoEvaluator(t *testing.T) {
	prov := newMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
		// No traits configured = no evaluator
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner(nil)
	eng := New(prov, reg, runner, nil)

	result, err := eng.Evaluate(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Contains(t, result.Blocking, "freshness")
}

func TestEngine_CheckReadiness_StaleTrait(t *testing.T) {
	prov := newMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner(nil)
	eng := New(prov, reg, runner, nil)

	// No trait stored = STALE
	result, err := eng.CheckReadiness(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Len(t, result.Blocking, 1)
	assert.Contains(t, result.Blocking[0], "STALE")
}
