// Package testutil provides shared test utilities for Interlock.
package testutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// MockProvider is an in-memory Provider implementation for testing.
type MockProvider struct {
	mu        sync.Mutex
	pipelines map[string]types.PipelineConfig
	traits    map[string]types.TraitEvaluation
	runs      map[string]types.RunState
	runIndex  map[string][]string
	readiness map[string]types.ReadinessResult
	events    []types.Event
}

// NewMockProvider creates a new in-memory mock provider.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		pipelines: make(map[string]types.PipelineConfig),
		traits:    make(map[string]types.TraitEvaluation),
		runs:      make(map[string]types.RunState),
		runIndex:  make(map[string][]string),
		readiness: make(map[string]types.ReadinessResult),
	}
}

func (m *MockProvider) RegisterPipeline(_ context.Context, config types.PipelineConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pipelines[config.Name] = config
	return nil
}

func (m *MockProvider) GetPipeline(_ context.Context, id string) (*types.PipelineConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	return &p, nil
}

func (m *MockProvider) ListPipelines(_ context.Context) ([]types.PipelineConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.PipelineConfig
	for _, p := range m.pipelines {
		result = append(result, p)
	}
	return result, nil
}

func (m *MockProvider) DeletePipeline(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pipelines, id)
	return nil
}

func (m *MockProvider) PutTrait(_ context.Context, pipelineID string, trait types.TraitEvaluation, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traits[pipelineID+":"+trait.TraitType] = trait
	return nil
}

func (m *MockProvider) GetTraits(_ context.Context, pipelineID string) ([]types.TraitEvaluation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.TraitEvaluation
	prefix := pipelineID + ":"
	for k, v := range m.traits {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			result = append(result, v)
		}
	}
	return result, nil
}

func (m *MockProvider) GetTrait(_ context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.traits[pipelineID+":"+traitType]
	if !ok {
		return nil, nil
	}
	return &t, nil
}

func (m *MockProvider) PutRunState(_ context.Context, run types.RunState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, exists := m.runs[run.RunID]
	m.runs[run.RunID] = run
	if !exists {
		m.runIndex[run.PipelineID] = append([]string{run.RunID}, m.runIndex[run.PipelineID]...)
	}
	return nil
}

func (m *MockProvider) GetRunState(_ context.Context, runID string) (*types.RunState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}
	return &r, nil
}

func (m *MockProvider) ListRuns(_ context.Context, pipelineID string, limit int) ([]types.RunState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.runIndex[pipelineID]
	if limit > len(ids) {
		limit = len(ids)
	}
	var out []types.RunState
	for _, id := range ids[:limit] {
		out = append(out, m.runs[id])
	}
	return out, nil
}

func (m *MockProvider) CompareAndSwapRunState(_ context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error) {
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

func (m *MockProvider) PutReadiness(_ context.Context, result types.ReadinessResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readiness[result.PipelineID] = result
	return nil
}

func (m *MockProvider) GetReadiness(_ context.Context, pipelineID string) (*types.ReadinessResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.readiness[pipelineID]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *MockProvider) AppendEvent(_ context.Context, event types.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *MockProvider) ListEvents(_ context.Context, pipelineID string, limit int) ([]types.Event, error) {
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

func (m *MockProvider) Start(_ context.Context) error { return nil }
func (m *MockProvider) Stop(_ context.Context) error  { return nil }
func (m *MockProvider) Ping(_ context.Context) error  { return nil }

// Events returns a copy of all stored events (test helper).
func (m *MockProvider) Events() []types.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]types.Event, len(m.events))
	copy(out, m.events)
	return out
}

// DeleteTrait removes a trait entry directly (test helper for simulating TTL expiry).
func (m *MockProvider) DeleteTrait(pipelineID, traitType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.traits, pipelineID+":"+traitType)
}
