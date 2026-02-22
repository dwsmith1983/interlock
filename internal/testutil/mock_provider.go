// Package testutil provides shared test utilities for Interlock.
package testutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
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
	runLogs   map[string]types.RunLogEntry // key: "pipelineID:date:scheduleID"
	locks     map[string]bool
	reruns    map[string]types.RerunRecord

	pollCount atomic.Int64 // incremented on each ListPipelines call
}

// NewMockProvider creates a new in-memory mock provider.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		pipelines: make(map[string]types.PipelineConfig),
		traits:    make(map[string]types.TraitEvaluation),
		runs:      make(map[string]types.RunState),
		runIndex:  make(map[string][]string),
		readiness: make(map[string]types.ReadinessResult),
		runLogs:   make(map[string]types.RunLogEntry),
		locks:     make(map[string]bool),
		reruns:    make(map[string]types.RerunRecord),
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
	m.pollCount.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.PipelineConfig
	for _, p := range m.pipelines {
		result = append(result, p)
	}
	return result, nil
}

// PollCount returns the number of times ListPipelines has been called.
// Useful for waiting until the watcher has completed at least N poll cycles.
func (m *MockProvider) PollCount() int64 {
	return m.pollCount.Load()
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
	if limit <= 0 || limit > len(ids) {
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

func (m *MockProvider) ReadEventsSince(_ context.Context, pipelineID string, sinceID string, count int64) ([]types.EventRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// sinceID is a 1-based index formatted as "<idx>-0"; "0-0" means start from beginning
	startIdx := 0
	if sinceID != "" && sinceID != "0-0" {
		fmt.Sscanf(sinceID, "%d-", &startIdx)
	}

	var records []types.EventRecord
	idx := 0
	for _, e := range m.events {
		if e.PipelineID == pipelineID {
			idx++
			if idx <= startIdx {
				continue
			}
			records = append(records, types.EventRecord{
				StreamID: fmt.Sprintf("%d-0", idx),
				Event:    e,
			})
			if int64(len(records)) >= count {
				break
			}
		}
	}
	return records, nil
}

func (m *MockProvider) runLogKey(entry types.RunLogEntry) string {
	sid := entry.ScheduleID
	if sid == "" {
		sid = types.DefaultScheduleID
	}
	return entry.PipelineID + ":" + entry.Date + ":" + sid
}

func (m *MockProvider) PutRunLog(_ context.Context, entry types.RunLogEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry.ScheduleID == "" {
		entry.ScheduleID = types.DefaultScheduleID
	}
	m.runLogs[m.runLogKey(entry)] = entry
	return nil
}

func (m *MockProvider) GetRunLog(_ context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if scheduleID == "" {
		scheduleID = types.DefaultScheduleID
	}
	key := pipelineID + ":" + date + ":" + scheduleID
	e, ok := m.runLogs[key]
	if !ok {
		return nil, nil
	}
	return &e, nil
}

func (m *MockProvider) ListRunLogs(_ context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := pipelineID + ":"
	var entries []types.RunLogEntry
	for k, v := range m.runLogs {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			entries = append(entries, v)
			if limit > 0 && len(entries) >= limit {
				break
			}
		}
	}
	return entries, nil
}

func (m *MockProvider) AcquireLock(_ context.Context, key string, _ time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks[key] {
		return false, nil
	}
	m.locks[key] = true
	return true, nil
}

func (m *MockProvider) ReleaseLock(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.locks, key)
	return nil
}

func (m *MockProvider) PutRerun(_ context.Context, record types.RerunRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reruns[record.RerunID] = record
	return nil
}

func (m *MockProvider) GetRerun(_ context.Context, rerunID string) (*types.RerunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.reruns[rerunID]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *MockProvider) ListReruns(_ context.Context, pipelineID string, limit int) ([]types.RerunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.RerunRecord
	for _, r := range m.reruns {
		if r.PipelineID == pipelineID {
			result = append(result, r)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MockProvider) ListAllReruns(_ context.Context, limit int) ([]types.RerunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []types.RerunRecord
	for _, r := range m.reruns {
		result = append(result, r)
		if limit > 0 && len(result) >= limit {
			break
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

// Reruns returns a copy of all stored rerun records (test helper).
func (m *MockProvider) Reruns() []types.RerunRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []types.RerunRecord
	for _, r := range m.reruns {
		out = append(out, r)
	}
	return out
}

// DeleteTrait removes a trait entry directly (test helper for simulating TTL expiry).
func (m *MockProvider) DeleteTrait(pipelineID, traitType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.traits, pipelineID+":"+traitType)
}
