// Package testutil provides shared test utilities for Interlock.
package testutil

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// Compile-time interface satisfaction check.
var _ provider.Provider = (*MockProvider)(nil)

// MockProvider is an in-memory Provider implementation for testing.
type MockProvider struct {
	// Errors enables per-method error injection. Set Errors["MethodName"] before
	// running the test; the named method will return that error immediately.
	Errors map[string]error

	mu           sync.Mutex
	pipelines    map[string]types.PipelineConfig
	traits       map[string]types.TraitEvaluation
	traitHistory map[string][]types.TraitEvaluation // key: "pipelineID:traitType"
	runs         map[string]types.RunState
	runIndex     map[string][]string
	readiness    map[string]types.ReadinessResult
	events       []types.Event
	runLogs      map[string]types.RunLogEntry // key: "pipelineID:date:scheduleID"
	locks        map[string]string
	reruns       map[string]types.RerunRecord
	cascades     []cascadeMarker
	lateArrivals []types.LateArrival
	replays      map[string]types.ReplayRequest // key: "pipelineID:date:scheduleID"
	alerts       []types.Alert
	evalSessions map[string]types.EvaluationSession // key: sessionID
	dependencies map[string]map[string]bool         // key: upstreamID -> set of downstreamIDs
	sensors      map[string]types.SensorData        // key: "pipelineID:sensorType"
	quarantines  map[string]types.QuarantineRecord  // key: "pipelineID:date:hour"

	pollCount atomic.Int64 // incremented on each ListPipelines call
}

type cascadeMarker struct {
	PipelineID string
	ScheduleID string
	Date       string
	Source     string
}

// NewMockProvider creates a new in-memory mock provider.
func NewMockProvider() *MockProvider {
	return &MockProvider{
		Errors:       make(map[string]error),
		pipelines:    make(map[string]types.PipelineConfig),
		traits:       make(map[string]types.TraitEvaluation),
		traitHistory: make(map[string][]types.TraitEvaluation),
		runs:         make(map[string]types.RunState),
		runIndex:     make(map[string][]string),
		readiness:    make(map[string]types.ReadinessResult),
		runLogs:      make(map[string]types.RunLogEntry),
		locks:        make(map[string]string),
		reruns:       make(map[string]types.RerunRecord),
		replays:      make(map[string]types.ReplayRequest),
		evalSessions: make(map[string]types.EvaluationSession),
		dependencies: make(map[string]map[string]bool),
		sensors:      make(map[string]types.SensorData),
		quarantines:  make(map[string]types.QuarantineRecord),
	}
}

func (m *MockProvider) RegisterPipeline(_ context.Context, config types.PipelineConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["RegisterPipeline"]; err != nil {
		return err
	}
	m.pipelines[config.Name] = config
	// Sync dependency index.
	for _, upstream := range provider.ExtractUpstreams(&config) {
		if m.dependencies[upstream] == nil {
			m.dependencies[upstream] = make(map[string]bool)
		}
		m.dependencies[upstream][config.Name] = true
	}
	return nil
}

func (m *MockProvider) GetPipeline(_ context.Context, id string) (*types.PipelineConfig, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetPipeline"]; err != nil {
		return nil, err
	}
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
	if err := m.Errors["ListPipelines"]; err != nil {
		return nil, err
	}
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
	if err := m.Errors["DeletePipeline"]; err != nil {
		return err
	}
	config, ok := m.pipelines[id]
	if ok {
		for _, upstream := range provider.ExtractUpstreams(&config) {
			if deps, exists := m.dependencies[upstream]; exists {
				delete(deps, id)
			}
		}
	}
	delete(m.pipelines, id)
	return nil
}

func (m *MockProvider) PutTrait(_ context.Context, pipelineID string, trait types.TraitEvaluation, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutTrait"]; err != nil {
		return err
	}
	m.traits[pipelineID+":"+trait.TraitType] = trait
	// Dual-write history.
	histKey := pipelineID + ":" + trait.TraitType
	m.traitHistory[histKey] = append(m.traitHistory[histKey], trait)
	return nil
}

func (m *MockProvider) GetTraits(_ context.Context, pipelineID string) ([]types.TraitEvaluation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetTraits"]; err != nil {
		return nil, err
	}
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
	if err := m.Errors["GetTrait"]; err != nil {
		return nil, err
	}
	t, ok := m.traits[pipelineID+":"+traitType]
	if !ok {
		return nil, nil
	}
	return &t, nil
}

func (m *MockProvider) PutRunState(_ context.Context, run types.RunState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutRunState"]; err != nil {
		return err
	}
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
	if err := m.Errors["GetRunState"]; err != nil {
		return nil, err
	}
	r, ok := m.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}
	return &r, nil
}

func (m *MockProvider) ListRuns(_ context.Context, pipelineID string, limit int) ([]types.RunState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListRuns"]; err != nil {
		return nil, err
	}
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
	if err := m.Errors["CompareAndSwapRunState"]; err != nil {
		return false, err
	}
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
	if err := m.Errors["PutReadiness"]; err != nil {
		return err
	}
	m.readiness[result.PipelineID] = result
	return nil
}

func (m *MockProvider) GetReadiness(_ context.Context, pipelineID string) (*types.ReadinessResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetReadiness"]; err != nil {
		return nil, err
	}
	r, ok := m.readiness[pipelineID]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *MockProvider) AppendEvent(_ context.Context, event types.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["AppendEvent"]; err != nil {
		return err
	}
	m.events = append(m.events, event)
	return nil
}

func (m *MockProvider) ListEvents(_ context.Context, pipelineID string, limit int) ([]types.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListEvents"]; err != nil {
		return nil, err
	}
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

func (m *MockProvider) ReadEventsSince(_ context.Context, pipelineID, sinceID string, count int64) ([]types.EventRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ReadEventsSince"]; err != nil {
		return nil, err
	}

	// sinceID is a 1-based index formatted as "<idx>-0"; "0-0" means start from beginning
	startIdx := 0
	if sinceID != "" && sinceID != "0-0" {
		_, _ = fmt.Sscanf(sinceID, "%d-", &startIdx)
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
	if err := m.Errors["PutRunLog"]; err != nil {
		return err
	}
	if entry.ScheduleID == "" {
		entry.ScheduleID = types.DefaultScheduleID
	}
	m.runLogs[m.runLogKey(entry)] = entry
	return nil
}

func (m *MockProvider) GetRunLog(_ context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetRunLog"]; err != nil {
		return nil, err
	}
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
	if err := m.Errors["ListRunLogs"]; err != nil {
		return nil, err
	}
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

func (m *MockProvider) AcquireLock(_ context.Context, key string, _ time.Duration) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["AcquireLock"]; err != nil {
		return "", err
	}
	if m.locks[key] != "" {
		return "", nil
	}
	token := fmt.Sprintf("mock-%d", time.Now().UnixNano())
	m.locks[key] = token
	return token, nil
}

func (m *MockProvider) ReleaseLock(_ context.Context, key, token string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ReleaseLock"]; err != nil {
		return err
	}
	if m.locks[key] == token {
		delete(m.locks, key)
	}
	return nil
}

func (m *MockProvider) PutRerun(_ context.Context, record types.RerunRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutRerun"]; err != nil {
		return err
	}
	m.reruns[record.RerunID] = record
	return nil
}

func (m *MockProvider) GetRerun(_ context.Context, rerunID string) (*types.RerunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetRerun"]; err != nil {
		return nil, err
	}
	r, ok := m.reruns[rerunID]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *MockProvider) ListReruns(_ context.Context, pipelineID string, limit int) ([]types.RerunRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListReruns"]; err != nil {
		return nil, err
	}
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
	if err := m.Errors["ListAllReruns"]; err != nil {
		return nil, err
	}
	var result []types.RerunRecord
	for _, r := range m.reruns {
		result = append(result, r)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

func (m *MockProvider) WriteCascadeMarker(_ context.Context, pipelineID, scheduleID, date, source string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["WriteCascadeMarker"]; err != nil {
		return err
	}
	m.cascades = append(m.cascades, cascadeMarker{
		PipelineID: pipelineID,
		ScheduleID: scheduleID,
		Date:       date,
		Source:     source,
	})
	return nil
}

func (m *MockProvider) PutLateArrival(_ context.Context, entry types.LateArrival) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutLateArrival"]; err != nil {
		return err
	}
	m.lateArrivals = append(m.lateArrivals, entry)
	return nil
}

func (m *MockProvider) ListLateArrivals(_ context.Context, pipelineID, date, scheduleID string) ([]types.LateArrival, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListLateArrivals"]; err != nil {
		return nil, err
	}
	var result []types.LateArrival
	for _, la := range m.lateArrivals {
		if la.PipelineID == pipelineID && la.Date == date && la.ScheduleID == scheduleID {
			result = append(result, la)
		}
	}
	return result, nil
}

func (m *MockProvider) PutReplay(_ context.Context, entry types.ReplayRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutReplay"]; err != nil {
		return err
	}
	key := entry.PipelineID + ":" + entry.Date + ":" + entry.ScheduleID
	m.replays[key] = entry
	return nil
}

func (m *MockProvider) GetReplay(_ context.Context, pipelineID, date, scheduleID string) (*types.ReplayRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetReplay"]; err != nil {
		return nil, err
	}
	key := pipelineID + ":" + date + ":" + scheduleID
	r, ok := m.replays[key]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *MockProvider) ListReplays(_ context.Context, limit int) ([]types.ReplayRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListReplays"]; err != nil {
		return nil, err
	}
	var result []types.ReplayRequest
	for _, r := range m.replays {
		result = append(result, r)
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

// CascadeMarkers returns a copy of all stored cascade markers (test helper).
func (m *MockProvider) CascadeMarkers() []cascadeMarker {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]cascadeMarker, len(m.cascades))
	copy(out, m.cascades)
	return out
}

// --- AlertStore ---

func (m *MockProvider) PutAlert(_ context.Context, alert types.Alert) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutAlert"]; err != nil {
		return err
	}
	if alert.AlertID == "" {
		alert.AlertID = fmt.Sprintf("%d", alert.Timestamp.UnixMilli())
	}
	m.alerts = append(m.alerts, alert)
	return nil
}

func (m *MockProvider) ListAlerts(_ context.Context, pipelineID string, limit int) ([]types.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListAlerts"]; err != nil {
		return nil, err
	}
	var result []types.Alert
	// Iterate in reverse for newest first.
	for i := len(m.alerts) - 1; i >= 0; i-- {
		if m.alerts[i].PipelineID == pipelineID {
			result = append(result, m.alerts[i])
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *MockProvider) ListAllAlerts(_ context.Context, limit int) ([]types.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListAllAlerts"]; err != nil {
		return nil, err
	}
	var result []types.Alert
	for i := len(m.alerts) - 1; i >= 0; i-- {
		result = append(result, m.alerts[i])
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

// --- TraitHistoryStore ---

func (m *MockProvider) ListTraitHistory(_ context.Context, pipelineID, traitType string, limit int) ([]types.TraitEvaluation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListTraitHistory"]; err != nil {
		return nil, err
	}
	histKey := pipelineID + ":" + traitType
	history := m.traitHistory[histKey]
	// Return newest first.
	var result []types.TraitEvaluation
	for i := len(history) - 1; i >= 0; i-- {
		result = append(result, history[i])
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result, nil
}

// --- EvaluationSessionStore ---

func (m *MockProvider) PutEvaluationSession(_ context.Context, session types.EvaluationSession) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutEvaluationSession"]; err != nil {
		return err
	}
	m.evalSessions[session.SessionID] = session
	return nil
}

func (m *MockProvider) GetEvaluationSession(_ context.Context, sessionID string) (*types.EvaluationSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetEvaluationSession"]; err != nil {
		return nil, err
	}
	s, ok := m.evalSessions[sessionID]
	if !ok {
		return nil, nil
	}
	return &s, nil
}

func (m *MockProvider) ListEvaluationSessions(_ context.Context, pipelineID string, limit int) ([]types.EvaluationSession, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListEvaluationSessions"]; err != nil {
		return nil, err
	}
	var result []types.EvaluationSession
	for _, s := range m.evalSessions {
		if s.PipelineID == pipelineID {
			result = append(result, s)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

// --- DependencyStore ---

func (m *MockProvider) PutDependency(_ context.Context, upstreamID, downstreamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutDependency"]; err != nil {
		return err
	}
	if m.dependencies[upstreamID] == nil {
		m.dependencies[upstreamID] = make(map[string]bool)
	}
	m.dependencies[upstreamID][downstreamID] = true
	return nil
}

func (m *MockProvider) RemoveDependency(_ context.Context, upstreamID, downstreamID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["RemoveDependency"]; err != nil {
		return err
	}
	if deps, ok := m.dependencies[upstreamID]; ok {
		delete(deps, downstreamID)
	}
	return nil
}

func (m *MockProvider) ListDependents(_ context.Context, upstreamID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["ListDependents"]; err != nil {
		return nil, err
	}
	var result []string
	for id := range m.dependencies[upstreamID] {
		result = append(result, id)
	}
	return result, nil
}

// --- SensorStore ---

func (m *MockProvider) PutSensorData(_ context.Context, data types.SensorData) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutSensorData"]; err != nil {
		return err
	}
	m.sensors[data.PipelineID+":"+data.SensorType] = data
	return nil
}

func (m *MockProvider) GetSensorData(_ context.Context, pipelineID, sensorType string) (*types.SensorData, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetSensorData"]; err != nil {
		return nil, err
	}
	sd, ok := m.sensors[pipelineID+":"+sensorType]
	if !ok {
		return nil, nil
	}
	return &sd, nil
}

// --- ControlStore ---

func (m *MockProvider) GetControlStatus(_ context.Context, _ string) (*types.ControlRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetControlStatus"]; err != nil {
		return nil, err
	}
	return nil, nil
}

// --- QuarantineStore ---

func (m *MockProvider) PutQuarantineRecord(_ context.Context, record types.QuarantineRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["PutQuarantineRecord"]; err != nil {
		return err
	}
	key := record.PipelineID + ":" + record.Date + ":" + record.Hour
	m.quarantines[key] = record
	return nil
}

func (m *MockProvider) GetQuarantineRecord(_ context.Context, pipelineID, date, hour string) (*types.QuarantineRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["GetQuarantineRecord"]; err != nil {
		return nil, err
	}
	key := pipelineID + ":" + date + ":" + hour
	r, ok := m.quarantines[key]
	if !ok {
		return nil, nil
	}
	return &r, nil
}

func (m *MockProvider) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["Start"]; err != nil {
		return err
	}
	return nil
}

func (m *MockProvider) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["Stop"]; err != nil {
		return err
	}
	return nil
}

func (m *MockProvider) Ping(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.Errors["Ping"]; err != nil {
		return err
	}
	return nil
}

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
