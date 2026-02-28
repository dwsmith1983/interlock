// Package provider defines the storage backend interface for Interlock.
package provider

import (
	"context"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PipelineStore handles pipeline configuration storage.
type PipelineStore interface {
	RegisterPipeline(ctx context.Context, config types.PipelineConfig) error
	GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error)
	ListPipelines(ctx context.Context) ([]types.PipelineConfig, error)
	DeletePipeline(ctx context.Context, id string) error
}

// TraitStore handles trait evaluation storage.
type TraitStore interface {
	PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error
	GetTraits(ctx context.Context, pipelineID string) ([]types.TraitEvaluation, error)
	GetTrait(ctx context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error)
}

// RunStore handles run state storage.
type RunStore interface {
	PutRunState(ctx context.Context, run types.RunState) error
	GetRunState(ctx context.Context, runID string) (*types.RunState, error)
	ListRuns(ctx context.Context, pipelineID string, limit int) ([]types.RunState, error)
	CompareAndSwapRunState(ctx context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error)
}

// EventStore handles event log storage.
type EventStore interface {
	AppendEvent(ctx context.Context, event types.Event) error
	ListEvents(ctx context.Context, pipelineID string, limit int) ([]types.Event, error)
	ReadEventsSince(ctx context.Context, pipelineID string, sinceID string, count int64) ([]types.EventRecord, error)
}

// RunLogStore handles run log storage.
type RunLogStore interface {
	PutRunLog(ctx context.Context, entry types.RunLogEntry) error
	GetRunLog(ctx context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error)
	ListRunLogs(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error)
}

// RerunStore handles rerun record storage.
type RerunStore interface {
	PutRerun(ctx context.Context, record types.RerunRecord) error
	GetRerun(ctx context.Context, rerunID string) (*types.RerunRecord, error)
	ListReruns(ctx context.Context, pipelineID string, limit int) ([]types.RerunRecord, error)
	ListAllReruns(ctx context.Context, limit int) ([]types.RerunRecord, error)
}

// Locker handles distributed locking.
//
// AcquireLock returns an owner token on success (non-empty string) or ""
// if the lock was not acquired. ReleaseLock only deletes the lock if the
// provided token matches the current owner, preventing a process from
// releasing a lock it no longer holds (e.g. after a GC pause + TTL expiry).
type Locker interface {
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (string, error)
	ReleaseLock(ctx context.Context, key string, token string) error
}

// CascadeStore handles downstream pipeline notification.
type CascadeStore interface {
	WriteCascadeMarker(ctx context.Context, pipelineID, scheduleID, date, source string) error
}

// LateArrivalStore handles late-arrival tracking.
type LateArrivalStore interface {
	PutLateArrival(ctx context.Context, entry types.LateArrival) error
	ListLateArrivals(ctx context.Context, pipelineID, date, scheduleID string) ([]types.LateArrival, error)
}

// ReplayStore handles manual replay requests.
type ReplayStore interface {
	PutReplay(ctx context.Context, entry types.ReplayRequest) error
	GetReplay(ctx context.Context, pipelineID, date, scheduleID string) (*types.ReplayRequest, error)
	ListReplays(ctx context.Context, limit int) ([]types.ReplayRequest, error)
}

// AlertStore handles persisted alert storage.
type AlertStore interface {
	PutAlert(ctx context.Context, alert types.Alert) error
	ListAlerts(ctx context.Context, pipelineID string, limit int) ([]types.Alert, error)
	ListAllAlerts(ctx context.Context, limit int) ([]types.Alert, error)
}

// TraitHistoryStore provides access to historical trait evaluations.
type TraitHistoryStore interface {
	ListTraitHistory(ctx context.Context, pipelineID, traitType string, limit int) ([]types.TraitEvaluation, error)
}

// EvaluationSessionStore tracks evaluation session lifecycle.
type EvaluationSessionStore interface {
	PutEvaluationSession(ctx context.Context, session types.EvaluationSession) error
	GetEvaluationSession(ctx context.Context, sessionID string) (*types.EvaluationSession, error)
	ListEvaluationSessions(ctx context.Context, pipelineID string, limit int) ([]types.EvaluationSession, error)
}

// DependencyStore maintains a pipeline dependency index for O(1) downstream lookup.
type DependencyStore interface {
	PutDependency(ctx context.Context, upstreamID, downstreamID string) error
	RemoveDependency(ctx context.Context, upstreamID, downstreamID string) error
	ListDependents(ctx context.Context, upstreamID string) ([]string, error)
}

// SensorStore handles externally-landed sensor data.
type SensorStore interface {
	PutSensorData(ctx context.Context, data types.SensorData) error
	GetSensorData(ctx context.Context, pipelineID, sensorType string) (*types.SensorData, error)
}

// QuarantineStore handles quarantine record storage. Quarantine records are
// written by ETL jobs when bad records are separated during processing.
type QuarantineStore interface {
	PutQuarantineRecord(ctx context.Context, record types.QuarantineRecord) error
	GetQuarantineRecord(ctx context.Context, pipelineID, date, hour string) (*types.QuarantineRecord, error)
}

// ControlStore provides read access to CONTROL# pipeline health records
// for circuit-breaker gating.
type ControlStore interface {
	GetControlStatus(ctx context.Context, pipelineID string) (*types.ControlRecord, error)
}

// ReadinessStore handles readiness caching.
type ReadinessStore interface {
	PutReadiness(ctx context.Context, result types.ReadinessResult) error
	GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error)
}

// Provider is the storage backend interface. Phase 1 implements Redis/Valkey;
// future phases add DynamoDB, etcd, Firestore, and Cosmos.
type Provider interface {
	PipelineStore
	TraitStore
	RunStore
	EventStore
	RunLogStore
	RerunStore
	CascadeStore
	LateArrivalStore
	ReplayStore
	Locker
	AlertStore
	TraitHistoryStore
	EvaluationSessionStore
	DependencyStore
	SensorStore
	QuarantineStore
	ReadinessStore
	ControlStore

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Ping(ctx context.Context) error
}

// ---------------------------------------------------------------------------
// Consumer-scoped composite interfaces
//
// These document exactly which sub-interfaces each major consumer needs,
// enabling focused mocks and making dependency requirements explicit.
// ---------------------------------------------------------------------------

// OrchestratorProvider is the subset needed by the orchestrator Lambda.
type OrchestratorProvider interface {
	PipelineStore
	RunLogStore
	EventStore
	Locker
	CascadeStore
	DependencyStore
	QuarantineStore
	ControlStore
}

// TriggerProvider is the subset needed by the trigger Lambda.
type TriggerProvider interface {
	RunStore
	RunLogStore
	EventStore
}

// WatchdogProvider is the subset needed by the watchdog.
type WatchdogProvider interface {
	PipelineStore
	RunLogStore
	EventStore
	Locker
}

// EngineProvider is the subset needed by the evaluation engine.
type EngineProvider interface {
	PipelineStore
	TraitStore
	EventStore
	EvaluationSessionStore
	ReadinessStore
}

// ExtractUpstreams scans a pipeline's trait configs for upstreamPipeline values,
// returning a deduplicated list of upstream pipeline IDs.
func ExtractUpstreams(config *types.PipelineConfig) []string {
	if config == nil {
		return nil
	}
	seen := make(map[string]bool)
	var upstreams []string
	for _, tc := range config.Traits {
		if tc.Config == nil {
			continue
		}
		v, ok := tc.Config["upstreamPipeline"]
		if !ok {
			continue
		}
		id, ok := v.(string)
		if !ok || id == "" {
			continue
		}
		if !seen[id] {
			seen[id] = true
			upstreams = append(upstreams, id)
		}
	}
	return upstreams
}
