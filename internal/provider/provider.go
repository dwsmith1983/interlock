// Package provider defines the storage backend interface for Interlock.
package provider

import (
	"context"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
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
type Locker interface {
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string) error
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
	Locker

	// Readiness caching
	PutReadiness(ctx context.Context, result types.ReadinessResult) error
	GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error)

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Ping(ctx context.Context) error
}
