// Package provider defines the storage backend interface for Interlock.
package provider

import (
	"context"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// Provider is the storage backend interface. Phase 1 implements Redis/Valkey;
// future phases add DynamoDB, etcd, Firestore, and Cosmos.
type Provider interface {
	// Pipeline registration
	RegisterPipeline(ctx context.Context, config types.PipelineConfig) error
	GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error)
	ListPipelines(ctx context.Context) ([]types.PipelineConfig, error)
	DeletePipeline(ctx context.Context, id string) error

	// Trait state (with TTL)
	PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error
	GetTraits(ctx context.Context, pipelineID string) ([]types.TraitEvaluation, error)
	GetTrait(ctx context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error)

	// Run state (with CAS for locking)
	PutRunState(ctx context.Context, run types.RunState) error
	GetRunState(ctx context.Context, runID string) (*types.RunState, error)
	ListRuns(ctx context.Context, pipelineID string, limit int) ([]types.RunState, error)
	CompareAndSwapRunState(ctx context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error)

	// Readiness caching
	PutReadiness(ctx context.Context, result types.ReadinessResult) error
	GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error)

	// Event log — append-only audit trail
	AppendEvent(ctx context.Context, event types.Event) error
	ListEvents(ctx context.Context, pipelineID string, limit int) ([]types.Event, error)

	// Run log — durable per-pipeline-per-date tracking
	PutRunLog(ctx context.Context, entry types.RunLogEntry) error
	GetRunLog(ctx context.Context, pipelineID, date string) (*types.RunLogEntry, error)
	ListRunLogs(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error)

	// Rerun ledger — tracks reruns triggered by late data or corrections
	PutRerun(ctx context.Context, record types.RerunRecord) error
	GetRerun(ctx context.Context, rerunID string) (*types.RerunRecord, error)
	ListReruns(ctx context.Context, pipelineID string, limit int) ([]types.RerunRecord, error)
	ListAllReruns(ctx context.Context, limit int) ([]types.RerunRecord, error)

	// Distributed locking for watcher coordination
	AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string) error

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Ping(ctx context.Context) error
}
