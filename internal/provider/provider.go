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

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Ping(ctx context.Context) error
}
