---
title: Provider Interface
weight: 1
description: The Provider interface — 10 sub-interfaces covering storage, locking, and lifecycle.
---

The `Provider` interface (`internal/provider/provider.go`) is the storage abstraction layer. It composes 10 sub-interfaces plus readiness caching and lifecycle methods. Two implementations exist: Redis (local) and DynamoDB (AWS).

## Provider Interface

```go
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

    PutReadiness(ctx context.Context, result types.ReadinessResult) error
    GetReadiness(ctx context.Context, pipelineID string) (*types.ReadinessResult, error)

    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    Ping(ctx context.Context) error
}
```

## Sub-Interfaces

### PipelineStore

CRUD operations for pipeline configurations.

```go
type PipelineStore interface {
    RegisterPipeline(ctx context.Context, config types.PipelineConfig) error
    GetPipeline(ctx context.Context, id string) (*types.PipelineConfig, error)
    ListPipelines(ctx context.Context) ([]types.PipelineConfig, error)
    DeletePipeline(ctx context.Context, id string) error
}
```

### TraitStore

Stores trait evaluation results with TTL-based expiry.

```go
type TraitStore interface {
    PutTrait(ctx context.Context, pipelineID string, trait types.TraitEvaluation, ttl time.Duration) error
    GetTraits(ctx context.Context, pipelineID string) ([]types.TraitEvaluation, error)
    GetTrait(ctx context.Context, pipelineID, traitType string) (*types.TraitEvaluation, error)
}
```

- `PutTrait` stores a result with the given TTL
- `GetTraits` returns all non-expired traits for a pipeline
- `GetTrait` returns a single non-expired trait (or nil)

### RunStore

Manages pipeline run lifecycle state with optimistic concurrency.

```go
type RunStore interface {
    PutRunState(ctx context.Context, run types.RunState) error
    GetRunState(ctx context.Context, runID string) (*types.RunState, error)
    ListRuns(ctx context.Context, pipelineID string, limit int) ([]types.RunState, error)
    CompareAndSwapRunState(ctx context.Context, runID string, expectedVersion int, newState types.RunState) (bool, error)
}
```

`CompareAndSwapRunState` returns `(true, nil)` on success, `(false, nil)` on version mismatch (concurrent modification), and `(false, err)` on infrastructure failure.

**Redis implementation**: Lua script that atomically checks version, increments, and updates.

**DynamoDB implementation**: `UpdateItem` with `ConditionExpression: #version = :expected`.

### EventStore

Append-only event log for audit and archival.

```go
type EventStore interface {
    AppendEvent(ctx context.Context, event types.Event) error
    ListEvents(ctx context.Context, pipelineID string, limit int) ([]types.Event, error)
    ReadEventsSince(ctx context.Context, pipelineID string, sinceID string, count int64) ([]types.EventRecord, error)
}
```

- `ReadEventsSince` supports cursor-based pagination for the archiver

**Redis implementation**: Redis Streams (`XADD`, `XRANGE`, `XREAD`).

**DynamoDB implementation**: Items with `SK = EVENT#{timestamp}#{uuid}`, queried with `begins_with`.

### RunLogStore

Durable per-pipeline, per-date, per-schedule run tracking.

```go
type RunLogStore interface {
    PutRunLog(ctx context.Context, entry types.RunLogEntry) error
    GetRunLog(ctx context.Context, pipelineID, date, scheduleID string) (*types.RunLogEntry, error)
    ListRunLogs(ctx context.Context, pipelineID string, limit int) ([]types.RunLogEntry, error)
}
```

The three-parameter `GetRunLog` supports multi-schedule dedup: each `(pipelineID, date, scheduleID)` combination has exactly one run log entry.

### RerunStore

Tracks downstream pipeline reruns triggered by late-arriving data or manual requests.

```go
type RerunStore interface {
    PutRerun(ctx context.Context, record types.RerunRecord) error
    GetRerun(ctx context.Context, rerunID string) (*types.RerunRecord, error)
    ListReruns(ctx context.Context, pipelineID string, limit int) ([]types.RerunRecord, error)
    ListAllReruns(ctx context.Context, limit int) ([]types.RerunRecord, error)
}
```

**DynamoDB**: Uses dual-write pattern — truth item at `RERUN#{id}/STATE` + list copy at `PIPELINE#{id}/RERUN#{id}`. `ListAllReruns` queries `GSI1PK = RERUNS`.

### CascadeStore

Writes markers that trigger downstream pipeline evaluations.

```go
type CascadeStore interface {
    WriteCascadeMarker(ctx context.Context, pipelineID, scheduleID, date, source string) error
}
```

On DynamoDB, this writes a `MARKER#` record that the DynamoDB Stream + `stream-router` picks up to start a Step Function execution.

### LateArrivalStore

Records detection of data arriving after a pipeline's evaluation window.

```go
type LateArrivalStore interface {
    PutLateArrival(ctx context.Context, entry types.LateArrival) error
    ListLateArrivals(ctx context.Context, pipelineID, date, scheduleID string) ([]types.LateArrival, error)
}
```

### ReplayStore

Manages manual replay requests (re-process a pipeline for a specific date/schedule).

```go
type ReplayStore interface {
    PutReplay(ctx context.Context, entry types.ReplayRequest) error
    GetReplay(ctx context.Context, pipelineID, date, scheduleID string) (*types.ReplayRequest, error)
    ListReplays(ctx context.Context, limit int) ([]types.ReplayRequest, error)
}
```

### Locker

Distributed locking for evaluation exclusivity.

```go
type Locker interface {
    AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error)
    ReleaseLock(ctx context.Context, key string) error
}
```

- `AcquireLock` returns `(true, nil)` if the lock was acquired, `(false, nil)` if already held
- Lock keys follow the format `eval:{pipelineID}:{scheduleID}`

**Redis**: `SETNX` with TTL expiry.

**DynamoDB**: Conditional `PutItem` with `attribute_not_exists(PK) OR #ttl < :now`.

## Implementation Comparison

| Feature | Redis | DynamoDB |
|---|---|---|
| CAS mechanism | Lua script | ConditionExpression |
| Lock mechanism | SETNX + TTL | Conditional PutItem |
| Event storage | Streams (XADD) | Items with timestamp SK |
| TTL enforcement | Native Redis TTL | TTL attribute + client-side filter |
| List queries | Sorted sets | Query with begins_with |
| Readiness cache | Hash + TTL | Item + TTL attribute |

## Conformance Tests

Both implementations pass the same 15 contract tests in `internal/provider/providertest/`. These tests verify:

- Pipeline CRUD operations
- Trait put/get with TTL expiry
- Run state CAS (including 10-goroutine race condition)
- Event append and cursor-based reads
- Run log three-parameter keying
- Lock acquisition and release semantics
- Rerun dual-write consistency
