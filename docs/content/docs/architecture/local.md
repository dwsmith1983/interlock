---
title: Local Architecture
weight: 3
description: Redis provider, watcher poll loop, Postgres archiver, and Docker Compose topology.
---

The local variant runs Interlock as a long-running process using Redis for state and an optional Postgres archiver for durable storage.

## Component Topology

```
┌──────────────┐     ┌───────────┐     ┌───────────────┐
│  Interlock   │────→│   Redis   │     │  PostgreSQL   │
│  (watcher)   │←────│   7.x     │     │  (archiver)   │
└──────┬───────┘     └───────────┘     └───────┬───────┘
       │                                       ↑
       │         ┌──────────────┐              │
       ├────────→│  Evaluators  │              │
       │         │  (subprocess │              │
       │         │   or HTTP)   │              │
       │         └──────────────┘              │
       │                                       │
       │         ┌──────────────┐    ┌─────────┴───────┐
       └────────→│  Triggers    │    │    Archiver     │
                 │  (HTTP, cmd, │    │ (Redis→Postgres)│
                 │   etc.)      │    └─────────────────┘
                 └──────────────┘
```

## Watcher Loop

The watcher is a polling loop that drives the entire evaluation lifecycle locally. Each tick:

1. **List pipelines** — fetch all registered pipeline configs
2. **Check exclusions** — skip pipelines excluded by calendar, day-of-week, or specific date
3. **For each schedule** in each pipeline:
   - Check if the schedule window is active (`After` time reached in configured timezone)
   - **Acquire lock** — `eval:{pipelineID}:{scheduleID}` with computed TTL
   - **Check active run** — look up today's (and yesterday's) run log to see if a run is already in progress
   - **Handle active run** — if running, poll job status via `Runner.CheckStatus()`; transition to `COMPLETED` or `FAILED`
   - **Evaluate traits** — run all required + optional trait evaluators
   - **Check readiness** — apply the archetype's readiness rule
   - **Trigger pipeline** — if ready, CAS `PENDING → TRIGGERING → RUNNING`
   - **Check SLA** — fire alerts if evaluation or completion deadlines breached
   - **Release lock**

### Timing

| Parameter | Default | Configuration |
|---|---|---|
| Poll interval | 5m | `watcher.defaultInterval` or per-pipeline `watch.interval` |
| Trait timeout | 30s | `engine.defaultTimeout` or per-trait `timeout` |
| Lock TTL | computed | `(traitCount × maxTimeout) + 30s` buffer |
| Monitoring duration | 2h | `watch.monitoring.duration` |

## Redis Provider

Redis serves as the primary state store in local mode.

### Key Layout

| Data | Key Pattern | Structure |
|---|---|---|
| Pipeline config | `{prefix}:pipeline:{id}` | Hash |
| Trait result | `{prefix}:trait:{pipelineID}:{traitType}` | Hash + TTL |
| Run state | `{prefix}:run:{runID}` | Hash |
| Run index | `{prefix}:runs:{pipelineID}` | Sorted set |
| Run log | `{prefix}:runlog:{pipelineID}:{date}:{scheduleID}` | Hash |
| Run log index | `{prefix}:runlogs:{pipelineID}` | Sorted set |
| Event stream | `{prefix}:events:{pipelineID}` | Stream |
| Lock | `{prefix}:lock:{key}` | String + TTL |
| Readiness cache | `{prefix}:readiness:{pipelineID}` | Hash + TTL |

### Concurrency

- **Locks**: Redis `SETNX` with TTL for distributed locking
- **CAS**: Lua script that checks version, increments, and updates atomically
- **Event streams**: Redis Streams with `MAXLEN` trimming and `XTRIMMINID` retention

### Configuration

```yaml
redis:
  addr: localhost:6379
  password: ""
  db: 0
  keyPrefix: interlock
  readinessTTL: 1h
  retentionTTL: 168h   # 7 days
  runIndexLimit: 100
  eventStreamMax: 10000
```

## Postgres Archiver

The archiver runs as a background goroutine, periodically copying data from Redis to Postgres for durable, queryable storage.

### How It Works

1. Lists all pipelines from the provider
2. For each pipeline, reads events since the last cursor position
3. Batch-inserts events into Postgres (up to 500 per batch)
4. Upserts run states and run log entries
5. Updates cursor positions for incremental sync

### Configuration

```yaml
archiver:
  enabled: true
  interval: 5m
  dsn: postgres://user:pass@localhost:5432/interlock?sslmode=disable
```

### Postgres Schema

The archiver uses an `UpsertRun`, `UpsertRunLog`, and `InsertEvents` interface against the `Destination`:

- `runs` table — mirrors `RunState` with conflict resolution on `run_id`
- `run_logs` table — mirrors `RunLogEntry` with conflict resolution on `(pipeline_id, date, schedule_id)`
- `events` table — append-only event log
- `cursors` table — tracks per-pipeline, per-data-type stream positions

## Docker Compose

The local demo environment runs all components in Docker:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: interlock
      POSTGRES_USER: interlock
      POSTGRES_PASSWORD: interlock
    ports: ["5432:5432"]

  interlock:
    build:
      context: ../..        # repo root
    depends_on: [redis, postgres]
    volumes:
      - ./pipelines:/etc/interlock/pipelines
      - ./archetypes:/etc/interlock/archetypes
      - ./evaluators:/etc/interlock/evaluators
```

Build context is the repository root so the Go module builds correctly. Evaluators are shell scripts with `bash`, `redis-cli`, and `jq` available in the container.
