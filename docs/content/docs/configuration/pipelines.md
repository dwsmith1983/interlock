---
title: Pipelines
weight: 1
description: PipelineConfig schema, archetype resolution, trait definitions, and readiness rules.
---

## PipelineConfig

A pipeline is the central unit of configuration. Each pipeline references an archetype, overrides trait settings, and defines its trigger and SLA.

```yaml
name: my-pipeline
archetype: batch-ingestion
tier: 1
traits:
  check-freshness:
    evaluator: ./evaluators/check-freshness
    config:
      thresholdMinutes: 30
    ttl: 3600
    timeout: 30
  check-record-count:
    evaluator: builtin:record-count
    config:
      minRecords: 500
    ttl: 3600
    timeout: 30
trigger:
  type: http
  method: POST
  url: http://localhost:8080/trigger
retry:
  maxAttempts: 3
  backoffSeconds: 30
  backoffMultiplier: 2.0
  retryableFailures: [TRANSIENT, TIMEOUT]
sla:
  evaluationDeadline: "10:00"
  completionDeadline: "12:00"
  timezone: America/New_York
watch:
  enabled: true
  interval: 5m
  monitoring:
    enabled: true
    duration: 2h
```

### Field Reference

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Unique pipeline identifier |
| `archetype` | string | yes | Name of the archetype to apply |
| `tier` | int | no | Priority tier (lower = higher priority) |
| `traits` | map | no | Per-trait configuration overrides |
| `trigger` | TriggerConfig | no | How to start the pipeline job |
| `retry` | RetryPolicy | no | Automatic retry configuration |
| `sla` | SLAConfig | no | Evaluation and completion deadlines |
| `schedules` | []ScheduleConfig | no | Multi-schedule windows (defaults to daily) |
| `exclusions` | ExclusionConfig | no | Calendar/day/date exclusions |
| `watch` | PipelineWatchConfig | no | Per-pipeline watcher overrides |

## Archetypes

An archetype is a STAMP safety template. It declares the required and optional traits that a pipeline must satisfy, plus the rule for combining trait results.

```yaml
name: batch-ingestion
requiredTraits:
  - type: check-freshness
    description: Validates data freshness
    defaultConfig:
      thresholdMinutes: 60
    defaultTtl: 3600
    defaultTimeout: 30
  - type: check-record-count
    description: Validates minimum record count
    defaultConfig:
      minRecords: 1000
    defaultTtl: 3600
    defaultTimeout: 30
optionalTraits:
  - type: check-schema
    description: Validates schema compatibility
    defaultConfig: {}
    defaultTtl: 7200
    defaultTimeout: 60
readinessRule:
  type: all-required-pass
```

### Trait Definition Fields

| Field | Type | Description |
|---|---|---|
| `type` | string | Trait type name (matches evaluator path or builtin name) |
| `description` | string | Human-readable description |
| `defaultConfig` | map | Default configuration passed to the evaluator |
| `defaultTtl` | int | Default result TTL in seconds |
| `defaultTimeout` | int | Default evaluation timeout in seconds |

### Archetype Resolution

When a pipeline is evaluated, the engine:

1. Loads the archetype by name from the registry
2. Merges the archetype's trait definitions with pipeline-level overrides
3. Pipeline `traits` override `defaultConfig`, `defaultTtl`, and `defaultTimeout`
4. Pipeline `traits` can specify an `evaluator` path (the archetype does not set evaluator paths)

Archetype YAML files are loaded from directories listed in `archetypeDirs` (local) or from `ARCHETYPE_DIR` (Lambda, defaults to `/var/task/archetypes`).

## Trait Configuration

Each trait in the pipeline's `traits` map overrides the archetype defaults:

```yaml
traits:
  check-freshness:
    evaluator: ./evaluators/check-freshness   # subprocess path
    config:
      thresholdMinutes: 30                     # overrides defaultConfig
    ttl: 1800                                  # overrides defaultTtl
    timeout: 15                                # overrides defaultTimeout
```

### Evaluator Path Formats

| Format | Resolution |
|---|---|
| `./path/to/script` | Subprocess evaluator (local mode) |
| `check-freshness` | HTTP evaluator (`{baseURL}/check-freshness`) in Lambda mode |
| `builtin:name` | Builtin handler registered in CompositeRunner |

## Readiness Rules

The readiness rule determines how individual trait results combine into a pipeline-level readiness decision.

### `all-required-pass`

The only rule type currently supported. A pipeline is `READY` when every required trait has status `PASS`. Optional traits are evaluated but do not block readiness.

```yaml
readinessRule:
  type: all-required-pass
```

| Trait Statuses | Pipeline Readiness |
|---|---|
| All required = PASS | `READY` |
| Any required = FAIL | `NOT_READY` |
| Any required = STALE | `NOT_READY` (re-evaluation needed) |

## Retry Policy

Automatic retries on failure with exponential backoff:

```yaml
retry:
  maxAttempts: 3
  backoffSeconds: 30
  backoffMultiplier: 2.0
  retryableFailures:
    - TRANSIENT
    - TIMEOUT
```

| Field | Type | Default | Description |
|---|---|---|---|
| `maxAttempts` | int | 3 | Maximum retry attempts |
| `backoffSeconds` | int | 30 | Initial backoff duration |
| `backoffMultiplier` | float | 2.0 | Exponential multiplier |
| `retryableFailures` | []string | [TRANSIENT, TIMEOUT] | Failure categories to retry |

### Failure Categories

| Category | Description | Retryable by Default |
|---|---|---|
| `TRANSIENT` | Temporary infrastructure error | Yes |
| `TIMEOUT` | Operation timed out | Yes |
| `PERMANENT` | Non-recoverable error | No |
| `EVALUATOR_CRASH` | Evaluator process crashed | No |

Backoff formula: `backoffSeconds × backoffMultiplier^(attempt - 1)`

## Project Configuration

The top-level `interlock.yaml` ties everything together:

```yaml
provider: redis          # or "dynamodb"
redis:
  addr: localhost:6379
  keyPrefix: interlock
  readinessTTL: 1h
  retentionTTL: 168h

archetypeDirs: [./archetypes]
pipelineDirs: [./pipelines]
evaluatorDirs: [./evaluators]
calendarDirs: [./calendars]

engine:
  defaultTimeout: 30s

watcher:
  enabled: true
  defaultInterval: 5m

alerts:
  - type: console
  - type: webhook
    url: https://hooks.slack.com/...

archiver:
  enabled: true
  interval: 5m
  dsn: postgres://user:pass@localhost:5432/interlock?sslmode=disable
```
