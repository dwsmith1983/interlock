---
title: Quickstart
weight: 2
description: Configure your first pipeline and run a readiness check.
---

This guide walks through a minimal local setup using Redis.

## 1. Start Redis

```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

## 2. Create Configuration

Create `interlock.yaml` in your project directory:

```yaml
provider: redis
redis:
  addr: localhost:6379
  keyPrefix: interlock
  readinessTTL: 1h
  retentionTTL: 168h

archetypeDirs:
  - ./archetypes
pipelineDirs:
  - ./pipelines
evaluatorDirs:
  - ./evaluators

watcher:
  enabled: true
  defaultInterval: 5m

alerts:
  - type: console
```

## 3. Define an Archetype

Create `archetypes/batch-ingestion.yaml`:

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
readinessRule:
  type: all-required-pass
```

An archetype is a STAMP safety template — it declares what traits (safety checks) must pass before a pipeline is considered ready.

## 4. Define a Pipeline

Create `pipelines/my-pipeline.yaml`:

```yaml
name: my-pipeline
archetype: batch-ingestion
tier: 1
traits:
  check-freshness:
    evaluator: ./evaluators/check-freshness
    config:
      thresholdMinutes: 30
  check-record-count:
    evaluator: ./evaluators/check-record-count
    config:
      minRecords: 500
trigger:
  type: http
  method: POST
  url: http://localhost:8080/trigger
sla:
  evaluationDeadline: "10:00"
  completionDeadline: "12:00"
  timezone: America/New_York
```

The pipeline references the `batch-ingestion` archetype and overrides trait configurations. The trigger fires an HTTP POST when all required traits pass.

## 5. Write an Evaluator

Evaluators are executables that read JSON from stdin and write JSON to stdout.

Create `evaluators/check-freshness`:

```bash
#!/bin/bash
# Read input: {"pipelineID":"my-pipeline","traitType":"check-freshness","config":{"thresholdMinutes":30}}
INPUT=$(cat)
echo '{"status":"PASS","value":{"minutesSinceUpdate":12},"reason":"data is fresh"}'
```

```bash
chmod +x evaluators/check-freshness
```

The JSON protocol:

**Input** (stdin):
```json
{
  "pipelineID": "my-pipeline",
  "traitType": "check-freshness",
  "config": { "thresholdMinutes": 30 }
}
```

**Output** (stdout):
```json
{
  "status": "PASS",
  "value": { "minutesSinceUpdate": 12 },
  "reason": "data is fresh"
}
```

Valid status values: `PASS`, `FAIL`, `STALE`.

## 6. Register and Evaluate

```bash
# Register the pipeline
interlock pipeline register -f pipelines/my-pipeline.yaml

# Run a readiness check
interlock pipeline check my-pipeline
```

The readiness check evaluates all required traits and returns a `READY` or `NOT_READY` result. When ready, the configured trigger fires automatically (in watcher mode) or can be triggered manually.

## Next Steps

- [Architecture Overview](../architecture/overview) — understand the STAMP model and run state machine
- [Configuration](../configuration/pipelines) — full pipeline configuration reference
- [Local Deployment](../deployment/local) — run the complete stack with Docker Compose
