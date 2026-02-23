---
title: Local (Docker)
weight: 2
description: Docker Compose services, E2E test suite, and sensor-backed evaluators.
---

The local deployment uses Docker Compose to run Interlock with Redis and Postgres. This is ideal for development, testing, and demo purposes.

## Services

The `demo/local/docker-compose.yml` defines:

| Service | Image | Purpose |
|---|---|---|
| `redis` | `redis:7-alpine` | Primary state store |
| `postgres` | `postgres:16-alpine` | Archival storage |
| `seed` | Custom (Go) | Registers pipelines, serves webhook endpoint |
| `interlock` | Custom (Go) | Runs the watcher loop |

### Seed Container

The seed container registers pipelines and archetypes at startup, then exposes a webhook server on port 8888 for HTTP triggers to call.

### Interlock Container

The interlock container runs the watcher loop. It connects to Redis for state and Postgres for archival. Evaluator scripts are mounted as volumes and run as subprocess evaluators.

## Running Locally

```bash
cd demo/local
docker compose up -d
```

To rebuild after code changes:

```bash
docker compose up -d --build
```

## Evaluators

The local demo uses shell-script evaluators that read sensor values from Redis:

### check-freshness

Reads `sensor:freshness` from Redis. Returns `PASS` if the value is below the configured threshold.

```bash
#!/bin/bash
VALUE=$(redis-cli -h redis GET sensor:freshness)
if [ "$VALUE" -lt "$THRESHOLD" ]; then
  echo '{"status":"PASS","value":{"minutes":'$VALUE'},"reason":"data is fresh"}'
else
  echo '{"status":"FAIL","value":{"minutes":'$VALUE'},"reason":"data is stale"}'
fi
```

### check-record-count

Reads `sensor:record-count` from Redis. Returns `PASS` if count meets the minimum.

### check-upstream

Reads `sensor:upstream` from Redis. Returns `PASS` if the upstream pipeline completed.

### Setting Sensor Values

Control evaluator outcomes by writing sensor values directly to Redis:

```bash
docker compose exec redis redis-cli SET sensor:freshness 15
docker compose exec redis redis-cli SET sensor:record-count 5000
docker compose exec redis redis-cli SET sensor:upstream completed
```

## E2E Test Suite

The local E2E tests (`demo/local/e2e-test.sh`) run 6 scenarios that mirror the AWS E2E suite:

### Scenarios

| # | Scenario | Description |
|---|---|---|
| 1 | Progressive readiness | Traits start failing, sensors updated to passing, pipeline triggers |
| 2 | Quality drop re-run | Pipeline completes, quality drops, re-evaluation triggers re-run |
| 3 | Dedup | Same pipeline+date+schedule evaluates only once |
| 4 | Excluded day | Pipeline with exclusion on current day is skipped |
| 5 | Pipeline not found | Non-existent pipeline returns error gracefully |
| 6 | Watcher-driven | Full watcher loop integration (register → evaluate → trigger → complete) |

### Running E2E Tests

```bash
make local-e2e-test
```

This runs the full suite and saves results to `demo/local/e2e-results/`.

### Cleanup and Teardown

Between scenarios, run logs are cleaned up to ensure isolation:

```bash
docker compose exec redis redis-cli DEL interlock:runlog:my-pipeline:2026-02-23:daily
```

Full teardown:

```bash
make local-e2e-test-teardown
```

This runs `docker compose down -v` to remove containers and volumes.

## Archival Verification

The E2E suite verifies Postgres archival:

1. Archiver interval is set to 30 seconds for testing
2. After scenarios complete, wait 35 seconds for archival
3. Assert rows exist in `runs` and `events` Postgres tables

```bash
docker compose exec postgres psql -U interlock -d interlock \
  -c "SELECT COUNT(*) FROM runs;"
```

## Configuration Files

The local demo mounts configuration from `demo/local/`:

```
demo/local/
├── docker-compose.yml
├── interlock.yaml          # Watcher configuration
├── pipelines/              # Pipeline YAML definitions
├── archetypes/             # Archetype YAML definitions
├── evaluators/             # Shell script evaluators
├── calendars/              # Calendar YAML files
├── e2e-test.sh             # E2E test runner
└── e2e-results/            # Test output (gitignored)
```
