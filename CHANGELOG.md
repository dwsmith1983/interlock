# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-02-26

### Fixed

- **Evaluation retry loop**: Step Function now retries trait evaluation every 60s when traits aren't ready, terminating via the existing validation timeout. Previously in AWS event-driven mode, the SFN wrote a PENDING RUNLOG and exited immediately — the RUNLOG stayed PENDING indefinitely unless a new MARKER arrived. The local watcher loop was unaffected.
- **ResolvePipeline error logging**: Step Function now writes a FAILED RUNLOG when `ResolvePipeline` returns an error (missing archetype, pipeline not found, archetype resolution failure). Previously these configuration errors caused a silent exit with no audit trail.
- **`logResult` attempt counter**: `attemptNumber` now only increments when retrying after a terminal state (COMPLETED/FAILED/CANCELLED). Transitioning within the same attempt (e.g., PENDING → COMPLETED) preserves the counter.
- **Validation timeout test**: Fixed midnight boundary edge case in `TestCheckValidationTimeout_NotBreached` where `now + 1h` wrapping past midnight caused a false positive.

### Changed

- Step Function state count: 49 → 52 (added `LogResolveFailed`, `CheckIfFirstEvalAttempt`, `WaitForReadiness`)

## [0.2.0] - 2026-02-25

### Added

- **Lifecycle Events**: stream-router publishes SNS events on pipeline terminal status changes
  - `PIPELINE_COMPLETED` and `PIPELINE_FAILED` events emitted to a dedicated lifecycle SNS topic
  - Configurable via `LIFECYCLE_TOPIC_ARN` environment variable on stream-router Lambda
  - Best-effort publishing — errors are logged, not propagated
  - `LifecycleEventsPublished` expvar counter for observability
  - `LifecycleEvent` struct in `internal/lambda/types.go`
- **Stuck-Run Detection**: watchdog detects runs stuck in non-terminal states
  - `CheckStuckRuns()` pure function scans for runs in PENDING/TRIGGERING/RUNNING beyond a configurable threshold
  - Default threshold: 30 minutes, configurable via `WatchdogConfig.StuckRunThreshold`
  - Dedup via distributed lock (`watchdog:stuck:{pipeline}:{schedule}:{date}`, 24h TTL)
  - `EventRunStuck` event kind for audit trail
  - `RUN_STUCK` alert with Category `"stuck_run"`
  - `StuckRunsDetected` expvar counter
  - 265 new lines of tests in `watchdog_test.go`
- **Alert Categorization**: machine-readable `Category` field on all alerts
  - `Alert.Category` field (JSON: `alertType`) set by all alert producers
  - Values: `schedule_missed`, `stuck_run`, `evaluation_sla_breach`, `completion_sla_breach`, `validation_timeout`, `trait_drift`
  - Enables downstream consumers (alert-logger, dashboards) to filter and route by category without parsing message strings
- New `EventKind` values: `PIPELINE_COMPLETED`, `PIPELINE_FAILED`, `RUN_STUCK`

## [0.1.1] - 2026-02-25

### Added

- **SLA Watchdog**: Framework-level absence detection for silently missed pipelines
  - `internal/watchdog/` package with `CheckMissedSchedules()` pure function and `Watchdog` polling struct
  - Detects when upstream ingestion fails silently and no evaluation starts by the configured deadline
  - Deadline resolution: schedule-level `Deadline` > `SLA.EvaluationDeadline`
  - Dedup via distributed lock (`watchdog:{pipeline}:{schedule}:{date}`, 24h TTL)
  - Respects calendar exclusions and per-pipeline watch enable/disable
  - `EventScheduleMissed` event kind for audit trail
  - `WatchdogConfig` with enable/interval on `ProjectConfig`
  - `SchedulesMissed` expvar counter
  - 11 unit tests covering all edge cases
- AWS Lambda handler for watchdog (`cmd/lambda/watchdog/`), invoked by EventBridge
- Terraform resources: watchdog Lambda, EventBridge rule, IAM role (DynamoDB RW + SNS publish)

## [0.1.0] - 2026-02-23

Initial release of the Interlock STAMP-based safety framework for data pipeline reliability.

### Added

#### Core Framework
- STAMP safety model with archetypes, traits, readiness rules, and run state machine
- Engine for trait evaluation and readiness checking (`internal/engine/`)
- Subprocess and HTTP evaluator runners with JSON protocol (`internal/evaluator/`)
- Reactive watcher loop with poll-based evaluation lifecycle (`internal/watcher/`)
- Structured logging, request ID middleware, and HTTP API server

#### Storage Providers
- Redis provider with Lua CAS, Streams, TTL retention (`internal/provider/redis/`)
- DynamoDB provider with single-table design, conditional writes, GSI (`internal/provider/dynamodb/`)
- Postgres archival store with cursor-based incremental sync (`internal/provider/postgres/`)
- Shared provider conformance test suite — 15 contract tests both backends pass

#### Multi-Schedule and Calendar Exclusions
- Per-pipeline multi-schedule support with independent locks, run logs, and SLA windows
- Calendar-based exclusions (named YAML calendars + inline days/dates)
- Timezone-aware schedule activation and deadline resolution

#### Trigger Types
- 8 trigger types: HTTP, command, Airflow, Glue, EMR, EMR Serverless, Step Functions, Databricks
- Runner dispatch with injectable AWS SDK clients and lazy initialization
- Generic `CheckStatus` polling for all AWS and Databricks trigger types

#### Lifecycle Management
- Retry with configurable exponential backoff and failure classification
- Cascade notifications to downstream pipelines
- Post-completion drift monitoring with configurable duration
- Replay support for re-running completed pipelines
- SLA tracking with evaluation and completion deadlines

#### AWS Event-Driven Architecture
- 5 Lambda handlers: stream-router, evaluator, orchestrator, trigger, run-checker
- Step Function state machine (47 states) orchestrating full pipeline lifecycle
- DynamoDB Streams integration for event-driven evaluation
- SNS and S3 alert sinks for serverless alerting
- Shared Lambda init with environment-based configuration

#### Deployment
- Terraform modules for AWS deployment (DynamoDB, Lambda, Step Functions, SNS, IAM, EventBridge)
- S3 backend bootstrap for Terraform state
- Docker Compose local environment with Redis and Postgres
- Lambda build script for cross-compilation

#### Documentation
- Hugo documentation site with Hextra theme (18 content pages)
- GitHub Pages deployment workflow
- Architecture, configuration, deployment, and API reference docs

#### CI/CD
- Go CI workflow (lint, format, test)
- PR labeler for automatic path-based labels
- GitHub Pages docs deployment on push to main

#### Testing
- Unit tests for evaluator, orchestrator, run-checker, archiver, Lambda init, commands, schedule
- Local E2E test suite (6 scenarios with sensor-backed evaluators)
- AWS E2E test suite (6 scenarios with DynamoDB and Step Functions)

### License

Released under the [Elastic License 2.0](LICENSE).

[0.2.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.1
[0.2.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.0
[0.1.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.1
[0.1.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.0
