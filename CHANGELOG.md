# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-03-03

### Added

- **SLA monitoring via EventBridge Scheduler**: one-time Scheduler entries fire `SLA_WARNING` and `SLA_BREACH` events at exact timestamps, replacing the previous parallel-branch polling approach. Schedules auto-delete after firing. On job completion, unfired schedules are cancelled and `SLA_MET` is published.
- **Sub-daily execution granularity**: pipelines can run at hourly or daily cadence depending on sensor data. When sensors include both `date` and `hour` fields, the framework uses a composite execution date (`2026-03-03T10`). Glue triggers receive `--par_day` and `--par_hour` arguments automatically.
- **Infrastructure trigger retry**: trigger execution failures (e.g., Glue `ConcurrentRunsExceededException`) retry 4 times with exponential backoff (30s, 60s, 120s, 240s) via Step Functions native Retry. Each failure is logged to the joblog table for audit. This retry budget is separate from `maxRetries` for job failures.
- **StatusChecker fallback in check-job**: when no terminal joblog entry exists, the orchestrator polls the trigger API directly to determine job status.

### Changed

- **Declarative validation rules** replace the archetype/trait/evaluator system. Pipeline configs define validation as YAML rules (`exists`, `equals`, `gt`, `gte`, `lt`, `lte`, `age_lt`, `age_gt`) — no custom evaluator code needed.
- **3 DynamoDB tables** (control, joblog, rerun) replace the single-table design for clearer access patterns and independent scaling.
- **4 Lambda functions** (stream-router, orchestrator, sla-monitor, watchdog) replace the previous 7+ handlers. The orchestrator is a multi-mode handler covering evaluate, trigger, check-job, and post-run.
- **~12-state Step Functions workflow** with parallel SLA monitoring branch replaces the 47-state machine.
- **EventBridge events** replace SNS for all alerting and lifecycle notifications.
- **Reusable Terraform module** — consumers deploy infrastructure without framework code in their repo.
- **Framework reads DynamoDB only** — external processes push sensor data into the control table.
- **Sequential Step Functions workflow** (18 states) replaces the previous parallel-branch design. SLA monitoring uses EventBridge Scheduler instead of a parallel Wait/Fire branch within the state machine.
- **sla-monitor Lambda** supports 5 modes: `schedule`, `cancel`, `fire-alert`, `calculate`, `reconcile`.
- **Trigger state** retries infrastructure failures independently of job failure retries (`maxRetries`). Exhausted trigger retries route to SLA cleanup and graceful termination instead of crashing.

### Removed

- Redis and Postgres storage providers (AWS-only going forward)
- CLI binary (`cmd/interlock`) and HTTP server
- Archetype, trait, and evaluator subprocess system
- Local mode (Docker Compose + Redis)
- Cascade notifications, post-completion drift monitoring, replay support
- SNS alert sinks, S3 alert sinks
- cobra, chi, pgx, go-redis, color dependencies

### Fixed

- Pipeline config included in Step Functions execution input, eliminating redundant DynamoDB reads during orchestrator modes (#30)
- YAML configs converted to JSON before DynamoDB storage (#27)
- Missing trigger enable variables added to Terraform module (`enable_emr_serverless_trigger`, `enable_sfn_trigger`) (#26)
- Sensor data `data` map unwrapped in stream-router before trigger condition evaluation (#28)
- Orchestrator output uses `status` string instead of `passed` boolean (#29)
- SLA monitor handles relative `:MM` deadline format for hourly pipelines (#35)
- `check-job` skips non-terminal joblog events (e.g., `infra-trigger-failure`) to prevent infinite polling loops (#36)

## [0.3.1] - 2026-02-28

### Fixed

- **stream-router date accuracy**: MARKER record processing now reads the `date` field from the DynamoDB NewImage when present, falling back to `time.Now().UTC()` for backward compatibility. This ensures correct date resolution at midnight rollover — e.g., an h23 completion marker written at 00:01 carries the previous day's date rather than inheriting today from the wall clock.

## [0.3.0] - 2026-02-27

### Added

- **Post-Completion Monitoring Expiry**: watchdog handles `COMPLETED_MONITORING → COMPLETED` transitions
  - `CheckCompletedMonitoring()` pure function scans for runs in `COMPLETED_MONITORING` whose monitoring window has elapsed
  - Uses `RunLogEntry.UpdatedAt` as the monitoring start time — no schema changes required
  - Transitions expired runs to `COMPLETED` via `PutRunLog`, appends `EventMonitoringCompleted` audit event
  - Monitoring window duration read from `Watch.Monitoring.Duration` per-pipeline config
  - Wired into both local-mode `Watchdog.scan()` and Lambda handler
  - Offloads the monitoring wait from Step Function executions to the watchdog's 5-minute scan, freeing SFN capacity immediately after the Glue job succeeds
  - `MonitoringResult` exported type for callers to inspect results
  - 3 unit tests: expired → transitions, still-in-window → skips, already-completed → ignores

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

[0.4.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.4.0
[0.3.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.3.1
[0.3.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.3.0
[0.2.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.1
[0.2.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.0
[0.1.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.1
[0.1.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.0
