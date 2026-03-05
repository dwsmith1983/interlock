# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.1] - 2026-03-05

### Added

- **Proactive SLA monitoring**: watchdog Lambda creates EventBridge Scheduler entries for all pipelines with SLA configs, ensuring warnings and breaches fire even when pipelines never trigger (data never arrives, sensor fails, trigger missed). Idempotent via deterministic scheduler names — `ConflictException` means the schedule already exists and is skipped. (#45)

### Changed

- **SLA scheduling moved from SFN to watchdog**: removed `CheckSLAConfig` and `ScheduleSLAAlerts` states from the Step Function (18 → 16 states). The SFN retains only `CancelSLASchedules` to clean up unfired timers on job completion. (#45)
- **CancelSLASchedules accepts deadline/expectedDuration**: instead of pre-computed `warningAt`/`breachAt`, the cancel handler now receives raw SLA config and recalculates internally. This decouples cancel from the removed scheduling state. (#45)
- Watchdog Lambda now requires `SLA_MONITOR_ARN`, `SCHEDULER_ROLE_ARN`, and `SCHEDULER_GROUP_NAME` environment variables and `scheduler:CreateSchedule` + `iam:PassRole` IAM permissions. (#45)

## [0.5.0] - 2026-03-04

### Added

- **Centralized observability pipeline**: new `event-sink` Lambda writes all 14 EventBridge event types to a DynamoDB events table with a GSI for querying by event type and timestamp. New `alert-dispatcher` Lambda reads from an SQS alert queue and delivers formatted Slack notifications. EventBridge rules route events to both targets automatically. (#41)
- **Slack Bot API with message threading**: alert-dispatcher uses `chat.postMessage` with Bot token authentication. Thread records (`THREAD#{scheduleId}#{date}`) stored in the events table group related alerts into Slack threads by pipeline, schedule, and date. First alert for a pipeline-day creates a new thread; subsequent alerts reply in-thread. (#41)
- **SLA warning suppression**: `fire-alert` mode checks `BreachAt` timestamp before publishing `SLA_WARNING`. If breach time has already passed, the warning is suppressed to prevent duplicate warning+breach notifications. `handleSLASchedule` now includes `BreachAt` in warning payloads. `handleSLAReconcile` fires breach only (not both warning and breach) when past breach time. (#41)
- **Manual rerun system**: external processes write `RERUN_REQUEST#` records to the control table. Stream-router validates requests with a circuit breaker that compares sensor `updatedAt` vs joblog `completedAt` — rejects reruns when no new data has arrived. (#40)
- **Late data arrival detection**: stream-router detects sensor updates after job completion and publishes `LATE_DATA_ARRIVAL` events to EventBridge. (#40)
- **Prefix-match sensor keys**: stream-router matches sensor keys by prefix for per-period pipelines, enabling a single trigger condition to match sensors keyed with date+hour suffixes. (#38)
- **Watchdog trigger reconciliation**: watchdog re-evaluates sensor trigger conditions every 5 minutes. If a sensor meets the trigger threshold but no trigger lock exists (due to a silent completion-write failure), the watchdog acquires the lock, starts the Step Function, and publishes a `TRIGGER_RECOVERED` event. (#44)
- New event types: `LATE_DATA_ARRIVAL`, `RERUN_REJECTED`, `RETRY_EXHAUSTED`, `TRIGGER_RECOVERED`
- New DynamoDB events table with GSI1 (`eventType` → `timestamp`) for centralized event logging
- SQS alert queue with dead-letter queue for reliable Slack delivery

### Changed

- alert-dispatcher now requires `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` environment variables (replaces `SLACK_WEBHOOK_URL`)
- Terraform module exposes `slack_bot_token` (sensitive) and `slack_channel_id` variables instead of `slack_webhook_url`
- Lambda function count: 4 → 6 (added event-sink and alert-dispatcher)

### Fixed

- Orchestrator remaps per-period sensor keys during evaluation, preventing key mismatch when sensors use date+hour suffixes (#39)
- `ValidationExhausted` now ends the Step Functions execution as `FAILED` instead of `SUCCEEDED` (#41)
- Joblog entry written on validation exhaustion for audit trail completeness (#41)
- Watchdog prefix-matches trigger records for per-hour pipelines instead of requiring exact key match (#41)
- SLA deadline calculation for daily pipelines with next-day deadlines (e.g., `"02:00"`) now rolls forward 24 hours when the computed breach time is already past (#43)

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
- **18-state sequential Step Functions workflow** replaces the 47-state machine. SLA monitoring uses EventBridge Scheduler instead of a parallel branch.
- **EventBridge events** replace SNS for all alerting and lifecycle notifications.
- **Reusable Terraform module** — consumers deploy infrastructure without framework code in their repo.
- **Framework reads DynamoDB only** — external processes push sensor data into the control table.
- **sla-monitor Lambda** supports 5 modes: `schedule`, `cancel`, `fire-alert`, `calculate`, `reconcile`.
- **Trigger state** retries infrastructure failures independently of job failure retries (`maxRetries`). Exhausted trigger retries route to SLA cleanup and graceful termination instead of crashing.

### Removed

- Redis and Postgres storage providers (AWS-first; GCP and Azure planned after AWS stabilizes)
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

[0.5.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.5.0
[0.4.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.4.0
[0.3.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.3.1
[0.3.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.3.0
[0.2.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.1
[0.2.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.0
[0.1.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.1
[0.1.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.0
