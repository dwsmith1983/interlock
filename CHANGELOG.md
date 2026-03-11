# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Dry-run / shadow mode** (`dryRun: true`) — observation-only mode for evaluating Interlock against running pipelines without executing jobs. The stream-router evaluates trigger conditions, validation rules, and SLA projections inline, publishing all observations as EventBridge events. No Step Function executions, no job triggers, no rerun requests. New events: `DRY_RUN_WOULD_TRIGGER`, `DRY_RUN_LATE_DATA`, `DRY_RUN_SLA_PROJECTION`, `DRY_RUN_DRIFT`. DRY_RUN# markers stored with 7-day TTL for dedup and late-data detection. Post-run drift detection captures baseline at trigger time and compares when sensors update. SLA projection reuses production `handleSLACalculate` for consistent deadline resolution. Requires `schedule.trigger` and `job.type`. Calendar exclusions honored.
- **`DryRunSK` key helper** for DynamoDB DRY_RUN# sort keys.
- **`WriteDryRunMarker` / `GetDryRunMarker` store methods** with conditional write (idempotent dedup) and consistent read.
- **Config validation** for dry-run: requires both `job.type` and `schedule.trigger`.

## [0.8.0] - 2026-03-10

### Added

- **Inclusion calendar scheduling** (`schedule.include.dates`) — explicit YYYY-MM-DD date lists for pipelines that run on known irregular dates (monthly close, quarterly filing, specific business dates). Mutually exclusive with cron. Watchdog detects missed inclusion dates and publishes `IRREGULAR_SCHEDULE_MISSED` events.
- **Relative SLA** (`sla.maxDuration`) — duration-based SLA for ad-hoc pipelines with no predictable schedule. Clock starts at first sensor arrival and covers the entire lifecycle: evaluation → trigger → job → post-run → completion. Warning at 75% of maxDuration (or `breachAt - expectedDuration` when set). New events: `RELATIVE_SLA_WARNING`, `RELATIVE_SLA_BREACH`.
- **First-sensor-arrival tracking** — stream-router records `first-sensor-arrival#<date>` on lock acquisition (idempotent conditional write). Used as T=0 for relative SLA calculation.
- **Watchdog defense-in-depth for relative SLA** — `detectRelativeSLABreaches` scans pipelines with `maxDuration` config and fires `RELATIVE_SLA_BREACH` if the EventBridge scheduler failed to fire.
- **`WriteSensorIfAbsent` store method** — conditional PutItem that only writes if the key doesn't exist, used for first-sensor-arrival idempotency.
- **Config validation** for new fields: cron/include mutual exclusion, inclusion date format (YYYY-MM-DD), maxDuration format and 24h cap, maxDuration requires trigger.
- **Glue false-success detection** — `verifyGlueRCA` now checks both the RCA insight stream (Check 1) and the driver output stream for ERROR/FATAL log4j severity markers (Check 2). Catches Spark failures that Glue reports as SUCCEEDED when the application framework swallows exceptions.

### Changed

- `SLAConfig.Deadline` and `SLAConfig.ExpectedDuration` are now `omitempty` — relative SLA configs may omit the wall-clock deadline entirely.
- SFN ASL passes `maxDuration` and `sensorArrivalAt` to `CancelSLASchedules` and `CancelSLAOnCompleteTriggerFailure` states.
- sla-monitor `handleSLACalculate` routes to relative path when `MaxDuration` + `SensorArrivalAt` are present.

## [0.7.4] - 2026-03-10

### Fixed

- **False SLA warnings/breaches for sensor-triggered daily pipelines** — `scheduleSLAAlerts` resolved the SLA deadline against today's date, but sensor-triggered daily pipelines run T+1 (data for today completes tomorrow). Between 00:00 UTC and the deadline hour, the breach time landed on the same day instead of the next day, causing premature SLA alerts. The SLA calculation now shifts the deadline date by +1 day for sensor-triggered daily pipelines.

## [0.7.3] - 2026-03-10

### Added

- **Configurable drift detection field** (`PostRunConfig.DriftField`) — specifies which sensor field to compare for post-run drift detection. Defaults to `"sensor_count"` for backward compatibility. Fixes broken drift detection when sensors use a different field name (e.g., `"count"`).

### Fixed

- **Post-run drift detection never fired when sensor field was `count`** — drift comparison was hardcoded to `"sensor_count"` but bronze consumers write `"count"` in hourly-status sensors, causing `ExtractFloat` to return 0 for both baseline and current values. The `prevCount > 0 && currCount > 0` guard silently suppressed all drift detection.
- **Two flaky time-sensitive tests** — `TestSLAMonitor_Reconcile_PastWarningFutureBreach` and `TestWatchdog_MissedSchedule_DetailFields` used real wall-clock time instead of injected `NowFunc`, causing failures depending on time of day.

## [0.7.2] - 2026-03-08

### Added

- **Configurable sensor trigger deadline** (`trigger.deadline`) — closes auto-trigger window after expiry, publishes `SENSOR_DEADLINE_EXPIRED`.
- **TOCTOU-safe `CreateTriggerIfAbsent` store method** using DynamoDB conditional writes.
- **CloudWatch alarms**: Per-function Lambda error alarms, Step Functions failure alarm, DLQ depth alarms (control, joblog, alert queues), and DynamoDB Stream iterator age alarms. All alarm actions conditionally route to an SNS topic via `sns_alarm_topic_arn`.
- **EventBridge input transformers for alarm routing**: CloudWatch alarm state changes are reshaped into `INFRA_ALARM` InterlockEvent format and routed to both event-sink and alert-dispatcher — zero Go code changes required.
- **Lambda concurrency limits**: Per-function reserved concurrent executions via `lambda_concurrency` object variable (defaults: stream-router=10, orchestrator=10, sla-monitor=5, watchdog=2, event-sink=5, alert-dispatcher=3).
- **Secrets Manager Slack token**: `slack_secret_arn` variable enables alert-dispatcher to read the Slack bot token from Secrets Manager instead of an environment variable. Falls back to `SLACK_BOT_TOKEN` env var if not configured.
- **Lambda trigger IAM scoping**: `enable_lambda_trigger` and `lambda_trigger_arns` variables grant orchestrator `lambda:InvokeFunction` permission scoped to specific function ARNs.

### Changed

- **Sensor-triggered pipelines now receive proactive SLA scheduling** (removed cron-only guard).
- **Trigger deadline check extracted into independent `checkTriggerDeadlines` watchdog scan**.
- **Env var expansion restricted to `INTERLOCK_` prefix**: `os.ExpandEnv` in trigger config (Airflow, Databricks, Lambda) now only expands variables prefixed with `INTERLOCK_`, preventing unintended system variable substitution.
- **`time.Now()` → `d.now()` across all handlers**: All Lambda handlers use dependency-injected time for consistent testability.
- **Config cache deep copy via JSON round-trip**: `GetAll()` returns a deep copy preventing callers from mutating shared cache state.
- **Single-instant rule evaluation**: All validation rules within an evaluation cycle use the same timestamp for temporal consistency.

### Fixed

- **Trigger lock release on SFN start failure**: Both rerun and job-failure retry paths release the trigger lock if `StartExecution` fails, preventing permanently stuck pipelines (previously caused 4.5h deadlock).
- **`scheduleSLAAlerts` skip-on-error**: SLA alert scheduling now correctly skips on error instead of falling through to the next handler.
- **9 silent audit write error discards → WARN logging**: All `publishEvent` call sites across stream-router and orchestrator now log errors at WARN level instead of silently discarding them.
- **Missing `EVENTS_TABLE`/`EVENTS_TTL_DAYS` envcheck for alert-dispatcher**: Startup validation now checks for required environment variables.

## [0.7.1] - 2026-03-08

### Fixed

- **Glue RCA false-positive failure classification (Check 2 removed)**: The `verifyGlueRCA` Check 2 filter pattern (`?Exception ?Error ?FATAL ...`) matched benign JVM startup output in Glue's stderr (`/aws-glue/jobs/error`), causing every SUCCEEDED Glue job to be reclassified as FAILED. Classpath entries like `-XX:OnOutOfMemoryError` and Glue's internal `AnalyzerLogHelper` messages contain "Error" as substrings, producing a 100% false positive rate. Removed Check 2 entirely — Check 1 (GlueExceptionAnalysisJobFailed in the RCA log stream) is Glue's purpose-built mechanism for detecting false successes and is sufficient. Post-run validation provides the application-level safety net for data quality issues.

## [0.7.0] - 2026-03-08

### Added

- **Event-driven post-run monitoring**: Post-run drift detection moves from SFN poll loop to DynamoDB Stream processing. When a sensor arrives after job completion, stream-router compares the current sensor value against a date-scoped baseline captured at trigger completion. Drift above a configurable threshold triggers a rerun via the existing circuit breaker. Sensors arriving while a job is still running produce informational drift events only.
- **Configurable drift threshold**: `PostRunConfig.DriftThreshold` (`*float64`) sets the minimum sensor count change to trigger a drift rerun. Defaults to 0 (any change).
- **Watchdog post-run sensor absence detection**: Watchdog detects pipelines that completed without receiving post-run sensor data after a configurable `SensorTimeout` grace period. Publishes `POST_RUN_SENSOR_MISSING` event.
- **Typed trigger errors**: `TriggerError` with `Category` (PERMANENT/TRANSIENT) and `Unwrap()` support. `ClassifyFailure` uses `errors.As` for structured error handling. HTTP triggers return 4xx as PERMANENT, 5xx as TRANSIENT.
- **ConfigCache deep copy**: `GetAll()` returns a deep copy of cached configs, preventing callers from mutating shared cache state.
- **Validation engine string parsing**: `toFloat64` now handles string-typed numeric fields via `strconv.ParseFloat`, enabling `gte`/`lte` rules on sensor data stored as strings.
- **E2E test coverage**: 8 new end-to-end test groups covering rerun budget separation, post-run inflight drift, calendar exclusion skip, hour boundary rollover, concurrent drift dedup, pre-baseline sensor arrival, rerun after TTL expiry, and SLA hourly deadlines.

### Changed

- **Post-run removed from SFN**: 6 ASL states removed (`CheckHasPostRun`, `InitPostRunLoop`, `PostRunEvaluate`, `IsPostRunDone`, `WaitForPostRun`, `IncrementPostRunElapsed`). Job success routes directly to `CompleteTrigger`. State count: 30 → 24.
- **Post-run removed from orchestrator**: `handlePostRun` function and `"post-run"` dispatch case removed. Post-run logic is now entirely stream-based.
- **Watchdog uses dependency-injected time**: `WatchdogNowFunc` package variable replaced with `Deps.NowFunc` for consistent testability across all handlers.
- **Trigger runner context threading**: All AWS SDK client constructors (`getGlueClient`, `getEMRClient`, etc.) accept `context.Context` and pass it through to `aws.LoadDefaultConfig`.
- **SFN execution name truncation**: Execution names are truncated to 80 characters (AWS limit) at all 3 construction sites.
- **Environment variable scoping**: `os.ExpandEnv` in trigger config restricted to `INTERLOCK_` prefixed variables only.

### Fixed

- **Calendar exclusion uses execution date**: `isExcludedDate` checks the job's execution date (not `time.Now()`), preventing incorrect exclusions on re-runs for previous days. Supports both `YYYY-MM-DD` and composite `YYYY-MM-DDTHH` date formats.
- **Atomic lock reset**: `ResetTriggerLock` uses single DynamoDB `UpdateItem` with `attribute_exists(PK)` condition, eliminating the race window between delete and create.
- **Lock release on SFN start failure**: Both rerun and job-failure retry paths release the trigger lock if `StartExecution` fails, preventing permanently stuck pipelines.
- **Terminal trigger status on calendar exclusion**: `handleJobFailure` sets `FAILED_FINAL` instead of leaving the lock in `RUNNING` state to silently expire via TTL.
- **ASL CompleteTrigger failure path**: New `CheckSLAForCompleteTriggerFailure` → `CancelSLAOnCompleteTriggerFailure` → `CompleteTriggerFailed` states ensure SLA schedules are cancelled before entering terminal Fail state.
- **Event ordering**: `RERUN_ACCEPTED` only publishes after `ResetTriggerLock` confirms lock atomicity.
- **New events**: `BASELINE_CAPTURE_FAILED` (baseline capture error), `PIPELINE_EXCLUDED` (calendar exclusion in sensor, rerun, job-failure, and post-run drift paths), `RERUN_ACCEPTED` (audit trail for accepted reruns).
- **publishEvent error logging**: All 17 `publishEvent` call sites across stream-router and orchestrator now log errors at WARN level instead of silently discarding them.
- **SLA monitor error wrapping**: `createOneTimeSchedule` wraps errors with schedule name context via `fmt.Errorf`.
- **HTTP response body sanitization**: Error response bodies truncated to 512 bytes with control characters stripped.
- **DynamoDB table protection**: All 4 tables (control, joblog, events, rerun) now have `deletion_protection_enabled` and point-in-time recovery enabled.

## [0.6.2] - 2026-03-08

### Added

- **Lambda trigger type**: New `TriggerLambda` type for direct AWS Lambda SDK invocation (`RequestResponse`). Includes `LambdaTriggerConfig` with `functionName` and optional `payload` (supports env-var expansion). Non-polling `CheckStatus`. Useful when the orchestrator can invoke Lambda directly instead of going through function URLs.
- **Non-polling trigger synchronous completion**: `handleTrigger` now writes a success joblog entry immediately for non-polling triggers (http, command, lambda) and returns a sentinel `runId` so the Step Functions `CheckJob` JSONPath resolves. Previously, non-polling triggers that succeeded would crash the SFN because `$.triggerResult.runId` was omitted via `omitempty`.

## [0.6.1] - 2026-03-08

### Fixed

- **Glue RCA false-positive failure classification**: `verifyGlueRCA` Check 2 queried `/aws-glue/jobs/error` without a `FilterPattern`, causing benign stderr messages (e.g., `"Preparing ..."`) to be misclassified as failures. Added `FilterPattern` with error indicators (`?Exception ?Error ?FATAL ?Traceback ?OutOfMemoryError ?StackOverflowError`) so only genuine errors trigger failure classification. This prevented unnecessary re-runs that doubled Glue compute.
- **Watchdog date-boundary test flake**: `TerminalTriggerRetainsRecord` E2E test used sensor data without a `date` field, causing `ResolveExecutionDate` to fall back to `time.Now()`. When the calendar date advanced past the hardcoded trigger date, reconciliation resolved to a different date and fired a spurious `TRIGGER_RECOVERED`. Sensor data now includes an explicit date.

## [0.6.0] - 2026-03-07

### Added

- **SFN global timeout**: Step Function definition includes `TimeoutSeconds` (default 4h, configurable via `sfn_timeout_seconds` Terraform variable). Prevents unbounded execution if the orchestrator loop stalls.
- **Configurable trigger retry count**: Trigger state `MaxAttempts` is now driven by `trigger_max_attempts` Terraform variable (default 3, previously hardcoded 4). Reduces retry budget from 4 to 3 attempts.
- **Trigger terminal status lifecycle**: new `CompleteTrigger` ASL state sets trigger row to `COMPLETED` on job success and `FAILED_FINAL` on fail/timeout. New orchestrator `complete-trigger` mode with `Event` input field. Previously `TriggerStatusCompleted` was defined but never written, leaving all triggers stuck at `RUNNING` after SFN completion.
- **Bounded job poll window**: 5 new ASL states implement a time-bounded poll loop. Configurable via `jobPollWindowSeconds` in pipeline config (default: 3600s / 1h). `handleJobPollExhausted` writes a timeout joblog entry, sets trigger to `FAILED_FINAL`, and publishes `JOB_POLL_EXHAUSTED` event. Prevents unbounded `check-job` polling when external jobs hang.
- **Per-source rerun limits**: new `maxDriftReruns`, `maxManualReruns`, and `maxCodeRetries` fields on `JobConfig` with `*int` pointer semantics (nil = default, 0 = disabled). `CountRerunsBySource` filters rerun records by reason prefix, so drift/manual reruns no longer consume the job-failure retry budget.
- **Pipeline config validation**: `ValidatePipelineConfig` enforces bounds on all retry/rerun fields. Stream-router uses `getValidatedConfig` wrapper at all 3 config-read callsites — invalid configs are logged and skipped (fail-open).
- **Failure classification**: `FailureCategory` propagated from `StatusChecker` through `handleCheckJob` to joblog via variadic `WithFailureCategory` option. `handleJobFailure` reads the latest joblog category — `PERMANENT` failures use `maxCodeRetries` budget (default 1), `TRANSIENT`/empty use `maxRetries`. Separates infrastructure flakes from code bugs.
- **Dynamic trigger lock TTL**: `ResolveTriggerLockTTL()` reads `SFN_TIMEOUT_SECONDS` env var and adds a 30-minute buffer (default 4h30m). All 4 `AcquireTriggerLock` callsites use it. Terraform wires the variable to stream-router and watchdog Lambda environments.

### Removed

- Dead `FailureEvaluatorCrash` constant (defined but never referenced)

### Fixed

- **Glue false-success detection**: `checkGlueStatus` now cross-checks the CloudWatch RCA log stream when Glue reports `SUCCEEDED`. Glue can return `SUCCEEDED` via `GetJobRun` when the Spark driver catches a `SparkException` and exits cleanly (exit code 0). The RCA log stream (`{runId}-job-insights-rca-driver`) records `GlueExceptionAnalysisJobFailed` events with the actual failure reason. When detected, the job is reported as failed with the real error (e.g., "No space left on device"). Degrades gracefully if CloudWatch Logs is unavailable.
- **SLA alert suppression for completed pipelines**: `handleSLAFireAlert` checks trigger status before publishing — suppresses warnings/breaches when the pipeline already completed or permanently failed. Watchdog `scheduleSLAAlerts` skips schedule creation for finished pipelines, preventing ghost schedules after `CancelSLASchedules` cleanup.
- **Joblog fallback in SLA guards**: `handleSLAFireAlert` and `scheduleSLAAlerts` check the joblog for terminal events (`success`/`fail`/`timeout`) when the trigger row is nil or `RUNNING`. Covers cron pipelines (no trigger rows), TTL-expired triggers, and the race window before `CompleteTrigger` runs.
- **Watchdog forward-only alerting**: `detectMissedSchedules` now skips cron schedules whose most recent expected fire time is before the Lambda's cold start. Prevents retroactive `SCHEDULE_MISSED` alerts after fresh deploys or redeployments. Uses a `lastCronFire` helper to compute expected fire times from hourly (`MM * * * *`) and daily (`MM HH * * *`) cron patterns.
- **Watchdog reconcile mass-triggering**: `reconcileSensorTriggers` checked only `HasTriggerForDate` before re-triggering. Trigger records have 24h DynamoDB TTL — after expiry, reconcile saw "satisfied sensor + no trigger" and re-triggered completed pipelines. Now checks joblog for terminal events (`success`/`fail`/`timeout`) before acquiring a new lock. Additionally, `SetTriggerStatus` removes TTL on terminal statuses (`COMPLETED`/`FAILED_FINAL`) so trigger records persist indefinitely.

## [0.5.2] - 2026-03-05

### Fixed

- **Hourly SLA deadline offset**: relative deadline `:MM` for hourly pipelines (date format `2026-03-05T12`) now resolves to H+1:MM instead of H:MM. Data for hour H isn't generated until ~H+1:00, so the previous calculation set the breach deadline before data existed — guaranteeing a false breach every execution. Daily pipelines are unchanged.

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

[0.8.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.8.0
[0.7.4]: https://github.com/dwsmith1983/interlock/releases/tag/v0.7.4
[0.7.3]: https://github.com/dwsmith1983/interlock/releases/tag/v0.7.3
[0.7.2]: https://github.com/dwsmith1983/interlock/releases/tag/v0.7.2
[0.7.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.7.1
[0.7.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.7.0
[0.6.2]: https://github.com/dwsmith1983/interlock/releases/tag/v0.6.2
[0.6.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.6.1
[0.6.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.6.0
[0.5.2]: https://github.com/dwsmith1983/interlock/releases/tag/v0.5.2
[0.5.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.5.1
[0.5.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.5.0
[0.4.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.4.0
[0.3.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.3.1
[0.3.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.3.0
[0.2.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.1
[0.2.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.2.0
[0.1.1]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.1
[0.1.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.1.0
