# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`completedAt` on trigger rows and the `postRun.watchWindow` setting.** complete-trigger now records when a run completes as `completedAt` (RFC3339 UTC) on the trigger row, through the new `store.MarkTriggerCompleted` (`ControlRecord.CompletedAt`); a rerun's completion overwrites it. `postRun.watchWindow` (a Go duration above 0 and at most `720h`, default `4h`, `types.DefaultPostRunWatchWindow`) is how long after completion a date is watched for late data, drift and the post-run checks; it is validated now and enforced by the next change. New `lambda.PostRunWatchEnd` and `lambda.PostRunWatched` give the end of a completed date's watch and whether a time is still before it; nothing calls them yet. The watchdog's post-run sensor grace period default moves to `types.DefaultPostRunSensorTimeout` (still `2h`).
- **`evaluation-released` job event and `EVALUATION_RELEASED` event type.** New `types.JobEventEvaluationReleased` and `types.EventEvaluationReleased` (not on the alert rule) for a date whose readiness evaluation ran out before its cutoff and whose trigger lock was released (see the first `Changed (BREAKING)` entry). New `lambda.EvaluationReleased` reports whether a date's newest job row is `evaluation-released`. The SLA checks, the watchdog's cutoff check and the stream-router treat such a date as not started, so it can start again before the cutoff and is closed after it. When the watchdog's sensor-trigger reconciliation starts such a date again, it publishes `JOB_TRIGGERED` (`source: reconciliation`) instead of `TRIGGER_RECOVERED`.
- **`lambda.IsRerun` and `lambda.PublishCutoffExpired`.** `IsRerun` reports whether a run date has a `RERUN#` row, which every rerun path writes before it starts the execution. `PublishCutoffExpired` publishes the `SENSOR_DEADLINE_EXPIRED` alert for a date past its cutoff; `lambda.CloseRunAtCutoff` now uses it, with the same detail and message.
- **`CONFIG_INVALID` alert for a rejected pipeline config.** At the end of each pass the watchdog publishes `CONFIG_INVALID` (on the alert rule, red in Slack) for each `CONFIG` row the config cache rejected, with `reason` and `hash` in its detail and the parsed pipeline ID, or the raw PK, as `pipelineId`. It reports once per broken config version and day, guarded by a `CONFIG_INVALID#<hash>` marker row with a 24 h TTL; a failed publish deletes the marker so the next pass tries again, and a pass reports at most 20 rows. Slack alert text now turns control characters in every value (newlines, tabs, the rest of C0 and DEL) into spaces, so a value cannot start a line that reads as another alert.

### Changed (BREAKING)

- **`postRun.sensorTimeout` is validated.** A `sensorTimeout` that is not a Go duration, or is zero or negative, now rejects the config with `CONFIG_INVALID`; the watchdog used to fall back to `2h` or alert at once. With post-run `rules`, the effective `sensorTimeout` (default `2h`) must not exceed the effective `postRun.watchWindow` (default `4h`); such a config is rejected with `postRun.sensorTimeout <d> exceeds postRun.watchWindow <d>`, where a value taken from its default is marked `(default)`. These configs used to load.
- **An evaluation that runs out before its cutoff releases the date.** A first run whose evaluation window closed before `schedule.trigger.cutoff` used to write a `validation-exhausted` row, publish `VALIDATION_EXHAUSTED` and leave the trigger row `RUNNING`, so nothing could retry readiness for that date, the stale-trigger check later published a misleading `SFN_TIMEOUT`, and the date's SLA alerts were suppressed. Now ValidationExhausted writes an `evaluation-released` row, deletes the trigger row and publishes `EVALUATION_RELEASED` (not an alert, best effort). The next trigger sensor write, or the watchdog's reconciler while the trigger sensor still passes, starts evaluation again until the cutoff, and the existing cutoff guards close the date after it with one `SENSOR_DEADLINE_EXPIRED`. A cutoff applies only for a date key of its cadence, so an hourly `:MM` cutoff on a daily date releases nothing. Without a cutoff, or for a rerun (a date with a `RERUN#` row), exhaustion now also sets the trigger row `FAILED_FINAL`, and a failed `VALIDATION_EXHAUSTED` publish fails the state so it is retried, since that event is now the date's final alert. Each release, restart and exhaustion cycle publishes one `EVALUATION_RELEASED` and one `JOB_TRIGGERED`. A `RERUN_REQUEST` for a released date before its cutoff is refused with an `INFRA_FAILURE` alert ("lock reset failed"), since there is no trigger row to reset: write the sensor again. A released date without a `RUN#` row whose sensor stops passing never gets a trigger row, so it gets no final alert and cannot be rerun until a passing trigger sensor closes it after the cutoff. Lambdas and state machine deploy in either order: only the ValidationExhausted comment changed.
- **A run still evaluating at its cutoff no longer launches.** A run whose evaluation began before `schedule.trigger.cutoff` kept polling, launched the job if readiness passed after the cutoff, and otherwise ended with `VALIDATION_EXHAUSTED` and a `RUNNING` trigger row. Now Evaluate reports `cutoff` at or after the cutoff for a date with no `RERUN#` row, and IsReady sends it to ValidationExhausted, which sets the row `FAILED_FINAL` and publishes one `SENSOR_DEADLINE_EXPIRED` (`source: orchestrator`, same detail as the other sources) before ending the execution. An evaluation window that closes after the cutoff ends the same way. The alert can come up to one `evaluation.interval` after the cutoff. A run that passed readiness before the cutoff still launches, and requested reruns and failure retries are not checked. ValidationExhausted now reads the pipeline config, so a failed read fails the state (Retry, then InfraFailure). Either side can deploy first: an old state machine ignores the `cutoff` status, so the run ends when its window does and the handler then closes it at the cutoff, and an old Lambda never returns `cutoff`.
- **The cutoff is checked per `RUN#` run date, with `cutoffDayOffset`.** The cutoff is the latest time a run date may start. The watchdog used to guess the date (today in UTC, or the previous hour for `:MM`) and close it. Now, for each `RUN#` row (same window as SLA scheduling) whose cutoff has passed with no trigger row and no finished run, it writes `FAILED_FINAL` and publishes `SENSOR_DEADLINE_EXPIRED` once. If the publish fails, the `FAILED_FINAL` row is deleted so the next pass retries. A daily `HH:MM` cutoff falls on the run date plus `cutoffDayOffset` days (0 to 7, default 0) in `schedule.timezone` (else `sla.timezone`, else UTC); an hourly `:MM` cutoff falls at minute MM of the hour after the run's hour. A cutoff is never earlier than its wall time: one inside a DST gap falls at the gap end, and a time repeated at fall-back at its first occurrence. The cutoff must be `HH:MM` or `:MM`, and a non-zero offset is rejected with an hourly cutoff or without a cutoff. The alert detail carries `cutoff` and `cutoffAt` instead of `triggerDeadline`, the Slack alert shows both and is red, and the hint says to investigate and then submit a `RERUN_REQUEST`. A pipeline without `RUN#` rows gets no watchdog cutoff check. New `lambda.ResolveCutoffTime` and `lambda.CutoffPassed`.
- **A sensor processed after the cutoff no longer starts a run.** The stream-router and the watchdog's sensor-trigger reconciliation used to start a run for any ready sensor, so a date the watchdog had not closed yet, or one with no `RUN#` row, still started late. Now neither starts anything for a date past its cutoff, whether or not it has a `RUN#` row. A date with no trigger row is closed with `FAILED_FINAL` and one `SENSOR_DEADLINE_EXPIRED`, whose `source` is `stream-router` or `reconciliation`; the stream-router goes by the time it handles the record. An excluded date, or one whose job ran (`success`, `fail` or `timeout`), is left alone; a date whose launch or evaluation was exhausted never ran, so it is closed too and a `RERUN_REQUEST` can run it. Late data after a `COMPLETED` run still writes its rerun request, and a `RERUN_REQUEST` still runs after the cutoff. A failed publish removes the row and fails the stream record, which is redelivered up to three times; after that, reconciliation closes the date while the sensor stays ready. The guard does nothing for a date key of the other cadence than the cutoff (a date-only sensor with an hourly `:MM` cutoff, or a date-hour sensor with a daily `HH:MM` one). The watchdog's cutoff check, the stream-router and the reconciler share the new `lambda.CloseRunAtCutoff`.
- **`schedule.trigger.deadline` is renamed to `schedule.trigger.cutoff`.** It is the latest start time, and the old name was easy to confuse with `sla.deadline`. A config that still sets `deadline` under the trigger no longer has a cutoff.
- **`schedule.cron` is a start-by time, checked per `RUN#` date and alerted once.** The watchdog used to guess today's date in UTC, read only the cron's minute and hour, count a trigger row written when evaluation began as a start, and publish `SCHEDULE_MISSED` again every pass. Now, for each `RUN#` row of a cron pipeline (same window as SLA scheduling), it takes the cron's first fire in that date's local day or hour as the start-by time and, once that has passed, publishes `SCHEDULE_MISSED` unless the job started or the run finished (`COMPLETED`, `FAILED_FINAL`). The orchestrator marks the trigger row with `startedAt` just before it launches the job (a `lambda` job runs inside the launch). A `SENSOR#schedule-missed-check#<date>` marker keeps the alert to one per date; it is written only after the publish succeeds, so a failed publish is retried on the next pass. The detail carries `cron`, `startBy` and a new `actionHint`, and no longer `expectedTime`. A pipeline without `RUN#` rows gets no check, and `schedule.time` together with `schedule.cron` is rejected (`schedule.time is not used with schedule.cron; the cron time is the start-by time`). New `store.MarkTriggerStarted` and `ControlRecord.StartedAt`.
- **Proactive SLA schedules are created only for run dates that have a `RUN#` row.** The watchdog used to guess the run date from the clock (today in UTC, or the previous hour for an hourly deadline) and schedule SLA alerts for it. Now the user writes the expected run dates as `PK=PIPELINE#<id>`, `SK=RUN#<runKey>` rows in the control table (`YYYY-MM-DD`, or `YYYY-MM-DDThh` for an hourly `:MM` deadline, optional `ttl`), and the watchdog schedules the warning and breach for each row from 8 days before to 2 days after the current UTC date whose breach is still ahead and whose run has not finished. A pipeline with an SLA and no rows gets no proactive SLA alerts. A row of the other cadence is ignored, a malformed key is skipped with a warning, and an excluded date is judged by the row's date instead of the clock. The stream-router ignores `RUN#` rows. New `store.ListRunKeys` and `types.RunSK`.
- **The day an SLA deadline falls on is set by `sla.deadlineDayOffset`, not guessed.** A sensor-triggered pipeline with a daily `HH:MM` deadline used to have its deadline moved to the day after its execution date without saying so; cron pipelines did not. Now the deadline falls on the execution date plus `sla.deadlineDayOffset` days (0 to 7, default 0, the same day) for every pipeline. A sensor pipeline that relied on the old shift needs `deadlineDayOffset: 1`, or its runs that finish after the deadline on their own date are reported as `SLA_BREACH`. A non-zero offset is rejected with an hourly `:MM` deadline or without `sla.deadline`. The watchdog's SLA schedules, the cancel step's verdict and the dry-run projection all use the offset. The cancel step still deletes the run's SLA schedules first, then fails (and Step Functions retries it) when it cannot read the pipeline config, instead of judging against the unshifted date.

### Changed

- **The watchdog's sensor-trigger reconciliation covers pipelines with `schedule.cron`.** It used to skip them, a leftover from when cron started runs, so a missed stream record for a cron pipeline was never healed and a released date restarted only on its next sensor write. Cron is only the start-by time, so these pipelines are now reconciled like any other sensor-triggered pipeline, under the `cron` schedule ID: missed stream records are healed (`TRIGGER_RECOVERED`) and released dates restart on a watchdog pass (`JOB_TRIGGERED`, `source: reconciliation`). As for other sensor-triggered pipelines, a trigger sensor with no `date` field that keeps passing now makes the watchdog start each new day's run on its first pass after UTC midnight, since the date falls back to today; give the `date` in the sensor to control which run date it starts.
- **`schedule.cron` is validated.** It was never checked, so a typo or an unsupported form loaded without complaint. Now a non-empty `schedule.cron` must be standard five-field cron (minute, hour, day of month, month, day of week, with names, ranges, steps and lists), parsed with `github.com/robfig/cron/v3` and evaluated in `schedule.timezone`; `L`, `W` and `#` are not supported, and day of week is 0 to 6. A six-field expression, a descriptor such as `@daily`, a `TZ=` prefix, or an out-of-range value rejects the config with `schedule.cron "..." is not a valid five-field cron expression`, reported as `CONFIG_INVALID`. New `validation.ParseCron`, and `lambda.StartByTime`, which returns the first cron fire inside a run key's day or hour in the pipeline's timezone.
- **The orchestrator can start Step Functions executions.** It now requires `STATE_MACHINE_ARN` at startup; Terraform sets it and grants `states:StartExecution` on the pipeline state machine, and the LocalStack deploy corrects it (with the stream-router's and the watchdog's) when the state machine ARN differs from the predicted one. CompleteTrigger now receives the run ID and the execution name, and a new `IsRetrying` state ends an execution without cancelling the SLA schedules when CompleteTrigger reports `retrying`.
- **A dry-run pipeline's answer to a job failure checks the excluded date first and uses the failed row's own category.** `DRY_RUN_WOULD_RETRY` and `DRY_RUN_RETRY_EXHAUSTED` now report an excluded date before the budget, take the budget from the category on the `fail` or `timeout` row that arrived (`PERMANENT` uses `maxCodeRetries`) instead of the newest row for the date, and count no retries so far, since a dry run never starts an execution. Their detail has `retries: 0`.
- **The run's CompleteTrigger is now the only thing that retries a failed job.** The stream-router used to restart on every `fail` or `timeout` job log row while the same execution's CompleteTrigger wrote `FAILED_FINAL` and cancelled the SLA schedules, so whichever wrote last won. Now the stream-router starts nothing for those rows; for a dry-run pipeline it reports what CompleteTrigger would decide (`DRY_RUN_WOULD_RETRY` or `DRY_RUN_RETRY_EXHAUSTED`). CompleteTrigger reads the run's own row (a `PERMANENT` category uses `maxCodeRetries`) and counts the retries so far from the execution name. A dry-run pipeline or a missing config ends the run with `FAILED_FINAL`; an excluded date with `FAILED_FINAL` and `PIPELINE_EXCLUDED`; a spent budget with `FAILED_FINAL` and `RETRY_EXHAUSTED`; an execution that cannot be started with `FAILED_FINAL` and `INFRA_FAILURE`. Otherwise the next attempt starts, the SLA schedules are kept, and a trigger row that is still `RUNNING` gets a fresh TTL (a finished one is left alone). A retry is named `<execution>-r<N>` (the first is `<execution>-r1`), replacing `<pipeline>-<schedule>-<date>-rerun-<attempt>`, and a repeated CompleteTrigger does not start a second copy. The budget is per execution chain: a requested rerun or a reconciler start gets a fresh `maxRetries`. A closed job poll window counts as a timeout, so it now retries too. A `fail` or `timeout` that arrives after its execution ended, or for another run ID, no longer starts anything.
- **A closed job poll window now writes a `timeout` row, and `JOB_FAILED` comes from the job log.** The orchestrator used to write a non-terminal `job-poll-exhausted` row and publish `JOB_POLL_EXHAUSTED`; it now writes a `timeout` row for the run with the error "job poll window exhausted". `JOB_POLL_EXHAUSTED` and `job-poll-exhausted` are removed, including from the alert rule. The stream-router publishes `JOB_FAILED` for every `fail` or `timeout` row (detail `event`, `error`, `runId`, and `category` when set), including rows the job writes itself and rows that arrive after their run ended, with the error cut to 2 KB. CheckJob no longer publishes it for polled failures, so each failure alerts once. `JOB_FAILED` and `JOB_COMPLETED` now come only from a new row (not an overwrite) with a valid date, for a configured pipeline whose schedule and date have a trigger row; a dry-run pipeline's failure answer needs its dry-run marker instead.
- **HTTP jobs now report their own result.** A 2xx from the trigger endpoint used to count as success: the orchestrator wrote a `success` row as soon as the call returned. Now a 2xx only means the job was accepted. Each request carries the run identity the job needs for its result row: `X-Interlock-Pipeline-Id`, `X-Interlock-Schedule-Id`, `X-Interlock-Date` and `X-Interlock-Run-Id` (the Step Functions execution name), set after the configured headers so config cannot override them. `${INTERLOCK_PIPELINE_ID}`, `${INTERLOCK_SCHEDULE_ID}`, `${INTERLOCK_DATE}` and `${INTERLOCK_RUN_ID}` expand to the same values in the configured body and header values, and the default body gains `pipelineId`, `scheduleId`, `date` and `runId`. The run stays RUNNING until the job writes a `JOB#<scheduleId>#<date>#<unix millis>` row with that `runId` to the joblog table, or the job poll window closes. See the `http` section of the triggers docs for the row format and the IAM the job needs.

### Removed

- **The `command` trigger type.** It ran a binary named in the pipeline config inside the shared orchestrator Lambda, with that Lambda's role, so one team's config could act as the orchestrator. A config with `job.type: command` is now rejected at load and reported as `CONFIG_INVALID`, and so is any other `job.type` that is not a trigger type; the other pipelines keep running. Move such a job behind an `http` endpoint or a `lambda` function.

### Fixed

- **An exhausted evaluation no longer leaves its trigger row `RUNNING` or hides the SLA alerts of a date that can still start.** Before its cutoff the date is released, and otherwise its trigger row is set `FAILED_FINAL`, so the stale-trigger check no longer closes it with a late `SFN_TIMEOUT`. It still does when publishing `VALIDATION_EXHAUSTED` keeps failing through the step's retries, as on the cutoff path, and a retry after a publish that succeeded but whose status write failed sends the alert twice.
- **An exhausted evaluation no longer runs the SLA cancel step.** When the evaluation window closed without the rules passing, `ValidationExhausted` went on to `CheckCancelSLA`, which deleted the run's SLA schedules and published an SLA verdict for a job that never ran. It now ends the execution. The run's unfired SLA warning and breach schedules still fire. sla-monitor suppresses them for a date that ended (a `FAILED_FINAL` trigger row); for a date released before its cutoff they alert as usual, since it can still start.
- **A `lambda` job whose function returns an error is a job failure, not a trigger failure.** The orchestrator treated a `FunctionError` like an Invoke that never reached the function: it wrote `infra-trigger-failure` and failed the Trigger task, so the Trigger state's retry (`trigger_max_attempts`) ran a job that had already run and failed, and `job.maxRetries`, `JOB_FAILED` and the failure category never applied. Now it writes a `fail` row for the run with category `PERMANENT` and the function's error, publishes `JOB_TRIGGERED`, and lets the run end like any other job failure: `JOB_FAILED`, then CompleteTrigger retries under `maxCodeRetries` (default 1). Only an Invoke that fails (throttling, a missing function, no permission) still goes to the Trigger state's retry. The docs now say that `trigger_max_attempts` counts retries after the first attempt (default 3, so four attempts waiting 30s, 60s and 120s); some pages said four retries with a 240s wait.
- **Pipeline IDs now fit every name built from them.** A pipeline ID was free text, but Step Functions execution names (80 characters, no spaces or `/:#` and similar) and EventBridge Scheduler names (64 characters of `0-9a-zA-Z_.-`) are built from it. A long ID lost its start when an execution name was cut to its last 80 characters, so two pipelines could get the same name, and an ID with a forbidden character failed every start. A config now loads only when `pipeline.id` is 1 to 31 characters of `A-Za-z0-9_-` and equals the ID in its `PIPELINE#<id>` key; any other is rejected and reported as `CONFIG_INVALID`. Execution and SLA schedule names are no longer truncated or hashed. A requested rerun's name drops the free-text reason and carries the rerun attempt instead, `<pipeline>-<schedule>-<date>-rerun<attempt>-<unix>` (the reason stays on the rerun row), so two reruns started in the same second no longer collide; before, the second start failed and released the trigger lock the first run held. A `RERUN_REQUEST#` row whose date is not `YYYY-MM-DD` or `YYYY-MM-DDThh`, whose SK is malformed, or whose schedule is not the one the pipeline uses is skipped with a warning instead of being retried. The Terraform module fails `terraform plan` when a pipeline file's name differs from its `pipeline.id`.
- **One bad pipeline config no longer stops every pipeline.** The config scan failed as a whole when any `CONFIG` row was bad (a PK without `PIPELINE#`, a missing or non-string `config`, JSON that does not unmarshal such as `"maxRetries": "3"`), so the stream-router, the orchestrator's CompleteTrigger and every watchdog check failed for every pipeline in the deployment. The scan now skips a bad row, and also a config that fails validation, and keeps the rest; only a DynamoDB error fails it. Each skipped row is logged as a warning with its PK, the reason (cut to 1 KB) and a short SHA-256 of its `config` string, once per cache refresh. The watchdog used to act on configs the stream-router skipped as invalid; every Lambda that reads the config cache now sees the same accepted set. The cache also no longer rescans on every call when no config is accepted.
- **SLA alerts no longer go quiet while a failed job is retried.** The SLA warning and breach alerts, the watchdog's SLA scheduling and its relative-SLA breach check stopped when the trigger row was finished or when the newest job log row for the date was terminal. During a CompleteTrigger retry the trigger row stays `RUNNING` while that newest row is the previous attempt's `fail` or `timeout`, so a retried run lost its `SLA_WARNING`, `SLA_BREACH` and `RELATIVE_SLA_BREACH` alerts. With a trigger row, `COMPLETED` or `FAILED_FINAL` now means finished, and so does a `RUNNING` row whose newest job log row is any other terminal event (`success`, `validation-exhausted`, `infra-trigger-exhausted`); a `RUNNING` row whose newest row is a `fail` or `timeout`, or that has no job log row yet, is still running and keeps its SLA alerts. Without a trigger row, any terminal job log row still means finished. The watchdog also no longer recreates a warning schedule whose time has passed, which could repeat `SLA_WARNING`.
- **Slack alerts with event detail lost their message.** An event carrying a detail map (retry counts, errors) showed only the header and known detail fields; the message line now always follows the header. Values are escaped for Slack mrkdwn (`&`, `<`, `>`), so text from a job's error cannot mention `@channel` or form links, and the text is cut to 2900 characters (marked `...`) to stay under Slack's section limit.
- **A rerun no longer reads the previous attempt's job result.** CheckJob read the newest JOB# row for the pipeline, schedule and date without matching the run, so a rerun's first check saw the earlier attempt's `fail` and failed at once, and a newer non-terminal row hid an older terminal one. CheckJob now reads the newest `success`, `fail` or `timeout` row whose `runId` is its own run (new `store.GetJobResult`, which pages through the rows), and reports that row's error. The trigger mode always returns a run ID and a `metadata` object. The run ID is the job API's ID for polled types (falling back to the Step Functions execution name) and the execution name for `http`, `command` and `lambda`. The `success` row of `command` and `lambda` jobs, still written when the call returns, now carries it instead of `sync`.
- **The build script now produces the zips Terraform requires.** `deploy/build.sh` only wrote `deploy/dist/<handler>/bootstrap`, while `deploy/terraform/lambda.tf` reads `<dist_path>/<handler>.zip` for every function, so `terraform plan` failed on a fresh build. The script now zips each `bootstrap` (at the root of the archive) into `deploy/dist/<handler>.zip`, matching the LocalStack build.
- **The watchdog can now release trigger locks.** Its IAM policy granted `PutItem` and `UpdateItem` on the control table but not `DeleteItem`, so `ReleaseTriggerLock` was denied whenever the watchdog had to back out a lock after a failed Step Functions start during sensor-trigger reconciliation, leaving the lock row in place until its TTL expired. The `WriteControlTable` statement now includes `dynamodb:DeleteItem`.
- **alert-dispatcher no longer crash-loops when Slack is not configured.** `deploy/terraform/variables.tf` defaults the Slack bot token, secret ARN and channel ID to empty and documents that as logging only, but the Lambda required `SLACK_CHANNEL_ID` at startup and exited when no token was found, so a default deploy restarted on every invocation. `SLACK_CHANNEL_ID` is no longer a required variable; with no token from `SLACK_BOT_TOKEN` or `SLACK_SECRET_ARN` the dispatcher logs a warning and runs in logging-only mode, writing each alert to the log instead of Slack. A token without a channel ID is still rejected at startup, as is a `SLACK_SECRET_ARN` that cannot be read or holds an empty secret. Terraform now catches that case at plan time: a precondition on the alert-dispatcher function fails `terraform plan` when `slack_bot_token` or `slack_secret_arn` is set and `slack_channel_id` is empty. A channel ID with no token still starts in logging-only mode, but the startup warning now says that alerts are not delivered to Slack.
- **The stream iterator-age alarm now receives data.** The two per-mapping alarms used an `EventSourceMapping` dimension that `AWS/Lambda` `IteratorAge` does not publish, so they never got a datapoint and `treat_missing_data = "notBreaching"` hid that. They are replaced by a single `<environment>-interlock-stream-router-iterator-age` alarm on the function-level metric (dimension `FunctionName` only), which covers both the control and joblog streams. `terraform apply` destroys the two old alarms and creates the new one. Because this is the first time the alarm gets data, a stream backlog of 5 minutes or more now pages on the first evaluation after apply.
- **SLA schedule names now fit the EventBridge Scheduler limits.** Scheduler names must be 1-64 characters of `[0-9a-zA-Z_.-]`, but `<pipelineID>-<scheduleID>-<date>-sla-<suffix>` had no cap and kept every character of the pipeline ID, so a long or unusual pipeline ID made `CreateSchedule` fail and the SLA warning and breach alerts for that run were lost without notice. Create and cancel now share one `lambda.SLAScheduleName` helper, and the pipeline ID rule above keeps every name it builds within those limits.
- **Stream router reported the wrong batch-item identifier** — `stream.HandleStreamEvent` returned `ItemIdentifier: EventID` in `BatchItemFailures`. AWS matches `ReportBatchItemFailures` identifiers against the DynamoDB stream record `SequenceNumber`, so an unrecognised identifier made Lambda treat the entire batch as failed and re-drive every record in it. The handler now returns `record.Change.SequenceNumber` and logs it alongside the event ID (the event ID remains the log correlation ID) — but partial-batch reporting only checkpoints at the *first* failed record and still re-drives that record and every later one in the batch, so this narrows the replay of non-idempotent `WriteJobEvent`/`PublishEvent` side effects rather than eliminating it; making those handlers idempotent remains a separate follow-up.
- **Stream router acted on REMOVE records** — `handleRecord` never checked `EventName`, so a deleted row reached the write handlers with a nil `NewImage`. Deleting a `RERUN_REQUEST#` row (those rows have no TTL and are never cleaned up) started a rerun with `reason="manual"`; deleting a `SENSOR#` row — including the `postrun-baseline#` delete the rerun path issues itself — could publish `POST_RUN_PASSED`/`POST_RUN_FAILED` verdicts from absent data; and 30-day `JOB#` TTL expiries invoked the function for nothing. REMOVE records are now skipped and logged at info with the key, event ID and whether the delete came from DynamoDB's TTL process, with the single exception of `SK = CONFIG`, which still invalidates the config cache so a deleted pipeline config drops out immediately. Defense in depth: both DynamoDB event source mappings in `deploy/terraform/lambda.tf` now carry `filter_criteria` (writes always pass; REMOVE passes only for the control table's `CONFIG` row), so filtered records are neither delivered nor billed. **Operators must run `terraform apply` to pick up the new event source mapping filters**; the handler guard protects deployments in the meantime. `deploy/localstack/deploy.py` applies the equivalent `FilterCriteria`, but only when a mapping is created — tear down and redeploy an existing LocalStack stack to pick them up.

## [0.10.0] - 2026-09-12

### Added

- **LocalStack Community deployment** (`deploy/localstack/`) — Build script (auto-detects host arch arm64/amd64), Python deploy script using boto3 to match production Terraform resource shape, and Makefile targets for full local E2E testing. Creates DynamoDB tables with streams, EventBridge custom bus + rules, SQS alert queue, 6 Lambda functions, Step Functions state machine, IAM dummy roles, and event source mappings. Enables running real interlock Lambdas locally against LocalStack for integration testing without AWS costs.
- **`SKIP_SCHEDULER` env var** — `sla-monitor` Lambda no-ops EventBridge Scheduler calls (`CreateSchedule`, `DeleteSchedule`) when `SKIP_SCHEDULER=true`. Allows deployment to LocalStack Community (Scheduler is Pro-only) while preserving production behavior. Guards applied in `internal/lambda/sla_monitor.go`, `internal/lambda/sla/`, and `internal/lambda/watchdog/`.
- **Dead-letter queue subsystem** (`internal/dlq/`) — Typed error classification (transient vs permanent), SQS routing with slog fallback when SQS is unreachable, ULID-based record IDs, and per-record metrics counter interface. Includes no-op router for testing and dry-run modes.
- **Stream batch handler** (`internal/handler/`) — Implements AWS `ReportBatchItemFailures` for partial batch processing. Uses `SequenceNumber` (not `EventID`) per AWS contract. Enforces accounting invariant: processed + dlq_routed + batch_failures == total.
- **Lambda context middleware** (`internal/aws/lambda/`) — Derives `context.WithTimeout` from Lambda's remaining execution time minus a configurable safety buffer (default 500ms). Floors at 50ms to prevent zero/negative timeouts.
- **OpenTelemetry initialization** (`internal/telemetry/`) — OTLP gRPC trace and metric exporters with graceful no-op fallback when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset. Defines 6 application metrics: records processed, stage duration, rules evaluated, DLQ routed, worker pool active, circuit breaker state.
- **Structured logging with correlation IDs** (`internal/telemetry/`) — slog handler wrapper that injects `correlation_id` from context into every log record. JSON output with source location.
- **Circuit breaker for HTTP evaluators** (`internal/client/`) — Wraps external HTTP calls with `sony/gobreaker`. Configurable trip thresholds, nil-safe defaults, and state introspection.
- **Exponential backoff retry** (`internal/resilience/`) — Context-aware retry with jitter clamping to [0, 1], proper `time.NewTimer` cleanup (no timer leaks), and configurable max retries/delay.
- **Bounded worker pool** (`internal/concurrency/`) — Thin wrapper around `errgroup` + `semaphore.Weighted` for bounded concurrent processing within Lambda executions.
- **CI quality gates** — Makefile `audit` target using `golangci-lint` (reads `.golangci.yml`) and `go test -race`. GitHub Actions workflow updated to use `make audit` as a blocking gate.
- **DLQ audit tracker** (`internal/audit/`) — Record lifecycle tracking with RWMutex-protected state map, valid transition enforcement (PENDING→ACKED/REJECTED), duplicate detection, and reconciliation reporting for data loss detection.
- **Hardening config** (`internal/config/`) — Centralized env-var-based configuration for timeouts, worker pools, DLQ, and circuit breaker thresholds with validation at startup.
- **Pipeline stage decorators** (`internal/pipeline/`) — Composable `WithTimeout` and `Compose` decorators for pipeline stage wrapping. Context pre-cancellation check avoids unnecessary goroutine allocation.
- **Serverless health checks** (`internal/handler/`) — EventBridge `__ping__` handler with pluggable `HealthChecker` interface returning provider connectivity status.
- **CPU profiler** (`internal/handler/`) — Captures pprof CPU profiles on `__profile__` payloads and uploads to S3 with collision-resistant timestamped keys.
- **Integration and fault injection tests** (`tests/integration/`) — Mixed-batch stream processing, DLQ router failures, circuit breaker state transitions, retry exhaustion, and context cancellation under fault injection.

### Changed

- **HTTP trigger retry** — `ExecuteHTTP` retries transient failures (5xx, network errors) with exponential backoff. Request body resets between attempts. Permanent errors (4xx) skip retry.
- **Alert dispatcher circuit breaker** — Slack HTTP client wrapped with gobreaker to prevent cascade during Slack outages.
- **Stream router correlation IDs** — Per-record correlation IDs injected into context for structured log tracing across services.
- **Telemetry flush per invocation** — OTel providers flush (not shutdown) per Lambda invocation to survive environment reuse across warm starts.

### Fixed

- **SFN SLA cancel states no longer fail with `States.Runtime`** — `CancelSLASchedules` and `CancelSLAOnCompleteTriggerFailure` referenced `$.config.sla.deadline`, `$.config.sla.expectedDuration`, `$.config.sla.maxDuration` and `$.sensorArrivalAt`, but `types.SLAConfig` marks every field `omitempty`, so an absolute-only SLA never emitted `maxDuration`, a relative-only SLA never emitted `deadline`, and `sensorArrivalAt` was usually absent. The resulting `States.Runtime` is not retriable and is not caught by `Catch: ["States.ALL"]`, so every SLA-configured execution failed after `CompleteTrigger`. The SFN input now uses dedicated `SFNInput`/`SFNConfig`/`SFNSLA` types that always emit those keys, and both cancel states now forward `timezone` so `handleSLACancel` recomputes deadlines in the configured zone.
- **Trigger run IDs are extracted for every trigger type** — `ExtractRunID` only matched `runId`, `jobRunId`, `glue_job_run_id`, `executionArn`, `stepId` and `dagRunId`, while the step-function, EMR, EMR Serverless, Airflow and Databricks executors emit `sfn_execution_arn`, `emr_step_id`, `emr_sl_job_run_id`, `airflow_dag_run_id` and `databricks_run_id`. The empty run ID was then omitted from the Lambda result and the `CheckJob` state raised `States.Runtime` after the external job had already been launched. `OrchestratorOutput.RunID` is now always marshaled and an empty metadata map takes the sync-sentinel path.
- **Absolute SLA deadlines are anchored to the execution date** — `CalculateAbsoluteDeadline` rolled an explicit execution date forward by 24h (or 1h for `:MM` deadlines) whenever the deadline had already passed. `sla-monitor` `cancel` therefore published `SLA_MET` for runs that finished late, the `reconcile` breach branch was unreachable, and the watchdog scheduled breach alerts a day late. Roll-forward now applies only when no execution date is supplied, and uses `AddDate` so the wall-clock time survives DST transitions.
- **Orchestrator evaluate/trigger results satisfy the state machine contract** — `handleEvaluate` returned a result without `status` on storage failures, so the `IsReady` Choice dereferenced a missing `$.evaluateResult.status`; it now always emits a status. `handleTrigger` returned a partial result with a nil error on configuration failures, so `HasTriggerResult` saw `IsPresent=true` and `CheckJob` dereferenced a missing `runId`, killing the execution with the `TRIGGER#` lock stuck in `RUNNING` until TTL; those paths now return a Lambda error so `Trigger`'s Retry/Catch routes to `TriggerRetryExhausted`, which releases the lock.
- **Time-dependent tests** — `TestSLAMonitor_Calculate_ReturnsRFC3339` (failing since 2026-06-16), `TestSLAMonitor_Reconcile_ReturnsDeadlines`, `TestSLAMonitor_Cancel_RecalculatesWhenTimesNotProvided` and three watchdog proactive-SLA tests now inject `Deps.NowFunc` instead of relying on the wall clock.
- **SLA cancel verdict uses the T+1 date for sensor-triggered daily pipelines** — `handleSLACancel` and the dry-run SLA projection recomputed the absolute deadline from `input.Date`/`date` (the data date D), but sensor-triggered daily pipelines run T+1 — data for date D arrives on D+1, and the watchdog's proactive SLA scheduling already anchors the deadline to D+1. Now that an explicit date no longer rolls forward when its deadline has passed, `cancel` published a false `SLA_BREACH` for pipelines that finished on D+1 before their deadline. The new `internal/lambda.ResolveSLADate` centralizes the watchdog's T+1 rule (cron pipelines and hourly `:MM` deadlines are unaffected) and is now shared by the watchdog, `sla.handleSLACancel`, and `stream.publishDryRunSLAProjection`. `orchestrator.handleEvaluate` storage failures (`GetConfig` error, config not found, `GetAllSensors` error) are now also logged at error level instead of surfacing only in the returned `status: "error"` payload.

**Release notes**: verdicts for pipelines with a non-UTC `sla.timezone` are now computed in that zone; absolute SLA deadlines no longer roll forward to the next day when past; in-flight executions started before deploy are unaffected by the state-machine change; the sla-monitor `reconcile` mode has no production invoker today and is not a safety net for deadlines missed while the watchdog was down.

### Dependencies

- `go.opentelemetry.io/otel` v1.43.0 (traces + metrics)
- `go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc` (OTLP gRPC trace export)
- `go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc` (OTLP gRPC metric export)
- `github.com/sony/gobreaker` (circuit breaker)
- `github.com/oklog/ulid/v2` (DLQ record IDs)
- `golang.org/x/sync` (errgroup + semaphore for worker pool)

## [0.9.4] - 2026-03-29

### Refactored

- **Split `internal/lambda/` into handler-aligned sub-packages** — Monolithic package replaced with focused sub-packages: `orchestrator/`, `stream/`, `watchdog/`, `sla/`, `alert/`, `sink/`.
- **Extracted shared utilities into focused root files** — Common logic moved to dedicated files: publish, date, exclusion, sensor, schedule, config, terminal.
- **Trigger config registry** — Replaced `buildTriggerConfig` switch statement with generic registry map (`trigger_registry.go`).
- **SLA deadline calculations wired through `pkg/sla/`** — Pure functions for SLA deadline resolution, decoupled from Lambda handler context.

### Added

- **`pkg/sla/` package** — Pure SLA deadline calculation functions usable across packages.
- **`PipelineConfig.DeepCopy()` method** — Safe config cache isolation without JSON marshal/unmarshal roundtrip.
- **`EventWatchdogDegraded` event type** — Watchdog health observability event for degraded-state detection.
- **Smoke tests for all 6 `cmd/lambda/` packages** — `ValidateEnv` coverage for every Lambda entry point.

### Fixed

- **`HandleWatchdog` silent error suppression** — Now returns aggregate errors via `errors.Join` instead of silently returning nil.
- **`HandleWatchdog` degraded-state signaling** — Publishes `WATCHDOG_DEGRADED` event when checks fail.
- **Config cache isolation** — Uses typed `DeepCopy()` instead of JSON marshal/unmarshal roundtrip, eliminating silent data loss on unexported fields.

## [0.9.3] - 2026-03-14

### Changed

- **Shared HTTP client construction (DRY-2)** — Extracted `resolveHTTPClient()` replacing identical 7-line blocks in `ExecuteHTTP` and `ExecuteAirflow`.
- **Shared SLA schedule creation loop (DRY-3)** — Extracted `createSLASchedules()` replacing duplicated warning/breach schedule loops in watchdog and sla-monitor.
- **Split watchdog.go into focused files** — 1079-line monolith split into 5 files by domain: stale triggers, missed schedules, SLA alerting, and post-run monitoring (~200 lines each).

### Security

- **Encryption at rest for DynamoDB and SQS** — All DynamoDB tables now explicitly enable server-side encryption. SQS queues (alert queue, alert DLQ, stream-router DLQs) now use KMS encryption. New optional `kms_key_arn` variable for custom CMK when full key control and rotation are needed; defaults to AWS-managed keys (free, no configuration required).
- **SSRF protection on trigger HTTP clients** — Custom `http.Transport` with dial-time IP validation rejects connections to private, loopback, link-local, and multicast addresses. Protects HTTP, Airflow, and Databricks triggers against targeting internal endpoints.
- **EventBridge PutEvents partial failure detection** — `publishEvent` now checks `FailedEntryCount` on the response. Previously, partial failures were silently discarded.
- **Command trigger shell injection eliminated** — Replaced `sh -c` with direct `exec.CommandContext` + `strings.Fields` argument splitting. No shell interpretation of pipes, redirects, or variable expansion.

## [0.9.2] - 2026-03-13

### Fixed

- **Drift detection silently skipped zero values (BUG-1)** — `ExtractFloat` returned 0 for both missing keys and actual zero values, causing the `prevCount > 0` guard to silently skip legitimate transitions like 5000→0 or 0→5000. New `ExtractFloatOk` distinguishes absent from zero. Shared `DetectDrift` function consolidates 3 duplicated drift comparison sites.
- **RemapPerPeriodSensors map mutation during range (BUG-2)** — Adding keys during `range` iteration over a Go map is nondeterministic per the spec. Staging map now collects additions, merged after iteration.
- **Orphaned rerun burns retry budget (BUG-3)** — `handleRerunRequest` wrote the rerun record before acquiring the trigger lock. If lock acquisition failed, the rerun record was left orphaned and permanently consumed retry budget. Reordered to lock-first, then write.
- **Stream router discarded partial batch failures (BUG-4)** — `HandleStreamEvent` returned a single error, causing Lambda to retry the entire batch. Now returns `DynamoDBEventResponse` with per-record `BatchItemFailures` for partial retry via `ReportBatchItemFailures`.
- **SLA_MET published when pipeline never ran (BUG-5)** — `handleSLACancel` published SLA_MET regardless of whether a trigger existed. Now checks for trigger existence first.
- **Trigger deadline used SLA timezone instead of schedule timezone (BUG-6)** — `closeSensorTriggerWindow` read timezone from `cfg.SLA.Timezone` instead of `cfg.Schedule.Timezone`. Falls back to SLA timezone if schedule timezone is not set.
- **Validation mode case-sensitive (BUG-8)** — `EvaluateRules` matched mode with `switch mode` so "any" fell through to the default ALL branch. Now uses `strings.ToUpper(mode)`.
- **Epoch timestamp unit mismatch in rerun freshness (BUG-9)** — `checkSensorFreshness` compared raw epoch values without normalizing units. Timestamps below 1e12 (seconds) are now converted to milliseconds.
- **Post-run baseline field collision (BUG-10)** — Baseline was stored as a flat map, so two rules with the same field name overwrote each other. Now namespaced by rule key. Clean break: existing flat baselines self-heal on next pipeline completion.
- **publishEvent errors silently discarded in SLA reconcile (CQ-5)** — Replaced `_ = publishEvent(...)` with error-logged calls.

### Security

- **lambda_trigger_arns default changed to [] with precondition (SEC-1)** — Wildcard default removed; explicit ARN list required when triggers are enabled.
- **Slack plaintext token deprecation warning (SEC-2)** — Terraform `check` block warns at plan time when plaintext token is used without Secrets Manager.
- **Trigger IAM policy scoping (SEC-4)** — New variables `glue_job_arns`, `emr_cluster_arns`, `emr_serverless_app_arns`, `sfn_trigger_arns` (all default `[]`) with preconditions requiring non-empty values when the corresponding trigger is enabled.
- **EventBridge bus resource policy (SEC-5)** — Restricts PutEvents to Lambda execution roles only.

## [0.9.1] - 2026-03-13

### Added

- **`DRY_RUN_COMPLETED` event** — terminal event that closes the dry-run observation loop for each evaluation period. Published after `DRY_RUN_WOULD_TRIGGER` and `DRY_RUN_SLA_PROJECTION`, carrying the SLA verdict (`met`, `breach`, or `n/a`) so operators can see each period resolve.

### Fixed

- **Dry-run pipelines could start Step Function executions via rerun and job-failure paths** — `handleRerunRequest` and `handleJobFailure` did not check `cfg.DryRun` before calling `startSFNWithName`, allowing rerun requests and job failure retries to start real SFN executions for dry-run pipelines. Added dry-run guards in both handlers and defense-in-depth in `startSFNWithName` to suppress execution unconditionally. Watchdog reconciliation loop now skips dry-run pipelines to prevent orphaned trigger locks.
- **Watchdog scheduled real SLA alerts for dry-run pipelines** — `scheduleSLAAlerts`, `detectMissedSchedules`, `detectMissedInclusionSchedules`, `checkTriggerDeadlines`, `detectMissingPostRunSensors`, and `detectRelativeSLABreaches` all iterated over dry-run pipelines without checking `cfg.DryRun`. This caused EventBridge Scheduler entries for SLA_WARNING/SLA_BREACH, SCHEDULE_MISSED events, and RELATIVE_SLA_BREACH alerts to fire for observation-only pipelines. Added `cfg.DryRun` guard to all six watchdog functions.
- **Duplicate `JOB_COMPLETED` alerts for polled jobs** — `handleCheckJob` in the orchestrator published `JOB_COMPLETED` when polling detected success, but the stream-router's `handleJobSuccess` also published the same event when the JOB# record arrived via DynamoDB stream. Removed the orchestrator emission; the stream-router is now the single canonical source for `JOB_COMPLETED` across all job types.

## [0.9.0] - 2026-03-12

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

[0.10.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.10.0
[0.9.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.9.0
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
