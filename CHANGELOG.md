# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Trigger auth tokens from Secrets Manager.** The Terraform module's `trigger_secrets_arn` names a secret holding a JSON object of `INTERLOCK_` variables; the orchestrator reads it at cold start (env `TRIGGER_SECRETS_ARN`) and sets each pair in its environment, and a secret it cannot use stops the cold start. Its role gets `secretsmanager:GetSecretValue` on that secret only. `orchestrator_env` sets plain `INTERLOCK_` variables for non-secret values; the secret wins on a shared name.
- **The `airflow` trigger supports Airflow 3.** `apiVersion: 3` starts and polls DAG runs on the `/api/v2` REST API, sending `"logical_date": null` with the `conf`. Unset or `2` keeps the `/api/v1` calls. Validation rejects any other value. Runs started before this change keep polling `/api/v1`.

### Changed (BREAKING)

- **A `lambda` job with no `payload` now receives a JSON event** (the run object below) instead of an empty one, and an `airflow` job with no `body` a DAG run `conf`.
- **Step function input and EMR step arguments gain the date keys**, replacing a configured key of the same name. EMR step arguments are now sent in key order.
- **`${INTERLOCK_PIPELINE_ID}`, `${INTERLOCK_SCHEDULE_ID}`, `${INTERLOCK_DATE}` and `${INTERLOCK_RUN_ID}` in a `lambda` payload or `airflow` body** expand to the run's values, JSON-escaped, instead of environment variables of those names.
- **The `databricks` trigger starts the configured job with `run-now`, and `jobId` is that job's numeric ID**, quoted as a string so an ID above 2^53 stays exact. It called `runs/submit` with `jobId` as the run name, which needs a task definition and could not start an existing job, and the documented `body` field was never read. `arguments` now go out as `job_parameters` instead of `notebook_params`, with `par_day` and, for hourly dates, `par_hour` added, so the job must define those job-level parameters. Validation rejects a `jobId` that is missing, unquoted or not a positive base-10 integer, and the trigger checks it again before any request.
- **The `SFN_TIMEOUT` detail key `ttlExpired` is now `staleAt`** (RFC3339 UTC), the trigger row's stale deadline, and its `actionHint` reads "step function passed its stale deadline: check SFN execution history".
- **`sla.deadline` needs `expectedDuration`, and its format is checked.** A config with a deadline and no `expectedDuration`, or one that is not a Go duration, used to load and then got no `SLA_WARNING`, `SLA_BREACH` or `SLA_MET` at all. It is now rejected as `CONFIG_INVALID`, as are a zero or negative `expectedDuration`, a deadline that is not `HH:MM` or `:MM` (a one-digit hour such as `9:00` used to be accepted), and an `sla` block with neither `deadline` nor `maxDuration`.
- **Every pipeline must set `schedule.trigger` with a `cutoff`,** its latest automatic start. A config without one is rejected as `CONFIG_INVALID`. Past the cutoff the date is closed with `SENSOR_DEADLINE_EXPIRED` and only a `RERUN_REQUEST` runs it, so no date starts on its own at an arbitrary age. For a first run, an evaluation window that runs out now releases the date until the cutoff, and `VALIDATION_EXHAUSTED` is published only for a rerun.
- **The cutoff applies to every run key.** A date-only sensor on a pipeline with an hourly `:MM` cutoff, or an hourly sensor under a daily `HH:MM` cutoff, used to have no cutoff: it could start at any age and, when its evaluation window ran out, ended with `VALIDATION_EXHAUSTED`. A daily cutoff now covers the hourly keys of its date, and an hourly `:MM` cutoff falls MM minutes after a daily key's day ends. For hourly data under a daily cutoff, set `cutoffDayOffset` to 1 or more; with 0 and a `14:00` cutoff, hours `T13` to `T23` are closed with `SENSOR_DEADLINE_EXPIRED`.
- **With `sla.deadline`, the cutoff may not be earlier than the SLA breach time.** A pipeline keeps starting on its own until its SLA is missed; after the cutoff it only alerts and waits for a `RERUN_REQUEST`. The two times are compared in their own time zones on four dates across the year, so a cutoff that falls behind the deadline in some season is rejected. The check covers the run keys the deadline applies to: the daily key for a daily `HH:MM` deadline, the hourly keys `T00` and `T23` for an hourly `:MM` one. With the cutoff and the deadline in one zone, an hourly deadline under a daily cutoff needs `cutoffDayOffset` 1 or more. A pipeline with only `maxDuration` needs a cutoff but has no such check, since its breach time depends on when the data arrives.
- **`schedule.evaluation.window` and `interval` are checked.** A value that was not a Go duration used to fall back to the default without a word, and an interval under one second made the state machine evaluate in a tight loop. Both must now be whole seconds of at least `1s`, the interval may not exceed the window, and the window may allow at most 1000 evaluations (the Step Functions history limit).
- **`schedule.evaluation.window` plus the job poll window must fit the execution timeout.** With a 6h window and the default 4h `sfn_timeout_seconds` the execution used to time out mid-evaluation, with no `EVALUATION_RELEASED`, and the date was closed with `SFN_TIMEOUT`. Such a config is now rejected as `CONFIG_INVALID`; raise `sfn_timeout_seconds`, or shorten the window or `jobPollWindowSeconds`. The LocalStack deploy uses a 4h timeout.
- **`enable_emr_serverless_trigger` needs `emr_serverless_execution_role_arns`**, the IAM role ARNs job runs execute under, or the plan fails, and an `emr-serverless` job needs `executionRoleArn` and `entryPoint` (see Fixed).
- **Trigger conditions and rules are checked when the config loads.** An unknown operator (`eq`), a threshold that is not a number, an `age_lt` value that is not a Go duration (`1d`), `equals` without a value, a missing key or field, and `validation.trigger: ANY` with no rules used to load; the pipeline then never started, or no date ever passed validation. Each is now `CONFIG_INVALID`. Two forms that did run are rejected as well: a key containing `#` or equal to one of Interlock's own row names, which matches one fixed period or Interlock's own rows, and a `validation.trigger` other than `ALL` or `ANY`, which ran as `ALL` (so did a typo such as `ANYY`).
- **`schedule.time` must be `HH:MM`.** A value that did not parse was ignored, so `IRREGULAR_SCHEDULE_MISSED` fired at local midnight instead of at that time. It is now rejected, as is `schedule.time` without `schedule.include`. A one-digit hour such as `9:00`, which did work, is rejected too.
- **An airflow `url` or databricks `workspaceUrl` with a query, a fragment or user info is rejected.** Such a base swallowed the API path, so requests carrying the token went to the wrong path on that host. User info does not change the path, but it was stored in the clear with the run in the Step Functions execution history; send credentials in `headers`. A value that is not an `https` URL with a host is rejected too.
- **Airflow and Databricks base URLs must use `https`, and an `http` trigger `url` may not hold user info.** A plain `http://` base sent the bearer token in clear text; user info in an `http` trigger url reached the job log and the Step Functions history when a request failed. A failed `http` trigger request now reports its URL without the query, fragment or user info. An `http` trigger `url` that is not an `http` or `https` URL with a host is rejected too.
- **SLA alerts follow the trigger row.** While a date's row is `RUNNING`, `SLA_WARNING`, `SLA_BREACH`, `RELATIVE_SLA_WARNING` and `RELATIVE_SLA_BREACH` go out at their times whatever the job log says. A job that has reported success can still get them until its run is `COMPLETED`, normally within one job check interval (60s by default). If the final status cannot be recorded, or the step that ends a date after a failed launch fails after writing its job row, the row stays `RUNNING` and alerts due meanwhile go out until the watchdog closes the stale row (by default 4.5h after the latest attempt started). Where the cancel step ran, the watchdog has to schedule `SLA_WARNING` and `SLA_BREACH` again, so one due before its next pass is not sent.
- **A retry after a job failure waits before it starts the job again.** Retries used to start at once, so a fault lasting ten minutes could use up `maxRetries` within minutes. Retry N now waits 60 seconds doubled per retry, at most 15 minutes (1, 2, 4, 8, 15, ... minutes), for permanent failures too. The date stays `RUNNING` meanwhile. The longest wait a pipeline's budgets allow now counts toward the execution timeout: `schedule.evaluation.window` plus the job poll window plus that wait must fit `sfn_timeout_seconds`, or the config is rejected as `CONFIG_INVALID`. The check runs each time a stored config is loaded, so one whose two windows come within that wait of the timeout (60 seconds at the defaults, 15 minutes from 5 retries up) is rejected at the next load unless both `maxRetries` and `maxCodeRetries` (default 1) are 0. Apply the whole deploy before runs start: a run started by the previous stream-router or watchdog on the new state machine fails at its first state, and its date stays `RUNNING` until `SFN_TIMEOUT`.
- **The watchdog creates a run date's SLA schedules when its warning time is at most 6 hours away**, instead of as soon as the run date is within two days (UTC). It used to issue two `CreateSchedule` calls per pass for every pending run date in that window, up to 146 for an hourly pipeline in UTC with its rows written ahead, which could use up its 60 second timeout. If no pass schedules a date in the 6 hours before its warning (a watchdog outage, a rejected config, throttling, a `watchdog_schedule` slower than 6 hours), its `SLA_WARNING` is not sent, nor its `SLA_BREACH` if that lasts until the breach. A change to `sla.deadline`, `expectedDuration`, `timezone` or `deadlineDayOffset` now reaches every date not yet scheduled; a date already scheduled keeps its old times, so delete its two schedules to move them, at least 5 minutes after the change.

### Removed

- **The sla-monitor Lambda no longer accepts the `schedule`, `reconcile` and `calculate` modes**, and rejects them as unknown. Nothing in the deployment sent them: the watchdog creates the SLA schedules and Scheduler invokes `fire-alert`.
- **The worker pool, stage decorators, Lambda context middleware, health-check, profiler and stream batch handler packages added in 0.10.0 are gone.** They were never wired into a Lambda.
- **The dead-letter queue and audit tracker packages added in 0.10.0 are gone.** Nothing imported them once the stream batch handler went, and `github.com/oklog/ulid/v2` left the dependencies with them.
- **`schedule.calendar` is gone.** It was never read; list the dates in `exclude.dates`. A config that still sets it loads, and the key is ignored.
- **The README no longer lists six application metrics or a `correlation_id` log field.** Neither was ever emitted. The unused correlation-ID logger, the metric name constants and the `Tracer` and `Meter` accessors behind them are gone too.
- **The orchestrator no longer reads `STAGE_TIMEOUT`, `SHUTDOWN_BUFFER`, `WORKERS_PER_STAGE`, `DLQ_QUEUE_URL`, `DLQ_MAX_RETRIES`, `CB_TRIP_AFTER` or `CB_TIMEOUT`.** Nothing used them, and an invalid value no longer stops it at cold start.

### Fixed

- **`POST_RUN_SENSOR_MISSING` fired although the sensor had arrived unless its `updatedAt` was epoch milliseconds, missed sensors written under a per-period key, and never fired when the sensor did not exist at completion.** The check no longer compares `updatedAt` with the completion time: a post-run sensor has arrived when it exists and either differs from what it was when the run completed or did not exist then, and `<key>#<date>` sensors are matched.
- **A post-run sensor first written after the run completed (a per-period key, or a sensor's first ever write) had no baseline, so later changes to it were never reported as `POST_RUN_DRIFT` and never requested a rerun.** Its first value after completion is now the baseline for later writes. A dry-run pipeline does the same and reports `DRY_RUN_DRIFT`.
- **`IRREGULAR_SCHEDULE_MISSED` compared inclusion dates with the UTC date, so west of UTC it fired the evening before the date and was then never sent for a real miss; east of UTC it waited for UTC midnight.** Dates are now compared in `schedule.timezone`.
- **When an alert could not be published, the watchdog still closed the stale run as `FAILED_FINAL`, or still wrote the once-per-date marker for a missed inclusion date or a missing post-run sensor, so the alert was lost for good.** The run now stays `RUNNING` and the marker is not written, and the next pass tries again.
- **A run whose execution died could be deleted by DynamoDB before the watchdog reported it: no `SFN_TIMEOUT` was sent and the watchdog then started the date a second time.** A running trigger row now carries its stale deadline as `staleAt` and no `ttl`, so DynamoDB never deletes it and a row its execution left `RUNNING` is closed only by the watchdog. A pipeline switched to dry-run has its leftover running row closed the same way.
- **A run that did not complete (it ended `FAILED_FINAL`, its final status could not be recorded, or its status could not be read) was reported as `SLA_MET` when it ended before its breach time.** `SLA_MET` is now published only for a date whose trigger row is `COMPLETED`; `SLA_BREACH` is still published when the breach time has passed.
- **A run the watchdog started (a recovered trigger, or a sensor that arrived on an excluded day) had no first-arrival record, so it got no `RELATIVE_SLA_WARNING`, no `RELATIVE_SLA_BREACH` and, without a `deadline`, no SLA verdict.** The run now records the arrival when the watchdog starts it, so the SLA verdict counts `maxDuration` from that start, and `RELATIVE_SLA_WARNING` and `RELATIVE_SLA_BREACH` fire for the run dates the watchdog checks.
- **After a launch ran out of retries (`RETRY_EXHAUSTED`) the date's trigger row was deleted, so a later sensor write started it again, and the watchdog started it again 30 days later when its job rows expired.** The date now ends `FAILED_FINAL`: nothing starts it again automatically, a `RERUN_REQUEST` runs it, and it is no longer also reported as `SCHEDULE_MISSED`. A `RETRY_EXHAUSTED` that cannot be published fails the step and is retried; if it keeps failing, the row stays `RUNNING`, a `RERUN_REQUEST` is rejected as in progress, and the watchdog closes the date with `SFN_TIMEOUT` once its `staleAt` passes. A launch that runs out of retries after its breach time now also gets `SLA_BREACH` from the cancel step, as a job that fails after its breach time does, so it can follow the scheduled `SLA_BREACH`.
- **A rerun whose execution could not start deleted the date's trigger row and still spent the rerun budget, so the retried request was rejected and the date lost its record.** The date now keeps its status, no budget is spent, and a retry of the request can start the rerun.
- **A rerun request for a date whose run is still in progress started a second execution beside it.** It is now rejected with `RERUN_REJECTED` and spends no budget; request it again once the run ends. A run whose execution ended without closing its row (it timed out or was stopped, or the step that would have closed the row failed) stays `RUNNING` until the watchdog's stale-trigger check closes it with `SFN_TIMEOUT`, once its stale deadline (`staleAt`) has passed: `sfn_timeout_seconds` plus 30 minutes from the run's latest attempt (4h30m by default), plus up to one watchdog interval. Dry-run pipelines report the same rejection.
- **A sensor whose key merely began with the trigger key or a post-run rule key (`orders-count` beside `orders`) was treated as that sensor: it could start the run, was handled as late data, and its own post-run checks never ran.** A sensor now matches a key only when it is that key or that key followed by `#` and a period, such as `orders#2026-03-10`.
- **A rejected or accepted rerun, or a late-data row, no longer hides the date's last run from the circuit breaker and the late-data check.** Asking twice no longer gets a rerun past the breaker, and late data after a rejected rerun is reported again. Late-data and drift reruns are not sent through the breaker.
- **The rerun circuit breaker rejected every rerun of a successful date when its sensors' `updatedAt` values were RFC3339**, the documented format. It and `age_lt` now read RFC3339, epoch seconds and epoch milliseconds (a number or a string of digits), and the breaker treats an `updatedAt` it cannot read as absent, with a warning in the stream-router log.
- **With `kms_key_arn` set, the orchestrator, watchdog, sla-monitor and event-sink roles had no grant on the key**, so they could not read or write the encrypted tables. Only the stream-router and alert-dispatcher roles were granted it, for their queues. Every Lambda role now gets `kms:Decrypt` and `kms:GenerateDataKey` on the key, and `kms_key_arn` must now be a key ARN: an alias ARN, key ID or wildcard fails at plan.
- **Every job learns the date it runs for, so a rerun or retry processes its own date.** A `step-function` job gets `par_day` (`YYYYMMDD`) and, for an hourly date, `par_hour` (`HH`) as top-level keys of its execution input, and an `emr` step gets `--par_day=YYYYMMDD` and `--par_hour=HH`, like `glue`. A `lambda` payload and an `airflow` body expand the run identity variables; with none configured, the function's event or the DAG run's `conf` is the object the `http` default body is (`par_day`, `par_hour` for hourly dates, `pipelineId`, `scheduleId`, `date`, `runId`). An `emr-serverless` job gets `--par_day` and `--par_hour` as entry point arguments.
- **The `emr-serverless` trigger can start a job run.** It called `StartJobRun` with only the application ID and job name, so EMR Serverless rejected every start for want of an execution role and a job driver. A job now names its runtime role in `executionRoleArn` and its `entryPoint`, which runs as a `spark-submit` with the optional `entryPointArguments` and `sparkSubmitParameters`; a config missing either fails before the call. The client token is the SHA-256 of the run ID, so a start the Trigger state retries reuses the token and EMR Serverless does not start a second job run, while a failure retry or rerun, with its own run ID, starts a new one. The Terraform module's new `emr_serverless_execution_role_arns` lists the runtime roles, and the orchestrator gets `iam:PassRole` on them, only when passed to `emr-serverless.amazonaws.com`.
- **Alerts never reached Slack on a default deploy.** The alert queues were encrypted with the AWS managed SQS key, which EventBridge cannot use, so it could not deliver alert events to them. They now use SQS-managed encryption (SSE-SQS) unless `kms_key_arn` is set; a customer key needs the EventBridge key policy statement shown in the Terraform deployment docs.
- **Every `step-function` run ended as a timeout.** The orchestrator was granted `states:DescribeExecution` on the state machine ARNs, but AWS authorizes it on the execution ARN, so every status poll was denied. It is now granted on `execution:<name>:*` for each state machine (an alias or version suffix is dropped), and `sfn_trigger_arns` rejects anything but a state machine, alias or version ARN. `emr-serverless:GetJobRun` had the same mistake, granted on the application rather than its job runs, and is now granted on `<application>/jobruns/*`.
- **The `step-function` docs showed an `input` field that was never read.** The config field is `arguments`, a map whose entries become top-level keys of the execution input; the docs now say so. Nothing was removed.
- **An invalid `airflow` body no longer quotes part of an expanded variable in the error.** The body is parsed after `${...}` expansion, and the JSON decoder's message could quote a character of an `INTERLOCK_` value such as a token; the error is now fixed text.
- **Trigger errors no longer carry the tokens their request was sent with.** An endpoint that echoes the request in its error response, or a `lambda` function that puts its event in its error, could put an `INTERLOCK_` value into the job log, events and Slack. Every `INTERLOCK_` value of 4 or more characters that `${...}` put into the request is now replaced with `[REDACTED]` in that request's error and in status poll errors, as is, JSON-escaped or URL-encoded, before the error body is cut to its limit. `airflow` and `databricks` error bodies are now truncated to 512 bytes and stripped of control characters like `http` ones, as is a `lambda` function error. A `lambda` payload that is not valid JSON after expansion fails before the invoke with fixed text.
- **Trigger calls no longer follow a redirect that sends the configured headers elsewhere or drops the request.** A redirect is followed only to the same host as written in the URL, including any explicit port, keeping the scheme or going from `http` to `https` (so an upgrade is followed when the URL has no port), and keeping the method. One to another host or port, from `https` to `http`, or one that changes the method (a 301 or 302 turns a POST into a GET without its body) is refused, and the call fails with an error naming the redirect target. An `http` job whose POST (or other non-GET request) is answered with 303 See Other counts as triggered; the redirect is not followed. A GET follows a same-origin 303 like any other redirect. `http`, `airflow` and `databricks` responses are read up to 1 MiB; a larger one fails with `response exceeds 1 MiB`.
- **`airflow` and `databricks` status polls now send the trigger's headers.** They went out with none, so an API that needs a token answered every poll with 401 and the run only ended at the state machine timeout. The headers are expanded and redacted like the trigger's own, and a poll runs only when the run's URL has the scheme and host of the trigger config, so a token is never sent elsewhere. Changing a trigger's URL while a run is polling therefore stops the polls for that run until the state machine times out.
- **A `databricks` status poll's message is cleaned before it reaches the job log.** It now ends with the run's `state_message` after the life cycle and result states, and is stripped of control characters, has `INTERLOCK_` header values redacted and is cut to 512 bytes, like an error body. A `databricks` response that is not a JSON object, or whose `run_id` is not a positive integer, fails with fixed text instead of the JSON decoder's message, which could quote up to 1 MiB of the response.
- **A `lambda` job that outlasts the orchestrator is no longer invoked again.** The orchestrator Lambda timed out waiting on the call, so the Trigger state retried and invoked the function a second time, and CompleteTrigger could then start a retry that invoked it a third. The call now waits until 15 seconds before the orchestrator's deadline (about 100 seconds) and then records one `fail` row with the new category `OUTCOME_UNKNOWN`, which CompleteTrigger never retries: the run ends `FAILED_FINAL`, and the row's `JOB_FAILED` alert carries the message. The function keeps running to its own timeout and its result is not recorded. Longer jobs belong in a `step-function` trigger. CompleteTrigger also no longer takes a failed read of the run's row as a transient failure; the error goes to the task's own retry, so no attempt starts on a guess.
- **An incomplete or misspelt trigger config is now rejected when the config loads**, instead of failing each time the job triggers. Validation decodes `job.config` as the orchestrator does and reports a key the job type does not have (such as `dag_id` for `dagId`) and each empty field the trigger requires: `url` (http), `url` and `dagId` (airflow), `jobName` (glue), `clusterId`, `stepName` and `command` (emr), `applicationId`, `jobName`, `executionRoleArn` and `entryPoint` (emr-serverless), `workspaceUrl` and `jobId` (databricks), `stateMachineArn` (step-function) and `functionName` (lambda). An `http` `method` must be empty or one of `GET`, `HEAD`, `POST`, `PUT`, `PATCH` and `DELETE` in upper case, since a lower-case `get` escaped the redirect rule for `GET`. The orchestrator validates the config it reads at trigger time too, so a row rewritten after a run started fails the Trigger step, naming the reasons, before anything is sent. An `emr-serverless` `--par_day` or `--par_hour` entry point argument with no value after it is rejected, as the run date would overwrite the next flag or be appended as a second copy. The emr docs example gains the `stepName` and `command` it needed, the airflow docs drop a `pollInterval` key that was never read, and the docs now use the decoded key names (`dagId`, `clusterId`, `stateMachineArn`).
- **The `glue` false-success check reads the job insights stream from the group Glue writes it to.** After a run reports `SUCCEEDED`, the orchestrator looks for a `GlueExceptionAnalysisJobFailed` event in the run's job insights stream. It read the group `GetJobRun` reports, `/aws-glue/jobs`, which holds no streams, so it never saw that event; it now always reads `/aws-glue/jobs/logs-v2`. A job with a security configuration or a custom `--continuous-log-logGroup` writes elsewhere and gets no check. Glue 5.0 writes no job insights stream, so its runs get no false-success check either. The failure reason has control characters other than newline and tab removed and is cut to 512 bytes. The driver log `ERROR`/`FATAL` check is removed: it read a `<runId>` stream that Glue does not write, and Glue's driver logs carry an `ERROR` line on most successful runs. A failed CloudWatch Logs query, other than a missing stream or log group, now logs a warning naming the log group, the stream and the AWS error code. A CloudWatch Logs client that cannot be built now logs a warning naming the log group, the run and the error code. Neither fails the run. The orchestrator's `logs:FilterLogEvents` grant no longer includes `/aws-glue/jobs/error`.
- **A `databricks` start that the Trigger state retries no longer starts a second run.** `run-now` went out without an `idempotency_token`, so when the response was lost and the step retried, Databricks started the job again. The token is now the SHA-256 of the run ID in hex (64 characters, the API's limit), so a Trigger state retry, or a redrive of the same execution that runs the Trigger state again, gets the run it already started, while a failure retry, rerun or watchdog start, with its own run ID, starts a new one.
- **The run identity variables can no longer be set as trigger values.** A trigger secret key or an `orchestrator_env` key named `INTERLOCK_PIPELINE_ID`, `INTERLOCK_SCHEDULE_ID`, `INTERLOCK_DATE` or `INTERLOCK_RUN_ID` was accepted and then silently lost to the run's own value; the secret now stops the cold start and the plan fails.
- **Trigger request URLs escape the IDs they carry.** The `airflow` trigger pasted the configured DAG ID and the DAG run ID Airflow returned into its request paths as they were, and the `databricks` status poll did the same with the run ID in its `run_id` query, so a `/`, `?`, `#` or `..` in one could change the path or query sent to the configured host. Each ID now goes out as one escaped path segment or one query value, and the `airflow` trigger refuses a DAG ID or DAG run ID of `.` or `..`, which would still be a dot segment after escaping.
- **A pipeline with only `maxDuration` and `RUN#` rows logged `SLA calculate failed` at ERROR on every watchdog pass.** The watchdog's proactive SLA scheduling now skips a config with no `deadline`, since a relative SLA has no absolute schedule.
- **A rerun of a date that had ended in a launch or validation failure could get no `SLA_WARNING`, `SLA_BREACH` or relative SLA alerts:** whether a run was finished was read from the newest job row, and the rerun was only seen as running when its `rerun-accepted` row had been written. A date's trigger row now decides, and the job log only when there is no trigger row. A restart of a released date is reported as `JOB_TRIGGERED` even when a rejected rerun request was recorded after the release. A rejected rerun request recorded above a finished job's result, on a date with no trigger row, no longer lets the watchdog start the date again, or lets the date be closed with `SENSOR_DEADLINE_EXPIRED` or get SLA alerts.
- **A throttled Lambda call is retried for three minutes before the execution fails.** Outside the Trigger state, a throttled orchestrator or sla-monitor call was retried for 14 seconds and then failed the execution; on Trigger it used up launch attempts (210 seconds at the default). Every state now retries throttling seven times, after 2, 4, 8, 16, 32, 60 and 60 seconds, separately from its other retries.
- **An execution that failed in Evaluate, CheckJob or ValidationExhausted (a crash, or throttling that outlasted its retries) published no event for the date and left the date `RUNNING` until the watchdog's `SFN_TIMEOUT` 4.5 hours later.** It now publishes `INFRA_FAILURE`, whose message carries the error and its cause, and ends the date `FAILED_FINAL`, so a `RERUN_REQUEST` can run it at once. If the job had been launched it may still be running: check it before requesting the rerun.
- **A run whose date was of the other cadence than `sla.deadline` got an `SLA_MET` or `SLA_BREACH` against a time that was not its deadline:** for a daily date under an hourly `:MM` deadline, minute MM of the hour the cancel step ran in, on that date; for an hourly date under a daily `HH:MM` deadline, HH:MM on its date. Such a date gets no SLA schedules, and now no verdict either (a relative SLA still gives one). Its dry-run projection has no `deadline` or `marginSeconds` and the status `n/a`.
- **`RELATIVE_SLA_WARNING` and `RELATIVE_SLA_BREACH` were only checked for daily run dates of today and yesterday (UTC), so hourly run dates and runs started two or more days after their date got none.** The watchdog now checks every run date from 8 days back to 2 days ahead (UTC) that has a first sensor arrival. It sends nothing once a date's breach time is more than 24 hours past, so a breach it could not send within 24 hours is lost. A rerun started later than that gets no `RELATIVE_SLA_BREACH`; a rerun of today's or yesterday's daily date used to get one if none had been sent.
- **A long launch error filled the Slack `RETRY_EXHAUSTED` alert and was stored whole in the `infra-trigger-exhausted` job row.** The error is now cut to 1 KB in both, so the message's closing hint to submit a `RERUN_REQUEST` is not cut off.
- **Slack alerts let some line-breaking and text-direction characters through.** Only the C0 controls (newlines and tabs among them) and DEL became spaces. The C1 controls (U+0080 to U+009F), the line and paragraph separators (U+2028, U+2029) and the bidirectional controls (such as U+202E) now do too.
- **Slack alerts for some failures and warnings carried the info icon.** `RELATIVE_SLA_BREACH`, `IRREGULAR_SCHEDULE_MISSED`, `POST_RUN_FAILED`, `POST_RUN_DRIFT`, `BASELINE_CAPTURE_FAILED` and `INFRA_ALARM` now get the red circle, and `RELATIVE_SLA_WARNING` the yellow one.

## [0.11.0] - 2026-11-18

### Upgrading from 0.10.0

Pipeline configs ship with `terraform apply` (`deploy/terraform/config_loader.tf`), so edit them before or with the apply. A config that fails the new validation is rejected as `CONFIG_INVALID`, and that pipeline stops running until it is fixed.

**Edit pipeline files.**

1. Rename `schedule.trigger.deadline` to `schedule.trigger.cutoff`; the old key is ignored, leaving no cutoff. The value must be `HH:MM` with a two-digit hour or `:MM` (0.10.0 accepted `9:30`). A daily cutoff on a later day than the run date needs `cutoffDayOffset` (0 to 7).
2. Add `sla.deadlineDayOffset: 1` to a sensor-triggered pipeline that relied on 0.10.0 moving its daily deadline to the next day. The default is 0, the execution date.
3. Remove `schedule.time` from cron pipelines (it rejects the config with `schedule.cron`), and check that `schedule.cron` is five-field cron.
4. After the apply, late data, drift, the post-run checks and automatic reruns are acted on only for `postRun.watchWindow` (default `4h`) after a run completes, for every pipeline. Set a longer `watchWindow` on a pipeline that needs more.
5. With post-run `rules`, a config whose effective `postRun.sensorTimeout` (default `2h`) exceeds the effective `watchWindow` (default `4h`) is rejected, so a pipeline with a sensor timeout above `4h` must set `watchWindow`. `sensorTimeout` must be positive.
6. With `sla.maxDuration`, keep `sla.expectedDuration` positive and below it.
7. Use IANA zone names in `schedule.timezone` and `sla.timezone`. An unknown name or `Local` rejects the config; empty is UTC.
8. Move `job.type: command` jobs to `http` or `lambda`; `command`, or any unknown type, rejects the config.
9. Keep `pipeline.id` to 1 to 31 characters of `A-Za-z0-9_-` and name each pipeline file after it, or the config is rejected and `terraform plan` fails.

**Change jobs, sensors and event consumers.**

10. Make each `http` job write a `JOB#<scheduleId>#<date>#<unix millis>` row with `event` and the `runId` from `X-Interlock-Run-Id` to the joblog table (`dynamodb:PutItem`). A 2xx only means accepted; with no row the run times out after `job.jobPollWindowSeconds`.
11. Put the run date in the trigger sensor's `date` field for a pipeline with `schedule.cron` and `schedule.trigger`. Reconciliation now covers cron pipelines, and a passing sensor without `date` runs for today's UTC date, so it starts a new run each UTC day.
12. Update event consumers: `JOB_POLL_EXHAUSTED` is gone (a closed poll window is `JOB_FAILED`); exhaustion before the cutoff publishes `EVALUATION_RELEASED`, not `VALIDATION_EXHAUSTED`; `SENSOR_DEADLINE_EXPIRED` has `cutoff`/`cutoffAt` instead of `triggerDeadline`; `SCHEDULE_MISSED` has `cron`/`startBy` instead of `expectedTime`. Retries are named `<execution>-r<N>` and requested reruns `<pipeline>-<schedule>-<date>-rerun<attempt>-<unix>`.

**Build and apply.**

13. Build with Go 1.26 and `deploy/build.sh`, then `terraform apply`; no variable was added or renamed, and one apply updates every Lambda, the state machine and the pipeline configs together. The apply grants the watchdog `dynamodb:DeleteItem` and the orchestrator `states:StartExecution` (with `STATE_MACHINE_ARN`, now required at startup), filters the stream mappings, adds `CONFIG_INVALID` to the alert rule, and replaces the two iterator-age alarms with one that may page at once on an existing backlog. `slack_channel_id` is required once `slack_bot_token` or `slack_secret_arn` is set.
14. Apply while no runs are in flight, or expect this: an execution keeps the definition it started with, and a 0.10.0 execution's CompleteTrigger passes no `runId` or `executionName`, so a job that fails in a run straddling the apply is not retried. With the default `job.maxRetries: 0` the date ends `FAILED_FINAL` with `RETRY_EXHAUSTED`; with `maxRetries` above 0 the execution ends in `CompleteTriggerFailed`, leaving the trigger row `RUNNING` until `SFN_TIMEOUT`.
15. In a staged or manual deploy, update the orchestrator with or before the stream-router. A date completed by 0.10.0 has no `completedAt`, so late data and drift for it are ignored.
16. Tear down and redeploy an existing LocalStack stack (`make -f deploy/localstack/Makefile localstack-teardown localstack-deploy`); its deploy adds the stream filters only when it creates a mapping.

**After the apply.**

17. Write a `PK=PIPELINE#<id>`, `SK=RUN#<runKey>` row (`YYYY-MM-DD`, or `YYYY-MM-DDThh` for hourly `:MM` deadlines and cutoffs, optional `ttl`) to the control table for each run date you expect; your process writes them, not Interlock. The watchdog reads rows from 8 days before to 2 days after the current UTC date, so write each one before that date's first deadline, start-by or cutoff, and the first ones before the first run date you want covered. Without them there are no proactive SLA alerts, no `SCHEDULE_MISSED` and no watchdog cutoff check.

### Added

- **Expected run dates.** The watchdog reads `RUN#<runKey>` rows from 8 days before to 2 days after the current UTC date for the SLA schedules, the cron start-by check and the cutoff check. A malformed key is skipped with a warning, a key of the other cadence is ignored, and the stream-router ignores the rows. New `types.RunSK`.
- **Post-run watch window.** complete-trigger records `completedAt` (RFC3339 UTC) on the trigger row, replaced by a rerun's completion, and `postRun.watchWindow` (above 0, at most `720h`, default `4h`) sets how long after it the date is watched. New `types.DefaultPostRunWatchWindow`, `types.DefaultPostRunSensorTimeout` (`2h`), `ControlRecord.CompletedAt` and `ControlRecord.StartedAt`.
- **`EVALUATION_RELEASED`** (not on the alert rule) and the `evaluation-released` job event, for a date released before its cutoff. New `types.EventEvaluationReleased` and `types.JobEventEvaluationReleased`.
- **`CONFIG_INVALID` alert.** The watchdog publishes it for each rejected `CONFIG` row, with `reason` and `hash` in its detail, once per broken config version and day and at most 20 per pass; a failed publish is retried on the next pass.

### Changed (BREAKING)

- **`schedule.trigger.cutoff` (was `deadline`) is the latest time a run date may start.** A daily `HH:MM` cutoff falls on the run date plus `cutoffDayOffset` days (0 to 7, default 0) in `schedule.timezone`, else `sla.timezone`, else UTC; an hourly `:MM` cutoff falls in the hour after the run's hour. A time in a DST gap moves to the gap's end, a repeated time to its first occurrence. The cutoff must be `HH:MM` or `:MM`, and a non-zero offset is rejected with an hourly cutoff or without a cutoff.
- **Nothing starts for a date past its cutoff.** The stream-router and the watchdog's reconciliation start nothing, and close a date without a trigger row as `FAILED_FINAL` with one `SENSOR_DEADLINE_EXPIRED`; the watchdog closes each unstarted `RUN#` date the same way, and a run still evaluating at the cutoff is not launched. The alert carries `cutoff`, `cutoffAt` and its `source`. Excluded dates and dates whose job ran are left alone, and a `RERUN_REQUEST` still runs after the cutoff.
- **Evaluation that runs out before the cutoff releases the date.** The trigger row is deleted and `EVALUATION_RELEASED` published, so the next trigger sensor write or watchdog pass starts evaluation again, and the date's SLA alerts still fire. Without a cutoff for the date, or for a rerun, exhaustion publishes `VALIDATION_EXHAUSTED` and sets `FAILED_FINAL`.
- **`schedule.cron` is a start-by time per `RUN#` date.** Once the cron's first fire in the date's local day (or hour) passes with no job launched and the run not finished, the watchdog publishes `SCHEDULE_MISSED` once, with `cron`, `startBy` and `actionHint`. A run counts as started when its job launches (`startedAt` on the trigger row), not when evaluation begins.
- **Proactive SLA alerts follow `RUN#` rows.** The watchdog schedules the warning and breach only for `RUN#` dates whose breach is ahead and whose run has not finished, instead of guessing the date from the clock.
- **`sla.deadlineDayOffset` sets the deadline's day.** A daily deadline falls on the execution date plus `deadlineDayOffset` days (0 to 7, default 0) for every pipeline, in the SLA schedules, the end-of-run verdict and the dry-run projection. A non-zero offset is rejected with an hourly `:MM` deadline or without `sla.deadline`.
- **Late data, drift and the post-run checks act only inside the watch window.** After `completedAt` plus `postRun.watchWindow`, or for a `COMPLETED` row without `completedAt`, a trigger or post-run sensor is logged as ignored and an automatic (`late-data`, `data-drift`) rerun request is skipped. Manual reruns, inflight drift and dry-run pipelines are not bounded. `POST_RUN_SENSOR_MISSING` counts `sensorTimeout` from `completedAt`.
- **Sensor-trigger reconciliation covers cron pipelines.** 0.10.0 skipped pipelines with `schedule.cron`; now missed stream records are healed (`TRIGGER_RECOVERED`) and released dates restart (`JOB_TRIGGERED`, `source: reconciliation`) for them too. A trigger sensor without a `date` field takes today's UTC date, so one that keeps passing starts each new day's run after UTC midnight; put the run date in `date`.
- **HTTP jobs report their own result.** The run stays `RUNNING` until the job writes a `JOB#` row with its `runId` or the job poll window closes. Each request carries `X-Interlock-Pipeline-Id`, `X-Interlock-Schedule-Id`, `X-Interlock-Date` and `X-Interlock-Run-Id`; the matching `${INTERLOCK_*}` variables expand in body and headers, and the default body gains those four fields.
- **Time zones must be IANA names.** An unknown `schedule.timezone` or `sla.timezone`, or `Local`, rejects the config; 0.10.0 fell back to UTC silently.
- **`schedule.cron` is validated.** It must be standard five-field cron (day of week 0 to 6); `L`, `W`, `#`, a sixth field, descriptors such as `@daily` and a `TZ=` prefix are rejected, and so is `schedule.time` alongside it. New `validation.ParseCron`.
- **Post-run and relative SLA durations are validated.** `postRun.sensorTimeout` must be positive and, with post-run `rules`, at most the effective `postRun.watchWindow`. With `sla.maxDuration`, `sla.expectedDuration` must be positive and below it.
- **Pipeline IDs must fit the names built from them.** `pipeline.id` must be 1 to 31 characters of `A-Za-z0-9_-` (`validation.MaxPipelineIDLen`) and match its `PIPELINE#<id>` key, and Terraform fails `plan` when a file's name differs from its `pipeline.id`.

### Changed

- **The run's CompleteTrigger is the only thing that retries a failed job.** It reads the run's own row (`PERMANENT` uses `maxCodeRetries`), counts attempts from the execution name, and starts `<execution>-r<N>` with the SLA schedules kept, or ends with `FAILED_FINAL` and `RETRY_EXHAUSTED` (`PIPELINE_EXCLUDED` for an excluded date, `INFRA_FAILURE` when the retry cannot start). A requested rerun or reconciler start gets a fresh `maxRetries`. The stream-router starts nothing for `fail` or `timeout` rows.
- **Dry-run failure answers mirror CompleteTrigger.** `DRY_RUN_WOULD_RETRY` and `DRY_RUN_RETRY_EXHAUSTED` check the excluded date first, use the failed row's category, and report `retries: 0`.
- **A closed job poll window writes a `timeout` row** and retries like any timeout. The stream-router publishes `JOB_FAILED` once per new `fail` or `timeout` row (`error` cut to 2 KB, plus `runId` and `category`) and `JOB_COMPLETED` per new `success` row, for a date with a trigger row.
- **Rerun names carry the attempt:** `<pipeline>-<schedule>-<date>-rerun<attempt>-<unix>`, so two reruns in the same second no longer collide. Execution and SLA schedule names are no longer truncated or hashed.

### Removed

- **The `command` trigger type** (`types.TriggerCommand`, `types.CommandTriggerConfig`), which ran a binary named in the config with the orchestrator's role.
- **`JOB_POLL_EXHAUSTED` and the `job-poll-exhausted` job event** (`types.EventJobPollExhausted`, `types.JobEventJobPollExhausted`), also from the alert rule.
- **`TriggerCondition.Deadline`**, replaced by `Cutoff`.

### Fixed

- **Time zones load on any Lambda runtime image.** `pkg/validation`, which every Lambda links, imports `time/tzdata`.
- **One bad pipeline config no longer stops every pipeline.** The config scan skips a row that does not parse or validate, logs it with its PK, reason and hash, and keeps the rest; every Lambda sees the same accepted set.
- **`RELATIVE_SLA_WARNING` is published** once at first sensor arrival + `maxDuration` - `expectedDuration` (75% of `maxDuration` without `expectedDuration`). It and `RELATIVE_SLA_BREACH` are marked sent only after a successful publish, so a failed one is retried.
- **SLA alerts continue while a failed job is retried**, and the watchdog no longer recreates a warning schedule whose time has passed.
- **An exhausted evaluation ends cleanly.** It no longer leaves the trigger row `RUNNING` (and a later `SFN_TIMEOUT`) or runs the SLA cancel step; unfired SLA schedules still fire, and sla-monitor suppresses them for a `FAILED_FINAL` date.
- **A `lambda` job whose function returns an error is a job failure:** a `fail` row with category `PERMANENT`, `JOB_FAILED`, and retries under `maxCodeRetries` (default 1). Only a failed Invoke uses the Trigger state's `trigger_max_attempts` retries.
- **A rerun reads its own job result**, the newest `success`, `fail` or `timeout` row with its `runId`, not the previous attempt's. A `lambda` job's `success` row carries the execution name as `runId` instead of `sync`.
- **Slack alerts keep their message line**, escape mrkdwn, turn control characters into spaces, and stay within 2900 characters.
- **SLA schedule names fit EventBridge Scheduler limits**, so a long pipeline ID no longer loses its SLA alerts. A malformed `RERUN_REQUEST#` row is skipped with a warning instead of retried.
- **`deploy/build.sh` writes the `deploy/dist/<handler>.zip` files Terraform reads.**
- **The watchdog can release trigger locks** (`dynamodb:DeleteItem` on the control table).
- **alert-dispatcher runs in logging-only mode without a Slack token** instead of crash-looping.
- **The stream iterator-age alarm receives data:** one `<environment>-interlock-stream-router-iterator-age` alarm replaces two that had a dimension with no data.
- **The stream-router reports failed records by `SequenceNumber`**, so Lambda no longer retries the whole batch.
- **The stream-router ignores REMOVE records** except a deleted `CONFIG` row, and the event source mappings filter them out. Deleting a `RERUN_REQUEST#` or `SENSOR#` row no longer starts a rerun or a post-run verdict. The LocalStack deploy adds its filters only when it creates a mapping.

### Known limits

- The relative SLA checks only daily run dates for today and yesterday (UTC).
- `POST_RUN_SENSOR_MISSING` checks only today's UTC daily trigger row.
- A cutoff applies only to a run key of its cadence (`YYYY-MM-DD` for `HH:MM`, `YYYY-MM-DDThh` for `:MM`).
- A cutoff alert for a run still evaluating can come up to one `evaluation.interval` late.
- A `RERUN_REQUEST` for a released date before its cutoff is refused with `INFRA_FAILURE` ("lock reset failed"); write the sensor again.
- A released date without a `RUN#` row whose sensor stops passing gets no final alert until a passing sensor closes it after the cutoff.
- When `VALIDATION_EXHAUSTED` or `SENSOR_DEADLINE_EXPIRED` cannot be published through the step's retries, the row stays `RUNNING` until the watchdog's `SFN_TIMEOUT`.
- A retry after a `VALIDATION_EXHAUSTED` publish that succeeded but whose status write failed sends the alert twice.
- Partial batch reporting still redelivers the failed record and every later one in the batch.

### Dependencies

- Go 1.26 (`go.mod`)
- `github.com/robfig/cron/v3` v3.0.1 (new, `pkg/validation`)
- `go.opentelemetry.io/otel` v1.47.0, `otlptracegrpc` v1.45.0
- `google.golang.org/grpc` v1.83.2 (indirect)

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

[0.11.0]: https://github.com/dwsmith1983/interlock/releases/tag/v0.11.0
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
