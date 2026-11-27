package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// manualRerunHint is the action a manual-rerun event asks for.
const manualRerunHint = "Rerun by writing a manual RERUN_REQUEST row (see rerunRequest) once the new data is checked."

// maxManualRerunSensorData is the most sensor data, as JSON bytes, a
// manual-rerun event carries.
const maxManualRerunSensorData = 32 * 1024

// manualRerunInput is what the place that stopped an automatic rerun knows
// about it: the request reason (late-data or data-drift), where it came from
// (sensor), the bound that stopped it (the endedBy label of
// logAutoRerunEnded), the late sensor write, and now.
type manualRerunInput struct {
	reason, source, blockedBy string
	sensorKey                 string
	sensorData                map[string]interface{}
	arrivedAt                 time.Time
	now                       time.Time
}

// recordManualRerunRequired publishes MANUAL_RERUN_REQUIRED the first time an
// automatic rerun of the date's completion tr is stopped for in.reason, and
// MANUAL_RERUN_STILL_REQUIRED each later time. A date whose latest job outcome
// is not a success, or tr without a completion time, gets neither. The marker
// manual-rerun-alert-<reason>#<date> keeps the completion alerted for and is
// written only after a REQUIRED publish succeeds. Failures are logged.
func recordManualRerunRequired(ctx context.Context, d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string,
	tr *types.ControlRecord, job *types.JobLogRecord, in manualRerunInput) {
	if lambda.AutoRerunEnd(cfg, tr).IsZero() || job == nil || job.Event != types.JobEventSuccess {
		return
	}
	markerKey := "manual-rerun-alert-" + in.reason + "#" + date
	marker, err := d.Store.GetSensorData(ctx, pipelineID, markerKey)
	if err != nil {
		d.Logger.WarnContext(ctx, "failed to read manual rerun alert marker, alerting as if absent",
			"pipelineId", pipelineID, "date", date, "error", err)
	}
	eventType, state := types.EventManualRerunRequired, "required"
	if marker != nil && marker["completedAt"] == tr.CompletedAt {
		eventType, state = types.EventManualRerunStillRequired, "still required"
	}
	msg := fmt.Sprintf("manual rerun %s for %s: %s after automatic reruns ended (%s)", state, pipelineID, in.reason, in.blockedBy)
	if err := lambda.PublishEvent(ctx, d, string(eventType), pipelineID, schedule, date, msg,
		manualRerunDetail(d, cfg, pipelineID, schedule, date, tr, job, in)); err != nil {
		d.Logger.WarnContext(ctx, "failed to publish event", "type", eventType, "error", err)
		return
	}
	if eventType != types.EventManualRerunRequired {
		return
	}
	if err := d.Store.WriteSensor(ctx, pipelineID, markerKey, map[string]interface{}{
		"completedAt": tr.CompletedAt, "alertedAt": in.now.UTC().Format(time.RFC3339)}); err != nil {
		d.Logger.WarnContext(ctx, "failed to write manual rerun alert marker", "pipelineId", pipelineID, "date", date, "error", err)
	}
}

// manualRerunDetail is the detail of a manual-rerun event: why and how the
// automatic rerun stopped, the job and its last run, the late data, and the
// request row a person writes to rerun. A value that is unknown is left out,
// and so is sensor data over maxManualRerunSensorData.
func manualRerunDetail(d *lambda.Deps, cfg *types.PipelineConfig, pipelineID, schedule, date string,
	tr *types.ControlRecord, job *types.JobLogRecord, in manualRerunInput) map[string]interface{} {
	completed, _ := time.Parse(time.RFC3339, tr.CompletedAt) // parseable: AutoRerunEnd is not zero
	detail := map[string]interface{}{
		"reason": in.reason, "blockedBy": in.blockedBy, "source": in.source, "jobType": string(cfg.Job.Type),
		"completedAt": tr.CompletedAt, "finishedAgo": in.now.Sub(completed).String(),
		"maxAutoRerunAge": lambda.MaxAutoRerunAge(cfg).String(),
		"watchWindow":     lambda.PostRunWatchEnd(cfg, tr).Sub(completed).String(),
		"rerunRequest": map[string]interface{}{"table": d.Store.ControlTable, "PK": types.PipelinePK(pipelineID),
			"SK": types.RerunRequestSK(schedule, date), "reason": "manual"},
		"actionHint": manualRerunHint,
	}
	if ids := jobIdentifiers(cfg); len(ids) > 0 {
		detail["job"] = ids
	}
	for k, v := range map[string]string{"lastRunId": job.RunID, "startedAt": tr.StartedAt, "sensorKey": in.sensorKey} {
		if v != "" {
			detail[k] = v
		}
	}
	if !in.arrivedAt.IsZero() {
		detail["arrivedAt"] = in.arrivedAt.UTC().Format(time.RFC3339)
	}
	if in.sensorData != nil {
		if data, err := json.Marshal(in.sensorData); err != nil || len(data) > maxManualRerunSensorData {
			detail["sensorDataOmitted"] = true
		} else {
			detail["sensorData"] = in.sensorData
		}
	}
	return detail
}

// jobIdentifiers names the job a pipeline launches with an allowlist of its
// job.config fields: never headers, bodies, payloads, arguments or tokens,
// and URLs without user info, query or fragment. It is nil for a config that
// does not decode.
func jobIdentifiers(cfg *types.PipelineConfig) map[string]string {
	unmarshal, ok := lambda.TriggerUnmarshalers[cfg.Job.Type]
	if !ok {
		return nil
	}
	data, err := json.Marshal(cfg.Job.Config)
	tc := types.TriggerConfig{Type: cfg.Job.Type}
	if err != nil || unmarshal(data, &tc) != nil {
		return nil
	}
	ids := map[string]string{}
	add := func(kv ...string) {
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i+1] != "" {
				ids[kv[i]] = kv[i+1]
			}
		}
	}
	switch {
	case tc.HTTP != nil:
		add("url", trigger.StripURL(tc.HTTP.URL), "method", tc.HTTP.Method)
	case tc.Airflow != nil:
		add("url", trigger.StripURL(tc.Airflow.URL), "dagId", tc.Airflow.DagID)
	case tc.Glue != nil:
		add("jobName", tc.Glue.JobName)
	case tc.EMR != nil:
		add("clusterId", tc.EMR.ClusterID, "stepName", tc.EMR.StepName)
	case tc.EMRServerless != nil:
		add("applicationId", tc.EMRServerless.ApplicationID, "jobName", tc.EMRServerless.JobName)
	case tc.Databricks != nil:
		add("workspaceUrl", trigger.StripURL(tc.Databricks.WorkspaceURL), "jobId", tc.Databricks.JobID)
	case tc.StepFunction != nil:
		add("stateMachineArn", tc.StepFunction.StateMachineARN)
	case tc.Lambda != nil:
		add("functionName", tc.Lambda.FunctionName)
	}
	return ids
}
