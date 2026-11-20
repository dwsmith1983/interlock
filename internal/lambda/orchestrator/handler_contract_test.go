package orchestrator_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/internal/trigger"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// configItem builds the control-table row that store.GetConfig expects.
// Every test in this file uses pipeline "p".
func configItem(t *testing.T, cfg types.PipelineConfig) map[string]ddbtypes.AttributeValue {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	}
}

// strAttr returns item[name] as a string, "" when the attribute is absent. An
// attribute of another type fails the test.
func strAttr(t *testing.T, item map[string]ddbtypes.AttributeValue, name string) string {
	t.Helper()
	av, ok := item[name]
	if !ok {
		return ""
	}
	sv, ok := av.(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Errorf("attribute %q = %T, want a string", name, av)
		return ""
	}
	return sv.Value
}

// joblogPuts returns a PutItemFn that records every job log item written.
func joblogPuts(rows *[]map[string]ddbtypes.AttributeValue) func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if *in.TableName == "joblog" {
			*rows = append(*rows, in.Item)
		}
		return &dynamodb.PutItemOutput{}, nil
	}
}

func testDeps(fake *storetest.FakeDynamo) *lambda.Deps {
	return &lambda.Deps{
		Store:  storetest.NewStore(fake),
		Logger: slog.Default(),
	}
}

// fakeExecutor is a lambda.TriggerExecutor double that keeps the config it ran.
type fakeExecutor struct {
	meta map[string]interface{}
	err  error
	got  *types.TriggerConfig
	hook func() // when set, Execute calls it first
}

func (f *fakeExecutor) Execute(_ context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error) {
	if f.hook != nil {
		f.hook()
	}
	f.got = cfg
	return f.meta, f.err
}

// TestEvaluate_AlwaysEmitsStatus guards the IsReady Choice state, which reads
// $.evaluateResult.status with no IsPresent guard. An absent status raises
// States.Runtime, which Catch: States.ALL cannot intercept.
func TestEvaluate_AlwaysEmitsStatus(t *testing.T) {
	goodCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Validation: types.ValidationConfig{
			Trigger: "ALL",
			Rules:   []types.ValidationRule{{Key: "upstream", Check: types.CheckExists}},
		},
	}

	tests := []struct {
		name       string
		fake       *storetest.FakeDynamo
		wantStatus string
		wantErrSub string
	}{
		{
			name: "GetConfig failure",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("dynamodb: internal error")
				},
			},
			wantStatus: "error",
			wantErrSub: "dynamodb: internal error",
		},
		{
			name:       "config not found",
			fake:       &storetest.FakeDynamo{},
			wantStatus: "error",
			wantErrSub: "config not found",
		},
		{
			name: "GetAllSensors failure",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, goodCfg)}, nil
				},
				QueryFn: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					return nil, errors.New("dynamodb: request limit exceeded")
				},
			},
			wantStatus: "error",
			wantErrSub: "request limit exceeded",
		},
		{
			name: "rules not satisfied",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, goodCfg)}, nil
				},
			},
			wantStatus: "not_ready",
		},
		{
			// The clock is real, so 2026-03-01 14:00 has passed.
			name: "cutoff passed",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, cutoffCfg())}, nil
				},
			},
			wantStatus: "cutoff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := orchestrator.HandleOrchestrator(context.Background(), testDeps(tt.fake), lambda.OrchestratorInput{
				Mode: "evaluate", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01",
			})
			if err != nil {
				t.Fatalf("evaluate must not return a Lambda error, got %v", err)
			}
			if out.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", out.Status, tt.wantStatus)
			}
			if tt.wantErrSub != "" && !strings.Contains(out.Error, tt.wantErrSub) {
				t.Errorf("error = %q, want it to contain %q", out.Error, tt.wantErrSub)
			}

			data, mErr := json.Marshal(out)
			if mErr != nil {
				t.Fatalf("marshal: %v", mErr)
			}
			var decoded map[string]interface{}
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if _, ok := decoded["status"]; !ok {
				t.Errorf("marshaled output = %s, want a status key for $.evaluateResult.status", data)
			}
		})
	}
}

// TestTrigger_FailuresReturnLambdaError guards HasTriggerResult/CheckJob.
// A nil error with a partial triggerResult makes IsPresent true and CheckJob
// dereference a missing runId; returning a Go error routes to
// TriggerRetryExhausted instead, which releases the trigger lock.
func TestTrigger_FailuresReturnLambdaError(t *testing.T) {
	badTypeCfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job: types.JobConfig{
			Type:   types.TriggerType("bogus"),
			Config: map[string]interface{}{"jobName": "x"},
		},
	}

	tests := []struct {
		name       string
		fake       *storetest.FakeDynamo
		wantErrSub string
	}{
		{
			name: "GetConfig failure",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("dynamodb: internal error")
				},
			},
			wantErrSub: "dynamodb: internal error",
		},
		{
			name:       "config not found",
			fake:       &storetest.FakeDynamo{},
			wantErrSub: "config not found",
		},
		{
			name: "unsupported trigger type",
			fake: &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, badTypeCfg)}, nil
				},
			},
			wantErrSub: "unsupported trigger type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := testDeps(tt.fake)
			d.TriggerRunner = &fakeExecutor{}
			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01",
			})
			if err == nil {
				t.Fatalf("trigger must return a Lambda error, got output %+v", out)
			}
			if !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Errorf("err = %v, want it to contain %q", err, tt.wantErrSub)
			}
			if out.Mode != "" || out.RunID != "" || out.Metadata != nil {
				t.Errorf("output = %+v, want the zero value so ResultPath is never written", out)
			}
		})
	}
}

// TestTrigger_SuccessAlwaysCarriesRunIDAndMetadata guards the CheckJob
// Parameters, which dereference both $.triggerResult.runId and
// $.triggerResult.metadata, and the run ID each trigger type reports under.
// An http job reports its own result, so the trigger writes no job log row
// for it; command and lambda still record success at once under the run ID.
func TestTrigger_SuccessAlwaysCarriesRunIDAndMetadata(t *testing.T) {
	const execName = "p-s-2026-03-01-1709280000"
	now := time.Date(2026, 3, 1, 6, 0, 0, 0, time.UTC)
	fallback := fmt.Sprintf("p-s-2026-03-01-%d", now.UnixMilli())
	job := func(tt types.TriggerType, cfg map[string]interface{}) types.JobConfig {
		return types.JobConfig{Type: tt, Config: cfg}
	}
	glue := job(types.TriggerGlue, map[string]interface{}{"jobName": "my-etl"})
	httpJob := job(types.TriggerHTTP, map[string]interface{}{"url": "https://example.com/trigger", "method": "POST"})

	tests := []struct {
		name        string
		job         types.JobConfig
		meta        map[string]interface{}
		execName    string
		date        string // default 2026-03-01
		wantHour    string // par_hour in the default http body; hourly dates only
		wantRunID   string
		wantSuccess bool // a success row under wantRunID
	}{
		{name: "polling trigger keeps its API run ID", job: glue, execName: execName,
			meta: map[string]interface{}{"glue_job_name": "my-etl", "glue_job_run_id": "jr_1"}, wantRunID: "jr_1"},
		{name: "polling trigger without an API run ID", job: glue, execName: execName,
			meta: map[string]interface{}{"glue_job_name": "my-etl"}, wantRunID: execName},
		{name: "http uses the execution name", job: httpJob, execName: execName, wantRunID: execName},
		{name: "http with empty metadata", job: httpJob, execName: execName, meta: map[string]interface{}{}, wantRunID: execName},
		{name: "http without an execution name", job: httpJob, wantRunID: fallback},
		{name: "http hourly date", job: httpJob, execName: execName, date: "2026-03-01T05", wantHour: "05", wantRunID: execName},
		{name: "lambda records success under the run ID", execName: execName, wantRunID: execName, wantSuccess: true,
			job: job(types.TriggerLambda, map[string]interface{}{"functionName": "f"})},
		{name: "airflow uses the execution name", execName: execName, wantRunID: execName,
			job: job(types.TriggerAirflow, map[string]interface{}{"url": "https://airflow.example.com", "dagId": "d"})},
		{name: "emr-serverless gets the execution name", execName: execName, wantRunID: execName,
			job: job(types.TriggerEMRServerless, map[string]interface{}{"applicationId": "app-1", "jobName": "etl",
				"executionRoleArn": "arn:aws:iam::123456789012:role/emr-job", "entryPoint": "s3://bucket/etl.py"})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "p"}, Job: tt.job}
			var jobRows []map[string]ddbtypes.AttributeValue
			fake := &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
				},
				PutItemFn: joblogPuts(&jobRows),
			}
			d := testDeps(fake)
			d.NowFunc = func() time.Time { return now }
			exec := &fakeExecutor{meta: tt.meta}
			d.TriggerRunner = exec

			date := cmp.Or(tt.date, "2026-03-01")
			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: date, ExecutionName: tt.execName,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out.RunID != tt.wantRunID {
				t.Errorf("runID = %q, want %q", out.RunID, tt.wantRunID)
			}
			if out.JobType != string(tt.job.Type) {
				t.Errorf("jobType = %q, want %q", out.JobType, tt.job.Type)
			}
			data, err := json.Marshal(out)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var decoded map[string]interface{}
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if _, ok := decoded["metadata"].(map[string]interface{}); !ok {
				t.Errorf("marshaled output = %s, want a metadata object for $.triggerResult.metadata", data)
			}

			if tt.wantSuccess {
				if len(jobRows) != 1 || strAttr(t, jobRows[0], "event") != types.JobEventSuccess ||
					strAttr(t, jobRows[0], "runId") != tt.wantRunID {
					t.Errorf("job log rows = %v, want one success row with runId %q", jobRows, tt.wantRunID)
				}
			} else if len(jobRows) != 0 {
				t.Errorf("job log rows = %v, want none", jobRows)
			}

			wantRun := types.RunIdentity{PipelineID: "p", ScheduleID: "s", Date: date, RunID: tt.wantRunID}
			switch tt.job.Type {
			case types.TriggerLambda:
				if exec.got.Lambda.Run != wantRun {
					t.Errorf("Lambda.Run = %+v, want %+v", exec.got.Lambda.Run, wantRun)
				}
			case types.TriggerAirflow:
				if exec.got.Airflow.Run != wantRun {
					t.Errorf("Airflow.Run = %+v, want %+v", exec.got.Airflow.Run, wantRun)
				}
			case types.TriggerEMRServerless:
				if exec.got.EMRServerless.Run != wantRun {
					t.Errorf("EMRServerless.Run = %+v, want %+v", exec.got.EMRServerless.Run, wantRun)
				}
			case types.TriggerGlue:
				if exec.got.Glue == nil || exec.got.Glue.JobName != "my-etl" {
					t.Errorf("Glue = %+v, want job name my-etl", exec.got.Glue)
				}
			}
			if tt.job.Type == types.TriggerHTTP {
				if exec.got.HTTP.Run != wantRun {
					t.Errorf("HTTP.Run = %+v, want %+v", exec.got.HTTP.Run, wantRun)
				}
				var body map[string]string
				wantBody := map[string]string{"par_day": "20260301", "pipelineId": "p", "scheduleId": "s", "date": date, "runId": tt.wantRunID}
				if tt.wantHour != "" {
					wantBody["par_hour"] = tt.wantHour
				}
				if !exec.got.HTTP.BodyGenerated {
					t.Error("default body not marked generated: it would go through variable expansion")
				}
				if err := json.Unmarshal([]byte(exec.got.HTTP.Body), &body); err != nil || !maps.Equal(body, wantBody) {
					t.Errorf("default body = %s (%v), want %v", exec.got.HTTP.Body, err, wantBody)
				}
			}
		})
	}
}

// TestTrigger_InfraFailureRowCarriesRunID: the run ID is known before the
// launch, so the infra-trigger-failure row belongs to the run like any other.
func TestTrigger_InfraFailureRowCarriesRunID(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: map[string]interface{}{"url": "https://example.com/run"}},
	}
	var jobRows []map[string]ddbtypes.AttributeValue
	fake := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
		},
		PutItemFn: joblogPuts(&jobRows),
	}
	d := testDeps(fake)
	d.TriggerRunner = &fakeExecutor{err: errors.New("connection refused")}

	_, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", ExecutionName: "exec-1",
	})
	if err == nil {
		t.Fatal("trigger must return a Lambda error when Execute fails")
	}
	if len(jobRows) != 1 || strAttr(t, jobRows[0], "event") != types.JobEventInfraTriggerFailure ||
		strAttr(t, jobRows[0], "runId") != "exec-1" {
		t.Errorf("job log rows = %v, want one infra-trigger-failure row with runId exec-1", jobRows)
	}
}

// TestTrigger_InfraFailureRowWriteFails: a failed audit row write does not
// hide the launch failure, which is still the Lambda error.
func TestTrigger_InfraFailureRowWriteFails(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "gold-etl"}},
	}
	fake := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
		},
		PutItemFn: func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb: provisioned throughput exceeded")
		},
	}
	d := testDeps(fake)
	d.TriggerRunner = &fakeExecutor{err: errors.New("glue trigger: ConcurrentRunsExceededException")}

	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", ExecutionName: "exec-1",
	})
	if err == nil || err.Error() != "trigger execute: glue trigger: ConcurrentRunsExceededException" {
		t.Errorf("err = %v, want the launch error", err)
	}
	if out.Mode != "" || out.RunID != "" || out.JobType != "" || out.Metadata != nil {
		t.Errorf("output = %+v, want the zero value", out)
	}
}

// TestTrigger_LambdaResult: a function that ran and returned an error is the
// job's result, a PERMANENT fail row that CompleteTrigger decides on. Only a
// failed Invoke goes to the Trigger state's retry.
func TestTrigger_LambdaResult(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerLambda, Config: map[string]interface{}{"functionName": "f"}},
	}
	jobErr := &trigger.JobFailedError{Message: "lambda trigger: function error: Unhandled: boom"}
	timeoutErr := &trigger.JobFailedError{Message: "lambda trigger: function did not finish within 105s", Category: types.FailureOutcomeUnknown}

	tests := []struct {
		name         string
		execErr      error
		wantErr      bool
		wantEvent    string
		wantMsg      string
		wantCategory string
	}{
		{name: "function error is a permanent job failure", execErr: fmt.Errorf("wrapped: %w", jobErr),
			wantEvent: types.JobEventFail, wantMsg: "wrapped: " + jobErr.Message, wantCategory: string(types.FailurePermanent)},
		{name: "lambda timeout is one failure", execErr: timeoutErr,
			wantEvent: types.JobEventFail, wantMsg: timeoutErr.Message, wantCategory: string(types.FailureOutcomeUnknown)},
		{name: "invoke error is an infra failure", execErr: errors.New("lambda trigger: Invoke failed: throttled"),
			wantErr: true, wantEvent: types.JobEventInfraTriggerFailure, wantMsg: "trigger execute: lambda trigger: Invoke failed: throttled"},
		{name: "normal return is success", wantEvent: types.JobEventSuccess, wantMsg: "lambda trigger completed synchronously"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var jobRows []map[string]ddbtypes.AttributeValue
			fake := &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
				},
				PutItemFn: joblogPuts(&jobRows),
			}
			d := testDeps(fake)
			d.TriggerRunner = &fakeExecutor{err: tt.execErr}
			rec := &recorder{}
			d.EventBridge, d.EventBusName = rec, "bus"

			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", ExecutionName: "exec-1",
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if out.Mode != "trigger" || out.RunID != "exec-1" || out.Metadata == nil {
					t.Errorf("output = %+v, want the trigger output for run exec-1", out)
				}
				if len(rec.events) != 1 || rec.events[0].Type != string(types.EventJobTriggered) {
					t.Errorf("events = %v, want one JOB_TRIGGERED", rec.events)
				}
			}
			if len(jobRows) != 1 {
				t.Fatalf("job log rows = %v, want exactly one", jobRows)
			}
			row := jobRows[0]
			if got := strAttr(t, row, "event"); got != tt.wantEvent {
				t.Errorf("event = %q, want %q", got, tt.wantEvent)
			}
			if got := strAttr(t, row, "runId"); got != "exec-1" {
				t.Errorf("runId = %q, want exec-1", got)
			}
			if got := strAttr(t, row, "error"); got != tt.wantMsg {
				t.Errorf("error = %q, want %q", got, tt.wantMsg)
			}
			if got := strAttr(t, row, "category"); got != tt.wantCategory {
				t.Errorf("category = %q, want %q", got, tt.wantCategory)
			}
		})
	}
}

// TestTrigger_LambdaFailRowWriteError: when the fail row cannot be written,
// the log is the only record of the job's failure, so it is an error that
// names the run and carries the function's error.
func TestTrigger_LambdaFailRowWriteError(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerLambda, Config: map[string]interface{}{"functionName": "f"}},
	}
	fake := &storetest.FakeDynamo{
		GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
		},
		PutItemFn: func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb: throttled")
		},
	}
	var logs bytes.Buffer
	d := testDeps(fake)
	d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	d.TriggerRunner = &fakeExecutor{err: &trigger.JobFailedError{Message: "function error: boom"}}

	if _, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", ExecutionName: "exec-1",
	}); err != nil {
		t.Fatalf("a failed row write must not fail the trigger, got %v", err)
	}
	for _, want := range []string{"level=ERROR", "runId=exec-1", `"function error: boom"`, "dynamodb: throttled"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log = %q, want it to contain %q", logs.String(), want)
		}
	}
}
