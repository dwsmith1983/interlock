package orchestrator_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func jobLogItem(millis, event, runID, errMsg string) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("s", "2026-03-01", millis)},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
		"runId": &ddbtypes.AttributeValueMemberS{Value: runID},
	}
	if errMsg != "" {
		item["error"] = &ddbtypes.AttributeValueMemberS{Value: errMsg}
	}
	return item
}

// TestCheckJob_ReadsOwnRunOnly guards reruns: the previous attempt's terminal
// row must not decide the new attempt's outcome, in either direction.
func TestCheckJob_ReadsOwnRunOnly(t *testing.T) {
	tests := []struct {
		name      string
		rows      []map[string]ddbtypes.AttributeValue // newest first
		wantEvent string
		wantError string
	}{
		{
			name:      "an older fail of run A does not fail run B",
			rows:      []map[string]ddbtypes.AttributeValue{jobLogItem("100", types.JobEventFail, "run-a", "boom")},
			wantEvent: "",
		},
		{
			name: "a newer fail of run A does not hide run B's success",
			rows: []map[string]ddbtypes.AttributeValue{
				jobLogItem("300", types.JobEventFail, "run-a", "boom"),
				jobLogItem("200", types.JobEventSuccess, "run-b", ""),
			},
			wantEvent: types.JobEventSuccess,
		},
		{
			name:      "a success row's message is not reported as an error",
			rows:      []map[string]ddbtypes.AttributeValue{jobLogItem("200", types.JobEventSuccess, "run-b", "http trigger completed synchronously")},
			wantEvent: types.JobEventSuccess,
		},
		{
			name: "run B's own fail is reported with its error",
			rows: []map[string]ddbtypes.AttributeValue{
				jobLogItem("300", types.JobEventInfraTriggerFailure, "run-b", "retrying"),
				jobLogItem("200", types.JobEventFail, "run-b", "disk full"),
				jobLogItem("100", types.JobEventSuccess, "run-a", ""),
			},
			wantEvent: types.JobEventFail,
			wantError: "disk full",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &storetest.FakeDynamo{
				QueryFn: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					return &dynamodb.QueryOutput{Items: tt.rows}, nil
				},
			}
			out, err := orchestrator.HandleOrchestrator(context.Background(), testDeps(fake), lambda.OrchestratorInput{
				Mode: "check-job", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", RunID: "run-b",
			})
			if err != nil {
				t.Fatalf("check-job: %v", err)
			}
			if out.Event != tt.wantEvent || out.Error != tt.wantError {
				t.Errorf("check-job = event %q error %q, want event %q error %q", out.Event, out.Error, tt.wantEvent, tt.wantError)
			}
		})
	}
}

// TestCheckJob_EmptyRunIDIsAnError pins the output for a lookup error and
// checks it is logged with the run's identity.
func TestCheckJob_EmptyRunIDIsAnError(t *testing.T) {
	var logs bytes.Buffer
	d := testDeps(&storetest.FakeDynamo{})
	d.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "check-job", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01",
	})
	if err != nil {
		t.Fatalf("check-job must not return a Lambda error, got %v", err)
	}
	if out.Event != "" || !strings.Contains(out.Error, "runID is required") {
		t.Errorf("check-job = event %q error %q, want no event and the lookup error", out.Event, out.Error)
	}
	for _, want := range []string{"level=WARN", "pipeline=p", "schedule=s", "date=2026-03-01", `runId=""`, "runID is required"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log = %q, want it to contain %q", logs.String(), want)
		}
	}
}

// fakeStatusChecker is a lambda.StatusChecker double.
type fakeStatusChecker struct {
	result lambda.StatusResult
	err    error
}

func (f *fakeStatusChecker) CheckStatus(context.Context, types.TriggerType, map[string]interface{}, map[string]string) (lambda.StatusResult, error) {
	return f.result, f.err
}

// TestCheckJob_PolledRowsCarryRunID: when CheckJob polls the job API and
// writes the result itself, the row belongs to input.RunID, so the next
// check (and a later rerun) reads it as this run's result.
func TestCheckJob_PolledRowsCarryRunID(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}},
	}
	tests := []struct {
		name         string
		result       lambda.StatusResult
		wantEvent    string
		wantError    string
		wantCategory string
	}{
		{name: "succeeded", result: lambda.StatusResult{State: "succeeded"}, wantEvent: types.JobEventSuccess},
		{
			name:      "failed",
			result:    lambda.StatusResult{State: "failed", Message: "OOM", FailureCategory: types.FailurePermanent},
			wantEvent: types.JobEventFail, wantError: "OOM", wantCategory: string(types.FailurePermanent),
		},
		{
			name:      "failed without a category",
			result:    lambda.StatusResult{State: "failed", Message: "OOM killed"},
			wantEvent: types.JobEventFail, wantError: "OOM killed",
		},
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
			d.StatusChecker = &fakeStatusChecker{result: tt.result}

			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "check-job", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", RunID: "jr_1",
				Metadata: map[string]interface{}{"glue_job_name": "etl", "glue_job_run_id": "jr_1"},
			})
			if err != nil {
				t.Fatalf("check-job: %v", err)
			}
			if out.Event != tt.wantEvent {
				t.Errorf("event = %q, want %q", out.Event, tt.wantEvent)
			}
			if len(jobRows) != 1 {
				t.Fatalf("job log rows = %v, want one", jobRows)
			}
			r := jobRows[0]
			if got := [4]string{strAttr(t, r, "runId"), strAttr(t, r, "event"), strAttr(t, r, "error"), strAttr(t, r, "category")}; got != [4]string{"jr_1", tt.wantEvent, tt.wantError, tt.wantCategory} {
				t.Errorf("row runId/event/error/category = %q, want %q", got, [4]string{"jr_1", tt.wantEvent, tt.wantError, tt.wantCategory})
			}
			if _, has := r["category"]; has != (tt.wantCategory != "") {
				t.Errorf("row %v: has category = %v, want %v", r, has, tt.wantCategory != "")
			}
		})
	}
}

// TestCheckJob_PollWithoutResult: a poll that gives no terminal state, or
// cannot be made, writes no row and reports no event, so the run waits.
func TestCheckJob_PollWithoutResult(t *testing.T) {
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}},
	}
	tests := []struct {
		name      string
		result    lambda.StatusResult
		checkErr  error
		configErr error
		noConfig  bool
		wantError string
	}{
		{name: "running", result: lambda.StatusResult{State: "running", Message: "RUNNING"}},
		// The error wins over any result that came with it.
		{name: "status error", result: lambda.StatusResult{State: "succeeded"}, checkErr: errors.New("glue: access denied")},
		{name: "config read error", configErr: errors.New("dynamodb: internal error"), wantError: "dynamodb: internal error"},
		{name: "config missing", noConfig: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var jobRows []map[string]ddbtypes.AttributeValue
			fake := &storetest.FakeDynamo{
				GetItemFn: func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					if tt.configErr != nil {
						return nil, tt.configErr
					}
					if tt.noConfig {
						return &dynamodb.GetItemOutput{}, nil
					}
					return &dynamodb.GetItemOutput{Item: configItem(t, cfg)}, nil
				},
				PutItemFn: joblogPuts(&jobRows),
			}
			d := testDeps(fake)
			d.StatusChecker = &fakeStatusChecker{result: tt.result, err: tt.checkErr}

			out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
				Mode: "check-job", PipelineID: "p", ScheduleID: "s", Date: "2026-03-01", RunID: "jr-999",
				Metadata: map[string]interface{}{"glue_job_name": "cdr-agg-hour", "glue_job_run_id": "jr-999"},
			})
			if err != nil {
				t.Fatalf("check-job must not return a Lambda error, got %v", err)
			}
			if out.Mode != "check-job" || out.Event != "" {
				t.Errorf("output = %+v, want mode check-job and no event", out)
			}
			if tt.wantError == "" && out.Error != "" || !strings.Contains(out.Error, tt.wantError) {
				t.Errorf("error = %q, want %q", out.Error, tt.wantError)
			}
			if len(jobRows) != 0 {
				t.Errorf("job log rows = %v, want none", jobRows)
			}
		})
	}
}

// TestCheckJob_PolledFailureDoesNotPublish: the stream-router publishes
// JOB_FAILED from the fail row CheckJob writes, so CheckJob itself must not.
func TestCheckJob_PolledFailureDoesNotPublish(t *testing.T) {
	d, _, rec := memDeps(t, types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerGlue, Config: map[string]interface{}{"jobName": "etl"}},
	})
	d.StatusChecker = &fakeStatusChecker{result: lambda.StatusResult{State: "failed", Message: "OOM"}}

	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "check-job", PipelineID: "p", ScheduleID: "s", Date: ctDate, RunID: "jr_1",
		Metadata: map[string]interface{}{"glue_job_name": "etl", "glue_job_run_id": "jr_1"},
	})
	if err != nil || out.Event != types.JobEventFail {
		t.Fatalf("check-job = %+v, %v, want event fail", out, err)
	}
	if len(rec.events) != 0 {
		t.Errorf("events = %+v, want none", rec.events)
	}
}
