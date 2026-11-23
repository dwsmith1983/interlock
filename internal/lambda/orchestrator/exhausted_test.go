package orchestrator_test

import (
	"cmp"
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestJobPollExhausted_TimeoutRowCarriesRunID: the closed poll window is a
// timeout of this run, which CompleteTrigger may retry; the stream-router
// publishes JOB_FAILED from the row, so this handler publishes nothing.
func TestJobPollExhausted_TimeoutRowCarriesRunID(t *testing.T) {
	d, mem, rec := memDeps(t, failCfg(1))
	out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "job-poll-exhausted", PipelineID: "p", ScheduleID: "s", Date: ctDate, RunID: "run-1",
	})
	if err != nil {
		t.Fatalf("job-poll-exhausted: %v", err)
	}
	if out.Mode != "job-poll-exhausted" || out.Status != "exhausted" {
		t.Errorf("output = %+v, want mode job-poll-exhausted and status exhausted", out)
	}
	rows := mem.Items("joblog")
	if len(rows) != 1 {
		t.Fatalf("job log rows = %v, want one", rows)
	}
	if got := [3]string{strAttr(t, rows[0], "event"), strAttr(t, rows[0], "runId"), strAttr(t, rows[0], "error")}; got != [3]string{types.JobEventTimeout, "run-1", "job poll window exhausted"} {
		t.Errorf("row event/runId/error = %q", got)
	}
	if len(rec.events) != 0 {
		t.Errorf("events = %+v, want none", rec.events)
	}
}

// TestJobPollExhausted_WriteErrorIsLambdaError: without the timeout row the
// stream-router cannot alert, so the failed write fails the state.
func TestJobPollExhausted_WriteErrorIsLambdaError(t *testing.T) {
	d, _, rec := memDeps(t, failCfg(1))
	failTablePut(d, "joblog", errors.New("dynamodb: throttled"))
	_, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "job-poll-exhausted", PipelineID: "p", ScheduleID: "s", Date: ctDate, RunID: "jr_xyz",
	})
	if err == nil || !strings.Contains(err.Error(), "write job poll timeout joblog") {
		t.Errorf("err = %v, want the joblog write error", err)
	}
	if len(rec.events) != 0 {
		t.Errorf("events = %+v, want none", rec.events)
	}
}

// TestTriggerExhausted: spent trigger retries leave an infra-trigger-exhausted
// joblog row carrying the last error, then RETRY_EXHAUSTED, then the trigger
// row is set FAILED_FINAL (no staleAt, no ttl) so nothing starts the date
// again on its own. A failed row read, joblog write or publish fails the
// state with the row still RUNNING, and a retry after a failed publish ends
// the date. A missing row is created FAILED_FINAL; a finished one (a Task
// retry after success) is left alone with nothing written.
func TestTriggerExhausted(t *testing.T) {
	cause := "ConcurrentRunsExceededException: Concurrent runs exceeded."
	final, running := types.TriggerStatusFailedFinal, types.TriggerStatusRunning
	retryExhausted := "event " + string(types.EventRetryExhausted)
	wantOps := []string{retryExhausted, "status " + final}
	exhaustedRow := [2]string{types.JobEventInfraTriggerExhausted, ""}
	throttled := errors.New("dynamodb: throttled")
	tests := []struct {
		name         string
		errorInfo    map[string]interface{}
		putErr       error
		publishErr   error
		updateErr    error
		getErr       error
		retry        bool // a first call whose publish fails runs before the checked one
		noRow        bool
		startStatus  string
		triggerFirst bool // a trigger call fails to launch first; its error is the Cause
		wantErrSub   string
		wantJoblog   [][2]string // event, runId
		wantError    string      // the exhausted row's error; "" for no attribute
		wantOps      []string
		wantRow      string // a RUNNING row keeps its staleAt; any other has neither staleAt nor ttl
	}{
		{name: "withCause", errorInfo: map[string]interface{}{"Error": "States.TaskFailed", "Cause": cause},
			wantJoblog: [][2]string{exhaustedRow}, wantError: cause, wantOps: wantOps, wantRow: final},
		{name: "noErrorInfo", wantJoblog: [][2]string{exhaustedRow}, wantOps: wantOps, wantRow: final},
		{name: "joblogWriteFails", errorInfo: map[string]interface{}{"Cause": cause},
			putErr: errors.New("dynamodb: write capacity exceeded"), wantErrSub: "write trigger-exhausted joblog",
			wantRow: running},
		{name: "afterTriggerInfraFailure", triggerFirst: true,
			wantJoblog: [][2]string{{types.JobEventInfraTriggerFailure, ctExec}, exhaustedRow},
			wantError:  "trigger execute: connection refused", wantOps: wantOps, wantRow: final},
		{name: "publishFails", errorInfo: map[string]interface{}{"Cause": cause}, publishErr: errors.New("eventbridge: throttled"),
			wantErrSub: "publish RETRY_EXHAUSTED", wantJoblog: [][2]string{exhaustedRow}, wantError: cause, wantRow: running},
		{name: "statusWriteFails", errorInfo: map[string]interface{}{"Cause": cause}, updateErr: throttled,
			wantErrSub: `trigger-exhausted: set trigger status "p"/s/` + ctDate + ": dynamodb: throttled",
			wantJoblog: [][2]string{exhaustedRow}, wantError: cause, wantOps: []string{retryExhausted}, wantRow: running},
		{name: "triggerReadFails", errorInfo: map[string]interface{}{"Cause": cause}, getErr: throttled,
			wantErrSub: `trigger-exhausted: get trigger "p"/s/` + ctDate + ": dynamodb: throttled", wantRow: running},
		{name: "publishFailsThenRetried", errorInfo: map[string]interface{}{"Cause": cause}, retry: true,
			wantJoblog: [][2]string{exhaustedRow, exhaustedRow}, wantError: cause, wantOps: wantOps, wantRow: final},
		{name: "rowAlreadyFinal", startStatus: final, wantRow: final},
		{name: "rowAlreadyCompleted", startStatus: types.TriggerStatusCompleted, wantRow: types.TriggerStatusCompleted},
		{name: "rowMissing", noRow: true, wantJoblog: [][2]string{exhaustedRow}, wantOps: wantOps, wantRow: final},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			d, mem, rec := memDeps(t, failCfg(1))
			now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = d.NowFunc // joblog rows written at different times get different keys
			if !tt.noRow {
				if ok, err := d.Store.AcquireTriggerLock(ctx, "p", "s", ctDate, time.Hour); err != nil || !ok {
					t.Fatalf("acquire trigger lock = %v, %v", ok, err)
				}
			}
			if tt.startStatus != "" {
				if err := d.Store.SetTriggerStatus(ctx, "p", "s", ctDate, tt.startStatus); err != nil {
					t.Fatalf("set trigger status: %v", err)
				}
			}
			rec.ops = nil
			if tt.putErr != nil {
				failTablePut(d, "joblog", tt.putErr)
			}
			fake := d.Store.Client.(*storetest.FakeDynamo)
			if tt.updateErr != nil {
				fake.UpdateItemFn = func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
					return nil, tt.updateErr
				}
			}
			if tt.getErr != nil {
				fake.GetItemFn = func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, tt.getErr
				}
			}
			errorInfo := tt.errorInfo
			if tt.triggerFirst {
				d.TriggerRunner = &fakeExecutor{err: errors.New("connection refused")}
				_, err := orchestrator.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
					Mode: "trigger", PipelineID: "p", ScheduleID: "s", Date: ctDate, ExecutionName: ctExec,
				})
				if err == nil || err.Error() != "trigger execute: connection refused" {
					t.Fatalf("trigger err = %v, want the launch error", err)
				}
				errorInfo = map[string]interface{}{"Error": "States.TaskFailed", "Cause": err.Error()}
				now = now.Add(time.Minute)
			}

			exhausted := lambda.OrchestratorInput{
				Mode: "trigger-exhausted", PipelineID: "p", ScheduleID: "s", Date: ctDate, ErrorInfo: errorInfo,
			}
			if tt.retry {
				rec.putErr = errors.New("eventbridge: throttled")
				if _, err := orchestrator.HandleOrchestrator(ctx, d, exhausted); err == nil || !strings.Contains(err.Error(), "publish RETRY_EXHAUSTED") {
					t.Fatalf("first call err = %v, want the publish error", err)
				}
				now = now.Add(time.Minute)
			}
			rec.putErr = tt.publishErr
			out, err := orchestrator.HandleOrchestrator(ctx, d, exhausted)
			if tt.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSub) {
					t.Errorf("err = %v, want it to contain %q", err, tt.wantErrSub)
				}
			} else if err != nil || out.Mode != "trigger-exhausted" || out.Status != "exhausted" {
				t.Errorf("trigger-exhausted = %+v, %v, want mode trigger-exhausted and status exhausted", out, err)
			}

			rows := mem.Items("joblog")
			var joblog [][2]string
			for _, row := range rows {
				joblog = append(joblog, [2]string{strAttr(t, row, "event"), strAttr(t, row, "runId")})
			}
			if !reflect.DeepEqual(joblog, tt.wantJoblog) {
				t.Errorf("joblog event/runId = %q, want %q", joblog, tt.wantJoblog)
			}
			if len(rows) > 0 {
				last := rows[len(rows)-1]
				if _, has := last["runId"]; has {
					t.Errorf("exhausted row %v, want no runId", last)
				}
				if _, has := last["error"]; has != (tt.wantError != "") || strAttr(t, last, "error") != tt.wantError {
					t.Errorf("exhausted row %v, want error %q", last, tt.wantError)
				}
			}
			if !slices.Equal(rec.ops, tt.wantOps) {
				t.Errorf("ops = %v, want %v", rec.ops, tt.wantOps)
			}
			row := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("s", ctDate))
			if got := strAttr(t, row, "status"); got != tt.wantRow {
				t.Errorf("trigger row status = %q, want %q", got, tt.wantRow)
			}
			_, hasStaleAt := row["staleAt"]
			_, hasTTL := row["ttl"]
			if hasStaleAt != (tt.wantRow == running) || hasTTL {
				t.Errorf("%s trigger row has staleAt = %v, ttl = %v", tt.wantRow, hasStaleAt, hasTTL)
			}
		})
	}
}

// TestValidationExhausted: an exhausted evaluation has four outcomes. Before
// a cutoff that applies to the date, a first run is released: the trigger row
// is deleted after an evaluation-released joblog row, then
// EVALUATION_RELEASED (best effort). For a rerun, or a row read without a
// cutoff (validation rejects one, so only an unvalidated read sees it), the run
// ends: validation-exhausted, VALIDATION_EXHAUSTED, then FAILED_FINAL. At or
// after the cutoff a first run ends with one SENSOR_DEADLINE_EXPIRED, published
// before FAILED_FINAL, and no joblog row. A missing or finished trigger row is
// left alone. No rerun lookup is made when no cutoff applies.
func TestValidationExhausted(t *testing.T) {
	expired, exhausted := "event "+string(types.EventSensorDeadlineExpired), "event "+string(types.EventValidationExhausted)
	released := "event " + string(types.EventEvaluationReleased)
	running, final, completed := types.TriggerStatusRunning, types.TriggerStatusFailedFinal, types.TriggerStatusCompleted
	wantDetail := map[string]interface{}{"source": "orchestrator", "cutoff": "14:00", "cutoffAt": "2026-03-10T14:00:00Z",
		"actionHint": "cutoff passed and the pipeline did not start; nothing more will start for this date. " +
			"Investigate, then submit a RERUN_REQUEST to run it manually"}
	releaseHint := "the lock is released; a new sensor write or the watchdog starts evaluation again before the cutoff"
	releasedDetail := map[string]interface{}{"source": "orchestrator", "cutoff": "14:00", "cutoffAt": "2026-03-10T14:00:00Z", "actionHint": releaseHint}
	type call struct {
		now        time.Time
		putErr     error
		wantErr    bool
		wantErrSub string
		wantStatus string
		wantRow    string // trigger row status after the call, "" for none; a RUNNING row keeps its staleAt, a FAILED_FINAL one has none
	}
	noCutoff := func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = "" }
	hourly := func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = ":15" }
	release := func(now time.Time) []call { return []call{{now: now, wantStatus: "released"}} }
	exhaust := func(now time.Time) []call { return []call{{now: now, wantStatus: "exhausted", wantRow: final}} }
	ve, er := types.JobEventValidationExhausted, types.JobEventEvaluationReleased
	tests := []struct {
		name         string
		tweak        func(*types.PipelineConfig)
		date         string // "" for cutoffDate
		noRow        bool
		startStatus  string // set on the trigger row before the first call
		rerun        bool
		noConfig     bool
		failConfig   bool
		failRerun    bool
		deleteFails  int // DeleteItem fails this many times first
		joblogFails  bool
		noRerunQuery bool
		calls        []call
		wantJoblog   []string
		wantOps      []string
		wantDetail   map[string]interface{}
	}{
		{name: "BeforeCutoffReleased", calls: release(at(0, 13, 59)),
			wantJoblog: []string{er}, wantOps: []string{"release", released}, wantDetail: releasedDetail},
		{name: "ReleasedThenTaskRetry", calls: append(release(at(0, 13, 59)), release(at(0, 13, 59))...),
			wantJoblog: []string{er}, wantOps: []string{"release", released}, wantDetail: releasedDetail},
		{name: "ReleaseEventFails", calls: []call{{now: at(0, 13, 59), putErr: errors.New("eventbridge: throttled"), wantStatus: "released"}},
			wantJoblog: []string{er}, wantOps: []string{"release"}},
		{name: "ReleaseDeleteFailsThenWorks", deleteFails: 1, calls: []call{
			{now: at(0, 13, 58), wantErr: true, wantRow: running},
			{now: at(0, 13, 59), wantStatus: "released"},
		}, wantJoblog: []string{er, er}, wantOps: []string{"release", released}, wantDetail: releasedDetail},
		{name: "HourlyCutoffHourlyKeyReleased", tweak: hourly, date: "2026-03-10T10", calls: release(at(0, 11, 0)),
			wantJoblog: []string{er}, wantOps: []string{"release", released}, wantDetail: map[string]interface{}{
				"source": "orchestrator", "cutoff": ":15", "cutoffAt": "2026-03-10T11:15:00Z", "actionHint": releaseHint}},
		{name: "NoTriggerRowBeforeCutoff", noRow: true, calls: release(at(0, 13, 59))},
		{name: "TriggerAlreadyFinishedReleaseFailedFinal", startStatus: final,
			calls: []call{{now: at(0, 13, 59), wantStatus: "released", wantRow: final}}},
		{name: "TriggerAlreadyFinishedReleaseCompleted", startStatus: completed,
			calls: []call{{now: at(0, 13, 59), wantStatus: "released", wantRow: completed}}},
		{name: "RerunLookupFailsBeforeCutoff", failRerun: true, calls: []call{{now: at(0, 13, 59), wantErr: true, wantRow: running}}},
		{name: "RowWithoutCutoff", tweak: noCutoff, noRerunQuery: true, calls: exhaust(at(0, 14, 30)),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "HourlyCutoffDailyDate", tweak: hourly, noRerunQuery: true, calls: exhaust(at(0, 13, 0)),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "DailyCutoffHourlyKey", date: "2026-03-10T10", noRerunQuery: true, calls: exhaust(at(0, 13, 0)),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "NoConfigExhausted", noConfig: true, noRerunQuery: true, calls: exhaust(at(0, 14, 30)),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "RerunBeforeCutoff", rerun: true, calls: exhaust(at(0, 13, 59)),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "RerunAfterCutoff", rerun: true, calls: exhaust(at(0, 14, 30)),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "ExhaustedPublishFailsThenWorks", rerun: true, calls: []call{
			{now: at(0, 14, 29), putErr: errors.New("eventbridge: throttled"), wantErr: true, wantRow: running},
			{now: at(0, 14, 30), wantStatus: "exhausted", wantRow: final},
		}, wantJoblog: []string{ve, ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "ExhaustedTaskRetry", rerun: true, calls: append(exhaust(at(0, 14, 30)), exhaust(at(0, 14, 30))...),
			wantJoblog: []string{ve}, wantOps: []string{exhausted, "status " + final}},
		{name: "TriggerAlreadyFinishedExhaustFailedFinal", rerun: true, startStatus: final,
			calls: []call{{now: at(0, 14, 30), wantStatus: "exhausted", wantRow: final}}},
		{name: "TriggerAlreadyFinishedExhaustCompleted", rerun: true, startStatus: completed,
			calls: []call{{now: at(0, 14, 30), wantStatus: "exhausted", wantRow: completed}}},
		{name: "AtCutoffThenTaskRetry", calls: []call{
			{now: at(0, 14, 0), wantStatus: "cutoff", wantRow: final},
			{now: at(0, 14, 0), wantStatus: "cutoff", wantRow: final},
		}, wantOps: []string{expired, "status " + final}, wantDetail: wantDetail},
		{name: "PublishFailsThenWorks", calls: []call{
			{now: at(0, 14, 30), putErr: errors.New("eventbridge: throttled"), wantErr: true, wantRow: running},
			{now: at(0, 14, 30), wantStatus: "cutoff", wantRow: final},
		}, wantOps: []string{expired, "status " + final}, wantDetail: wantDetail},
		{name: "NoTriggerRow", noRow: true, calls: []call{{now: at(0, 14, 30), wantStatus: "cutoff"}}},
		{name: "RerunLookupFails", failRerun: true, calls: []call{{now: at(0, 14, 30), wantErr: true, wantRow: running}}},
		{name: "ConfigReadFails", failConfig: true, calls: []call{{now: at(0, 14, 30), wantErr: true, wantRow: running}}},
		{name: "ExhaustedJoblogWriteFails", rerun: true, joblogFails: true, calls: []call{
			{now: at(0, 14, 30), wantErr: true, wantErrSub: "write validation-exhausted joblog", wantRow: running}}},
		{name: "ReleaseJoblogWriteFails", joblogFails: true, calls: []call{
			{now: at(0, 13, 59), wantErr: true, wantErrSub: "write evaluation-released joblog", wantRow: running}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := cutoffCfg()
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			date := cmp.Or(tt.date, cutoffDate)
			now := tt.calls[0].now
			d, mem, rec := cutoffDeps(t, cfg, &now)
			d.Store.Now = d.NowFunc // joblog rows written at different times get different keys
			fake := d.Store.Client.(*storetest.FakeDynamo)
			if !tt.noRow {
				if ok, err := d.Store.AcquireTriggerLock(ctx, "p", "s", date, time.Hour); err != nil || !ok {
					t.Fatalf("acquire trigger lock = %v, %v", ok, err)
				}
			}
			if tt.startStatus != "" {
				if err := d.Store.SetTriggerStatus(ctx, "p", "s", date, tt.startStatus); err != nil {
					t.Fatalf("set trigger status: %v", err)
				}
			}
			if tt.rerun {
				if _, err := d.Store.WriteRerun(ctx, "p", "s", date, "manual", ""); err != nil {
					t.Fatalf("write rerun: %v", err)
				}
			}
			if tt.noConfig {
				if _, err := fake.DeleteItem(ctx, &dynamodb.DeleteItemInput{TableName: aws.String("control"), Key: map[string]ddbtypes.AttributeValue{
					"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")}, "SK": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK}}}); err != nil {
					t.Fatalf("delete config: %v", err)
				}
			}
			if tt.failConfig {
				fake.GetItemFn = func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("dynamodb: internal error")
				}
			}
			rerunQueries, query := 0, fake.QueryFn
			fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				if *in.TableName == "rerun" {
					rerunQueries++
				}
				return query(ctx, in)
			}
			if tt.failRerun {
				failRerunQuery(d)
			}
			deleteFails, del := tt.deleteFails, fake.DeleteItemFn
			fake.DeleteItemFn = func(ctx context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
				if deleteFails > 0 {
					deleteFails--
					return nil, errors.New("dynamodb: throttled")
				}
				return del(ctx, in)
			}
			if tt.joblogFails {
				failTablePut(d, "joblog", errors.New("dynamodb: throttled"))
			}
			rec.ops = nil

			for i, c := range tt.calls {
				now, rec.putErr = c.now, c.putErr
				out, err := orchestrator.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
					Mode: "validation-exhausted", PipelineID: "p", ScheduleID: "s", Date: date,
				})
				if (err != nil) != c.wantErr {
					t.Fatalf("call %d: err = %v, wantErr %v", i, err, c.wantErr)
				}
				if c.wantErrSub != "" && !strings.Contains(err.Error(), c.wantErrSub) {
					t.Errorf("call %d: err = %v, want it to contain %q", i, err, c.wantErrSub)
				}
				if !c.wantErr && (out.Mode != "validation-exhausted" || out.Status != c.wantStatus) {
					t.Errorf("call %d: output = %+v, want status %q", i, out, c.wantStatus)
				}
				row := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("s", date))
				if got := strAttr(t, row, "status"); got != c.wantRow {
					t.Errorf("call %d: trigger row status = %q, want %q", i, got, c.wantRow)
				}
				if _, hasStaleAt := row["staleAt"]; row != nil && hasStaleAt != (c.wantRow == running) {
					t.Errorf("call %d: %s trigger row has staleAt = %v", i, c.wantRow, hasStaleAt)
				}
			}
			var joblog []string
			for _, row := range mem.Items("joblog") {
				joblog = append(joblog, strAttr(t, row, "event"))
			}
			if !slices.Equal(joblog, tt.wantJoblog) {
				t.Errorf("joblog events = %v, want %v", joblog, tt.wantJoblog)
			}
			if !slices.Equal(rec.ops, tt.wantOps) {
				t.Errorf("ops = %v, want %v", rec.ops, tt.wantOps)
			}
			if tt.wantDetail != nil && (len(rec.events) != 1 || !reflect.DeepEqual(rec.events[0].Detail, tt.wantDetail)) {
				t.Errorf("events = %+v, want one with detail %v", rec.events, tt.wantDetail)
			}
			if tt.noRerunQuery && rerunQueries != 0 {
				t.Errorf("rerun table queried %d times, want none without a cutoff", rerunQueries)
			}
		})
	}
}
