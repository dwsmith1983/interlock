package orchestrator_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	ctDate = "2026-03-01"
	ctExec = "p-s-2026-03-01-1709280000" // a first run's name (lambda.StartSFN)
)

type published struct {
	Type   string
	Detail map[string]interface{}
}

// recorder is the Step Functions and EventBridge double. A repeated execution
// name gets ExecutionAlreadyExists, as Step Functions does. ops logs every
// start, publish and (through memDeps) store write in order.
type recorder struct {
	mu       sync.Mutex
	startErr error
	putErr   error // PutEvents fails with it, recording nothing
	started  []string
	events   []published
	ops      []string
}

func (r *recorder) log(op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ops = append(r.ops, op)
}

func (r *recorder) StartExecution(_ context.Context, in *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	r.log("start " + *in.Name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startErr != nil {
		return nil, r.startErr
	}
	if slices.Contains(r.started, *in.Name) {
		return nil, &sfntypes.ExecutionAlreadyExists{}
	}
	r.started = append(r.started, *in.Name)
	return &sfn.StartExecutionOutput{}, nil
}

func (r *recorder) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	r.mu.Lock()
	putErr := r.putErr
	r.mu.Unlock()
	if putErr != nil {
		return nil, putErr
	}
	for _, e := range in.Entries {
		var evt types.InterlockEvent
		if err := json.Unmarshal([]byte(*e.Detail), &evt); err != nil {
			return nil, err
		}
		r.log("event " + *e.DetailType)
		r.mu.Lock()
		r.events = append(r.events, published{Type: *e.DetailType, Detail: evt.Detail})
		r.mu.Unlock()
	}
	return &eventbridge.PutEventsOutput{}, nil
}

// memDeps wires an in-memory store holding cfg, and the recorder. Trigger
// status writes and rerun rows that reach the store are logged as
// "status <S>" and "rerun <execution>", and trigger row deletes as "release".
func memDeps(t *testing.T, cfg types.PipelineConfig) (*lambda.Deps, *storetest.Mem, *recorder) {
	t.Helper()
	fake, mem := storetest.NewMem()
	mem.Put("control", configItem(t, cfg))
	rec := &recorder{}
	put, update, del := fake.PutItemFn, fake.UpdateItemFn, fake.DeleteItemFn
	fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		out, err := put(ctx, in)
		if sk := strAttr(t, in.Item, "SK"); err == nil && *in.TableName == "rerun" {
			rec.log("rerun " + sk[strings.LastIndex(sk, "#")+1:])
		} else if err == nil && strings.HasPrefix(sk, "TRIGGER#") {
			rec.log("status " + strAttr(t, in.Item, "status"))
		}
		return out, err
	}
	fake.UpdateItemFn = func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		out, err := update(ctx, in)
		for _, v := range []string{":s", ":running"} {
			if av, ok := in.ExpressionAttributeValues[v]; ok && err == nil && strings.Contains(*in.UpdateExpression, v) {
				rec.log("status " + av.(*ddbtypes.AttributeValueMemberS).Value)
			}
		}
		return out, err
	}
	fake.DeleteItemFn = func(ctx context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
		out, err := del(ctx, in)
		if err == nil && strings.HasPrefix(strAttr(t, in.Key, "SK"), "TRIGGER#") {
			rec.log("release")
		}
		return out, err
	}
	s := storetest.NewStore(fake)
	return &lambda.Deps{
		Store:           s,
		ConfigCache:     store.NewConfigCache(s, time.Minute),
		SFNClient:       rec,
		EventBridge:     rec,
		EventBusName:    "bus",
		StateMachineARN: "arn:aws:states:us-east-1:1:stateMachine:sm",
		NowFunc:         func() time.Time { return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC) },
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, mem, rec
}

func failCfg(maxRetries int) types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: map[string]interface{}{"url": "https://example.com"}, MaxRetries: maxRetries},
	}
}

func categorizedRow(millis, runID string, cat types.FailureCategory) map[string]ddbtypes.AttributeValue {
	row := jobLogItem(millis, types.JobEventFail, runID, "boom")
	row["category"] = &ddbtypes.AttributeValueMemberS{Value: string(cat)}
	return row
}

func strItem(kv ...string) map[string]ddbtypes.AttributeValue {
	item := map[string]ddbtypes.AttributeValue{}
	for i := 0; i < len(kv); i += 2 {
		item[kv[i]] = &ddbtypes.AttributeValueMemberS{Value: kv[i+1]}
	}
	return item
}

type ctCall struct{ event, exec, runID, wantStatus, wantNext, wantErr string }

func failCall(exec, wantStatus, wantNext string) ctCall {
	return ctCall{types.JobEventFail, exec, exec, wantStatus, wantNext, ""}
}

func TestCompleteTrigger_Failure(t *testing.T) {
	zero, one := 0, 1
	retrying, final, running := orchestrator.StatusRetrying, types.TriggerStatusFailedFinal, types.TriggerStatusRunning
	r1, r2 := ctExec+"-r1", ctExec+"-r2"
	exhausted, excluded := "event "+string(types.EventRetryExhausted), "event "+string(types.EventPipelineExcluded)
	toFinal := "status " + final
	tests := []struct {
		name          string
		maxRetries    int
		tweak         func(*types.PipelineConfig)
		row           string // TRIGGER# status before the calls: "" is RUNNING, "none" no row
		prior         bool   // r1 was already started and recorded (a Task retry after it)
		startErr      error
		rows          []map[string]ddbtypes.AttributeValue // job log, newest first
		calls         []ctCall
		wantOps       []string
		wantTrigger   string
		wantTTL       bool
		wantReruns    []string // execution names keying the rerun rows
		wantExhausted map[string]interface{}
	}{
		{
			name: "RetriesUntilMaxRetries", maxRetries: 2,
			calls:       []ctCall{failCall(ctExec, retrying, r1), failCall(r1, retrying, r2), failCall(r2, final, "")},
			wantOps:     []string{"rerun " + r1, "start " + r1, "rerun " + r2, "start " + r2, toFinal, exhausted},
			wantTrigger: final, wantReruns: []string{r1, r2},
			wantExhausted: map[string]interface{}{"retries": float64(2), "maxRetries": float64(2), "runId": r2},
		},
		{
			name: "PermanentUsesMaxCodeRetries", maxRetries: 3, tweak: func(c *types.PipelineConfig) { c.Job.MaxCodeRetries = &one },
			rows:    []map[string]ddbtypes.AttributeValue{categorizedRow("300", "other-run", types.FailureTransient), categorizedRow("200", r1, types.FailurePermanent)},
			calls:   []ctCall{failCall(r1, final, "")},
			wantOps: []string{toFinal, exhausted}, wantTrigger: final,
			wantExhausted: map[string]interface{}{"retries": float64(1), "maxRetries": float64(1), "runId": r1},
		},
		{
			name: "PermanentUsesMaxCodeRetries_OtherRunIgnored", maxRetries: 3, tweak: func(c *types.PipelineConfig) { c.Job.MaxCodeRetries = &one },
			rows:    []map[string]ddbtypes.AttributeValue{categorizedRow("300", "other-run", types.FailurePermanent), categorizedRow("200", r1, types.FailureTransient)},
			calls:   []ctCall{failCall(r1, retrying, r2)},
			wantOps: []string{"rerun " + r2, "start " + r2}, wantTrigger: running, wantTTL: true, wantReruns: []string{r2},
		},
		{
			// The job may still be running, so neither budget applies.
			name: "OutcomeUnknownIsFinal", maxRetries: 2, tweak: func(c *types.PipelineConfig) { c.Job.MaxCodeRetries = &one },
			rows:    []map[string]ddbtypes.AttributeValue{categorizedRow("200", ctExec, types.FailureOutcomeUnknown)},
			calls:   []ctCall{failCall(ctExec, final, "")},
			wantOps: []string{toFinal}, wantTrigger: final,
		},
		{
			name: "RetriedLambdaDoesNotDoubleStart", maxRetries: 2,
			calls:   []ctCall{failCall(ctExec, retrying, r1), failCall(ctExec, retrying, r1)},
			wantOps: []string{"rerun " + r1, "start " + r1, "start " + r1}, wantTrigger: running, wantTTL: true, wantReruns: []string{r1},
		},
		{
			// maxCodeRetries 0 makes a PERMANENT reading final, so a retry
			// proves a missing row counts as transient.
			name: "TimeoutWithoutRowRetries", maxRetries: 1, tweak: func(c *types.PipelineConfig) { c.Job.MaxCodeRetries = &zero },
			calls:   []ctCall{{types.JobEventTimeout, ctExec, ctExec, retrying, r1, ""}},
			wantOps: []string{"rerun " + r1, "start " + r1}, wantTrigger: running, wantTTL: true, wantReruns: []string{r1},
		},
		{
			// The Task's retry reads the row again; nothing starts on a guess.
			name: "LookupErrorIsAnError", maxRetries: 1,
			calls:       []ctCall{{types.JobEventTimeout, ctExec, "", "", "", "runID is required"}},
			wantTrigger: running,
		},
		{
			name: "ExcludedDateIsFinal", maxRetries: 2,
			tweak:   func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{ctDate}} },
			calls:   []ctCall{failCall(ctExec, final, "")},
			wantOps: []string{toFinal, excluded}, wantTrigger: final,
		},
		{
			name: "ExcludedDateIsFinal_BudgetSpent", maxRetries: 1,
			tweak:   func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{ctDate}} },
			calls:   []ctCall{failCall(r1, final, "")},
			wantOps: []string{toFinal, excluded}, wantTrigger: final,
		},
		{
			name: "MaxRetriesZero", calls: []ctCall{failCall(ctExec, final, "")},
			wantOps: []string{toFinal, exhausted}, wantTrigger: final,
			wantExhausted: map[string]interface{}{"retries": float64(0), "maxRetries": float64(0), "runId": ctExec},
		},
		{
			name: "DryRunIsFinal", maxRetries: 2,
			tweak: func(c *types.PipelineConfig) {
				c.DryRun, c.Schedule.Trigger = true, &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}
			},
			calls:   []ctCall{failCall(ctExec, final, "")},
			wantOps: []string{toFinal}, wantTrigger: final,
		},
		{
			// The pipeline ID in the stored config does not match, so the
			// cache finds no valid config for p.
			name: "InvalidConfigIsFinal", maxRetries: 2, tweak: func(c *types.PipelineConfig) { c.Pipeline.ID = "other" },
			calls:   []ctCall{failCall(ctExec, final, "")},
			wantOps: []string{toFinal}, wantTrigger: final,
		},
		{
			name: "LockRowGoneIsRecreated", maxRetries: 1, row: "none",
			calls:   []ctCall{failCall(ctExec, retrying, r1)},
			wantOps: []string{"rerun " + r1, "start " + r1, "status " + running}, wantTrigger: running, wantTTL: true, wantReruns: []string{r1},
		},
		{
			name: "TerminalRowLeftAlone", maxRetries: 1, row: final,
			calls:   []ctCall{failCall(ctExec, retrying, r1)},
			wantOps: []string{"rerun " + r1, "start " + r1}, wantTrigger: final, wantReruns: []string{r1},
		},
		{
			// A Task retry of CompleteTrigger after r1 already finished.
			name: "CompletedRowStaysCompleted", maxRetries: 1, row: types.TriggerStatusCompleted, prior: true,
			calls:   []ctCall{failCall(ctExec, retrying, r1)},
			wantOps: []string{"start " + r1}, wantTrigger: types.TriggerStatusCompleted, wantReruns: []string{r1},
		},
		{
			name: "StartFailureIsFinal", maxRetries: 1, startErr: errors.New("throttled"),
			calls:       []ctCall{failCall(ctExec, final, "")},
			wantOps:     []string{"rerun " + r1, "start " + r1, toFinal, "event " + string(types.EventInfraFailure)},
			wantTrigger: final, wantReruns: []string{r1},
		},
		{
			// The Task retry sees ExecutionAlreadyExists and must not
			// recreate a lock row it did not start the execution for.
			name: "AlreadyStartedLeavesMissingRowMissing", maxRetries: 1, row: "none", prior: true,
			calls:   []ctCall{failCall(ctExec, retrying, r1)},
			wantOps: []string{"start " + r1}, wantReruns: []string{r1},
		},
		{
			// A Task retry after r1 ran and completed, whose start then fails.
			name: "StartFailureKeepsCompletedRow", maxRetries: 1, row: types.TriggerStatusCompleted, startErr: errors.New("throttled"),
			calls:   []ctCall{failCall(ctExec, retrying, r1)},
			wantOps: []string{"rerun " + r1, "start " + r1}, wantTrigger: types.TriggerStatusCompleted, wantReruns: []string{r1},
		},
		{
			name: "EmptyExecutionNameIsAnError", maxRetries: 1,
			calls:       []ctCall{{types.JobEventFail, "", ctExec, "", "", "executionName is required"}},
			wantTrigger: running,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := failCfg(tt.maxRetries)
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, rec := memDeps(t, cfg)
			trigger := []string{"PK", types.PipelinePK("p"), "SK", types.TriggerSK("s", ctDate)}
			if row := cmp.Or(tt.row, running); row != "none" {
				mem.Put("control", strItem(append(trigger, "status", row)...))
			}
			if tt.prior {
				rec.started = []string{r1}
				mem.Put("rerun", strItem("PK", types.PipelinePK("p"), "SK", "RERUN#s#"+ctDate+"#"+r1, "reason", "job-fail-retry"))
			}
			for _, row := range tt.rows {
				mem.Put("joblog", row)
			}
			rec.startErr = tt.startErr

			for i, c := range tt.calls {
				out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
					Mode: "complete-trigger", PipelineID: "p", ScheduleID: "s", Date: ctDate,
					Event: c.event, ExecutionName: c.exec, RunID: c.runID,
				})
				if c.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), c.wantErr) {
						t.Fatalf("call %d: error %v, want it to contain %q", i+1, err, c.wantErr)
					}
					continue
				}
				if err != nil || out.Status != c.wantStatus || out.NextExecution != c.wantNext {
					t.Fatalf("call %d (%s): status %q next %q err %v, want %q %q", i+1, c.exec, out.Status, out.NextExecution, err, c.wantStatus, c.wantNext)
				}
			}

			if !slices.Equal(rec.ops, tt.wantOps) {
				t.Errorf("ops %q, want %q", rec.ops, tt.wantOps)
			}
			item := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("s", ctDate))
			if _, hasTTL := item["ttl"]; strAttr(t, item, "status") != tt.wantTrigger || hasTTL != tt.wantTTL {
				t.Errorf("trigger row %v, want status %q ttl %v", item, tt.wantTrigger, tt.wantTTL)
			}
			var reruns []string
			for _, row := range mem.Items("rerun") {
				reruns = append(reruns, strings.TrimPrefix(strAttr(t, row, "SK"), "RERUN#s#"+ctDate+"#"))
				if strAttr(t, row, "reason") != "job-fail-retry" {
					t.Errorf("rerun row %v, want reason job-fail-retry", row)
				}
			}
			if !slices.Equal(reruns, tt.wantReruns) {
				t.Errorf("rerun rows %v, want %v", reruns, tt.wantReruns)
			}
			for _, e := range rec.events {
				if e.Type == string(types.EventRetryExhausted) && !maps.Equal(e.Detail, tt.wantExhausted) {
					t.Errorf("RETRY_EXHAUSTED detail %v, want %v", e.Detail, tt.wantExhausted)
				}
			}
		})
	}
}

// TestCompleteTrigger_SuccessStampsCompletedAt: a success sets COMPLETED with
// the completion time and no TTL; a later completion moves the time forward.
func TestCompleteTrigger_SuccessStampsCompletedAt(t *testing.T) {
	d, mem, rec := memDeps(t, failCfg(0))
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return now }
	row := strItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("s", ctDate), "status", types.TriggerStatusRunning)
	row["ttl"] = &ddbtypes.AttributeValueMemberN{Value: "1"}
	mem.Put("control", row)

	for _, want := range []string{"2026-03-01T12:00:00Z", "2026-03-01T12:05:00Z"} {
		out, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
			Mode: "complete-trigger", PipelineID: "p", ScheduleID: "s", Date: ctDate,
			Event: types.JobEventSuccess, ExecutionName: ctExec, RunID: ctExec,
		})
		if err != nil || out.Status != types.TriggerStatusCompleted {
			t.Fatalf("complete-trigger at %s: status %q err %v, want %q", want, out.Status, err, types.TriggerStatusCompleted)
		}
		item := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("s", ctDate))
		if _, hasTTL := item["ttl"]; strAttr(t, item, "status") != types.TriggerStatusCompleted || strAttr(t, item, "completedAt") != want || hasTTL {
			t.Errorf("trigger row %v, want status COMPLETED, completedAt %s and no ttl", item, want)
		}
		now = now.Add(5 * time.Minute)
	}
	if !slices.Contains(rec.ops, "status "+types.TriggerStatusCompleted) {
		t.Errorf("ops %q, want them to hold %q", rec.ops, "status "+types.TriggerStatusCompleted)
	}
}

// TestCompleteTrigger_SuccessStatusWriteFails: without COMPLETED the run is
// not done, so the failed write is a Lambda error and nothing is published.
func TestCompleteTrigger_SuccessStatusWriteFails(t *testing.T) {
	d, mem, rec := memDeps(t, failCfg(0))
	mem.Put("control", strItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("s", ctDate), "status", types.TriggerStatusRunning))
	d.Store.Client.(*storetest.FakeDynamo).UpdateItemFn = func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
		return nil, errors.New("dynamodb: throttled")
	}
	_, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "complete-trigger", PipelineID: "p", ScheduleID: "s", Date: ctDate, Event: types.JobEventSuccess,
	})
	if err == nil || !strings.Contains(err.Error(), "set trigger status") {
		t.Errorf("err = %v, want the status write error", err)
	}
	if len(rec.events) != 0 || len(rec.ops) != 0 {
		t.Errorf("events %v ops %v, want none", rec.events, rec.ops)
	}
}

// TestCompleteTrigger_PostRunBaseline: a success with post-run rules stores
// the rule sensors' data (per-period keys under their base key) as the
// date's baseline. A failure, or a pipeline without post-run rules, stores
// none; a failed capture is published but does not fail the run.
func TestCompleteTrigger_PostRunBaseline(t *testing.T) {
	const date = "2026-03-08T06"
	captured := map[string]interface{}{"audit-result": map[string]interface{}{"sensor_count": 150.0}}
	sensors := map[string]map[string]interface{}{"audit-result": {"sensor_count": 150}, "other": {"x": 1}}
	tests := []struct {
		name         string
		sensors      map[string]map[string]interface{}
		event        string
		noPostRun    bool
		failSensors  bool
		wantStatus   string
		wantBaseline map[string]interface{}
		wantEvents   []string
		wantOps      []string
	}{
		{name: "captures", sensors: sensors, wantStatus: types.TriggerStatusCompleted, wantBaseline: captured,
			wantEvents: []string{string(types.EventPostRunBaselineCaptured)},
			wantOps:    []string{"status " + types.TriggerStatusCompleted, "event " + string(types.EventPostRunBaselineCaptured)}},
		{name: "perPeriodRemap", sensors: map[string]map[string]interface{}{"audit-result#20260308T06": {"sensor_count": 42}},
			wantStatus: types.TriggerStatusCompleted, wantBaseline: map[string]interface{}{"audit-result": map[string]interface{}{"sensor_count": 42.0}},
			wantEvents: []string{string(types.EventPostRunBaselineCaptured)}},
		{name: "failNoBaseline", sensors: sensors, event: types.JobEventFail, wantStatus: types.TriggerStatusFailedFinal,
			wantEvents: []string{string(types.EventRetryExhausted)}},
		// Failing sensor reads prove none is made without post-run rules.
		{name: "noPostRun", sensors: sensors, noPostRun: true, failSensors: true, wantStatus: types.TriggerStatusCompleted},
		{name: "sensorReadFails", failSensors: true, wantStatus: types.TriggerStatusCompleted,
			wantEvents: []string{string(types.EventBaselineCaptureFailed)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := failCfg(0)
			if !tt.noPostRun {
				cfg.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{
					{Key: "audit-result", Check: types.CheckGTE, Field: "sensor_count", Value: 100}}}
			}
			d, mem, rec := memDeps(t, cfg)
			mem.Put("control", strItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("s", date), "status", types.TriggerStatusRunning))
			for key, data := range tt.sensors {
				if err := d.Store.WriteSensor(ctx, "p", key, data); err != nil {
					t.Fatalf("write sensor %s: %v", key, err)
				}
			}
			if tt.failSensors {
				failTableQuery(d, "control", errors.New("dynamodb: throttled"))
			}
			rec.ops = nil

			out, err := orchestrator.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
				Mode: "complete-trigger", PipelineID: "p", ScheduleID: "s", Date: date, Event: cmp.Or(tt.event, types.JobEventSuccess), RunID: "run-1",
			})
			if err != nil || out.Status != tt.wantStatus {
				t.Fatalf("complete-trigger = %+v, %v, want status %q", out, err, tt.wantStatus)
			}
			baseline, err := d.Store.GetSensorData(ctx, "p", "postrun-baseline#"+date)
			if err != nil {
				t.Fatalf("read baseline: %v", err)
			}
			if !reflect.DeepEqual(baseline, tt.wantBaseline) {
				t.Errorf("baseline = %v, want %v", baseline, tt.wantBaseline)
			}
			if got := eventList(rec); !slices.Equal(got, tt.wantEvents) {
				t.Errorf("events = %v, want %v", got, tt.wantEvents)
			}
			if tt.wantOps != nil && !slices.Equal(rec.ops, tt.wantOps) {
				t.Errorf("ops = %v, want %v", rec.ops, tt.wantOps)
			}
		})
	}
}
