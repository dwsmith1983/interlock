package orchestrator_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
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
			name: "TimeoutWithoutRowRetries_LookupError", maxRetries: 1, tweak: func(c *types.PipelineConfig) { c.Job.MaxCodeRetries = &zero },
			calls:   []ctCall{{types.JobEventTimeout, ctExec, "", retrying, r1, ""}},
			wantOps: []string{"rerun " + r1, "start " + r1}, wantTrigger: running, wantTTL: true, wantReruns: []string{r1},
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
			calls:       []ctCall{{types.JobEventFail, "", "", "", "", "executionName is required"}},
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
