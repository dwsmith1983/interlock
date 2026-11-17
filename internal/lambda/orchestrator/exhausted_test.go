package orchestrator_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

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
	if _, err := orchestrator.HandleOrchestrator(context.Background(), d, lambda.OrchestratorInput{
		Mode: "job-poll-exhausted", PipelineID: "p", ScheduleID: "s", Date: ctDate, RunID: "run-1",
	}); err != nil {
		t.Fatalf("job-poll-exhausted: %v", err)
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

// TestValidationExhausted_Cutoff: an exhausted evaluation at or after the
// cutoff, for a date that is not a rerun, ends the RUNNING run with one
// SENSOR_DEADLINE_EXPIRED, published before FAILED_FINAL, and no joblog row.
// Every other case writes the validation-exhausted row as before.
func TestValidationExhausted_Cutoff(t *testing.T) {
	expired, exhausted := "event "+string(types.EventSensorDeadlineExpired), "event "+string(types.EventValidationExhausted)
	running, final := types.TriggerStatusRunning, types.TriggerStatusFailedFinal
	wantDetail := map[string]interface{}{"source": "orchestrator", "cutoff": "14:00", "cutoffAt": "2026-03-10T14:00:00Z",
		"actionHint": "cutoff passed and the pipeline did not start; nothing more will start for this date. " +
			"Investigate, then submit a RERUN_REQUEST to run it manually"}
	type call struct {
		now        time.Time
		putErr     error
		wantErr    bool
		wantStatus string
		wantRow    string // trigger row status after the call, "" for none; a RUNNING row keeps its ttl, a FAILED_FINAL one has none
	}
	asBefore := func(now time.Time) []call { return []call{{now: now, wantStatus: "exhausted", wantRow: running}} }
	tests := []struct {
		name       string
		tweak      func(*types.PipelineConfig)
		noRow      bool
		rerun      bool
		failConfig bool
		failRerun  bool
		calls      []call
		wantJoblog []string
		wantOps    []string
		wantDetail map[string]interface{}
	}{
		{name: "AtCutoffThenTaskRetry", calls: []call{
			{now: at(0, 14, 0), wantStatus: "cutoff", wantRow: final},
			{now: at(0, 14, 0), wantStatus: "cutoff", wantRow: final},
		}, wantOps: []string{expired, "status " + final}, wantDetail: wantDetail},
		{name: "BeforeCutoff", calls: asBefore(at(0, 13, 59)),
			wantJoblog: []string{types.JobEventValidationExhausted}, wantOps: []string{exhausted}},
		{name: "NoCutoff", tweak: func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = "" }, calls: asBefore(at(0, 14, 30)),
			wantJoblog: []string{types.JobEventValidationExhausted}, wantOps: []string{exhausted}},
		{name: "RerunAfterCutoff", rerun: true, calls: asBefore(at(0, 14, 30)),
			wantJoblog: []string{types.JobEventValidationExhausted}, wantOps: []string{exhausted}},
		{name: "PublishFailsThenWorks", calls: []call{
			{now: at(0, 14, 30), putErr: errors.New("eventbridge: throttled"), wantErr: true, wantRow: running},
			{now: at(0, 14, 30), wantStatus: "cutoff", wantRow: final},
		}, wantOps: []string{expired, "status " + final}, wantDetail: wantDetail},
		{name: "NoTriggerRow", noRow: true, calls: []call{{now: at(0, 14, 30), wantStatus: "cutoff"}}},
		{name: "RerunLookupFails", failRerun: true, calls: []call{{now: at(0, 14, 30), wantErr: true, wantRow: running}}},
		{name: "ConfigReadFails", failConfig: true, calls: []call{{now: at(0, 14, 30), wantErr: true, wantRow: running}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			cfg := cutoffCfg()
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			now := tt.calls[0].now
			d, mem, rec := cutoffDeps(t, cfg, &now)
			if !tt.noRow {
				if ok, err := d.Store.AcquireTriggerLock(ctx, "p", "s", cutoffDate, time.Hour); err != nil || !ok {
					t.Fatalf("acquire trigger lock = %v, %v", ok, err)
				}
			}
			if tt.rerun {
				if _, err := d.Store.WriteRerun(ctx, "p", "s", cutoffDate, "manual", ""); err != nil {
					t.Fatalf("write rerun: %v", err)
				}
			}
			if tt.failConfig {
				d.Store.Client.(*storetest.FakeDynamo).GetItemFn = func(context.Context, *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					return nil, errors.New("dynamodb: internal error")
				}
			}
			if tt.failRerun {
				failRerunQuery(d)
			}
			rec.ops = nil

			for i, c := range tt.calls {
				now, rec.putErr = c.now, c.putErr
				out, err := orchestrator.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
					Mode: "validation-exhausted", PipelineID: "p", ScheduleID: "s", Date: cutoffDate,
				})
				if (err != nil) != c.wantErr {
					t.Fatalf("call %d: err = %v, wantErr %v", i, err, c.wantErr)
				}
				if !c.wantErr && (out.Mode != "validation-exhausted" || out.Status != c.wantStatus) {
					t.Errorf("call %d: output = %+v, want status %q", i, out, c.wantStatus)
				}
				row := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("s", cutoffDate))
				if got := strAttr(t, row, "status"); got != c.wantRow {
					t.Errorf("call %d: trigger row status = %q, want %q", i, got, c.wantRow)
				}
				if _, hasTTL := row["ttl"]; row != nil && hasTTL != (c.wantRow == running) {
					t.Errorf("call %d: %s trigger row has ttl = %v", i, c.wantRow, hasTTL)
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
		})
	}
}
