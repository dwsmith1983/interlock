package orchestrator_test

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const cutoffDate = "2026-03-10"

// cutoffCfg is a pipeline whose run date 2026-03-10 has a 14:00 UTC cutoff
// and whose one rule needs the "upstream" sensor.
func cutoffCfg() types.PipelineConfig {
	return types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{
			Key: "ready", Check: types.CheckEquals, Field: "status", Value: "ready", Cutoff: "14:00"}},
		Validation: types.ValidationConfig{Trigger: "ALL", Rules: []types.ValidationRule{{Key: "upstream", Check: types.CheckExists}}},
		Job:        types.JobConfig{Type: types.TriggerHTTP, Config: map[string]interface{}{"url": "https://example.com"}},
	}
}

// at is hh:mm UTC on the cutoff date plus dayOffset days.
func at(dayOffset, hh, mm int) time.Time {
	return time.Date(2026, 3, 10+dayOffset, hh, mm, 0, 0, time.UTC)
}

// cutoffDeps is memDeps for cfg with the clock read from *now.
func cutoffDeps(t *testing.T, cfg types.PipelineConfig, now *time.Time) (*lambda.Deps, *storetest.Mem, *recorder) {
	t.Helper()
	d, mem, rec := memDeps(t, cfg)
	d.NowFunc = func() time.Time { return *now }
	return d, mem, rec
}

// failTablePut makes every PutItem on table fail with err.
func failTablePut(d *lambda.Deps, table string, err error) {
	fake := d.Store.Client.(*storetest.FakeDynamo)
	put := fake.PutItemFn
	fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if *in.TableName == table {
			return nil, err
		}
		return put(ctx, in)
	}
}

// failTableQuery makes every Query on table fail with err.
func failTableQuery(d *lambda.Deps, table string, err error) {
	fake := d.Store.Client.(*storetest.FakeDynamo)
	query := fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if *in.TableName == table {
			return nil, err
		}
		return query(ctx, in)
	}
}

// failRerunQuery makes every Query on the rerun table fail.
func failRerunQuery(d *lambda.Deps) {
	fake := d.Store.Client.(*storetest.FakeDynamo)
	query := fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if *in.TableName == "rerun" {
			return nil, errors.New("dynamodb: throttled")
		}
		return query(ctx, in)
	}
}

func eventList(rec *recorder) []string {
	var got []string
	for _, e := range rec.events {
		got = append(got, e.Type)
	}
	return got
}

// TestEvaluate_Cutoff: at or after the cutoff an evaluation that is not a
// rerun reports cutoff whether or not the rules pass, and writes nothing.
func TestEvaluate_Cutoff(t *testing.T) {
	passed := []string{string(types.EventValidationPassed)}
	type call struct {
		now        time.Time
		wantStatus string
		wantEvents []string // cumulative
	}
	tests := []struct {
		name      string
		tweak     func(*types.PipelineConfig)
		noSensor  bool
		sensorKey string // "" for upstream
		date      string // "" for cutoffDate
		rerun     bool
		failQuery bool
		calls     []call
	}{
		{name: "OneMinuteBeforeReady", calls: []call{{at(0, 13, 59), "passed", passed}}},
		{name: "BeforeCutoffNotReady", noSensor: true, calls: []call{{at(0, 13, 59), "not_ready", nil}}},
		{name: "PerPeriodHourlyKey", date: "2026-03-10T07", sensorKey: "upstream#20260310T07",
			calls: []call{{at(0, 13, 59), "passed", passed}}},
		{name: "PerPeriodDailyKey", date: "2026-03-10", sensorKey: "upstream#20260310",
			calls: []call{{at(0, 13, 59), "passed", passed}}},
		{name: "AtCutoffReady", calls: []call{{at(0, 14, 0), "cutoff", nil}}},
		{name: "AfterCutoffNotReady", noSensor: true, calls: []call{{at(0, 14, 30), "cutoff", nil}}},
		{name: "AfterCutoffRerun", rerun: true, calls: []call{{at(0, 14, 30), "passed", passed}}},
		{name: "NoCutoff", tweak: func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = "" },
			calls: []call{{at(0, 14, 30), "passed", passed}}},
		{name: "HourlyCutoffDailyDate", tweak: func(c *types.PipelineConfig) { c.Schedule.Trigger.Cutoff = ":15" },
			calls: []call{{at(0, 14, 30), "passed", passed}}},
		{name: "CutoffDayOffset", tweak: func(c *types.PipelineConfig) { c.Schedule.Trigger.CutoffDayOffset = 1 },
			calls: []call{{at(1, 13, 59), "passed", passed}, {at(1, 14, 0), "cutoff", passed}}},
		{name: "RerunLookupFails", failQuery: true, calls: []call{{at(0, 14, 30), "error", nil}}},
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
			if !tt.noSensor {
				if err := d.Store.WriteSensor(ctx, "p", cmp.Or(tt.sensorKey, "upstream"), map[string]interface{}{"status": "ok"}); err != nil {
					t.Fatalf("write sensor: %v", err)
				}
			}
			if tt.rerun {
				if _, err := d.Store.WriteRerun(ctx, "p", "s", cutoffDate, "manual", ""); err != nil {
					t.Fatalf("write rerun: %v", err)
				}
			}
			if tt.failQuery {
				failRerunQuery(d)
			}
			rec.ops = nil

			for i, c := range tt.calls {
				now = c.now
				out, err := orchestrator.HandleOrchestrator(ctx, d, lambda.OrchestratorInput{
					Mode: "evaluate", PipelineID: "p", ScheduleID: "s", Date: cmp.Or(tt.date, cutoffDate),
				})
				if err != nil {
					t.Fatalf("call %d: evaluate must not return a Lambda error, got %v", i, err)
				}
				if out.Mode != "evaluate" || out.Status != c.wantStatus {
					t.Errorf("call %d: output = %+v, want mode evaluate and status %q", i, out, c.wantStatus)
				}
				if c.wantStatus == "error" && !strings.Contains(out.Error, "dynamodb: throttled") {
					t.Errorf("call %d: error = %q, want the rerun lookup error", i, out.Error)
				}
				if got := eventList(rec); !slices.Equal(got, c.wantEvents) {
					t.Errorf("call %d: events = %v, want %v", i, got, c.wantEvents)
				}
			}
			for _, op := range rec.ops {
				if strings.HasPrefix(op, "status ") {
					t.Errorf("ops = %v, want no trigger status write", rec.ops)
				}
			}
			for _, item := range mem.Items("control") {
				if strings.HasPrefix(strAttr(t, item, "SK"), "TRIGGER#") {
					t.Errorf("control item %v, want no trigger row", item)
				}
			}
		})
	}
}
