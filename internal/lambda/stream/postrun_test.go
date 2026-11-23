package stream

import (
	"cmp"
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// postRunRecord is the stream record of the "row-count" post-run sensor for
// 2026-03-10 with count in the given field.
func postRunRecord(field, count string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "1",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute(types.SensorSK("row-count")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
					"date": events.NewStringAttribute("2026-03-10"),
					field:  events.NewNumberAttribute(count),
				}),
			},
		},
	}
}

// TestPostRunSensor_WatchWindow: drift and the post-run verdicts for a
// COMPLETED date are published only inside its post-run watch window (6h from
// completedAt here). A RUNNING date's inflight drift is not bounded by it, and
// a RUNNING date with no drift or no baseline publishes nothing. The baseline
// count is 100 unless a row has none, in DriftField "count" unless a row
// says otherwise. Calendar exclusion is checked on the sensor's date, not the
// wall-clock date. A baseline entry is left as it was, and no baseline row is
// written outside the window or for a RUNNING date.
func TestPostRunSensor_WatchWindow(t *testing.T) {
	const date = "2026-03-10"
	drift := []string{string(types.EventPostRunDrift)}
	tests := []struct {
		name        string
		status      string // trigger row; empty means COMPLETED
		completedAt string
		now         string
		count       string
		noBaseline  bool
		noRow       bool   // no trigger row
		field       string // the drift field; empty means count
		tweak       func(*types.PipelineConfig)
		wantEvents  []string
		wantRerun   bool
	}{
		{name: "inside, drift (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z", count: "120",
			wantEvents: drift, wantRerun: true},
		{name: "at the limit", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T18:00:00Z", count: "120"},
		{name: "inside, no drift (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:00:00Z", count: "100",
			wantEvents: []string{string(types.EventPostRunPassed)}},
		{name: "after, no drift", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T19:00:00Z", count: "100"},
		{name: "after, no baseline", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T19:00:00Z", count: "100", noBaseline: true},
		{name: "no completedAt", now: "2026-03-10T13:00:00Z", count: "120"},
		{name: "RUNNING row (inflight, guard)", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "120",
			wantEvents: []string{string(types.EventPostRunDriftInflight)}},
		{name: "RUNNING, no drift", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "100"},
		{name: "RUNNING, no baseline", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "500", noBaseline: true},
		{name: "FAILED_FINAL", status: types.TriggerStatusFailedFinal, completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:00:00Z", count: "120"},
		{name: "no trigger row", noRow: true, now: "2026-03-10T13:00:00Z", count: "120"},
		{name: "drift, excluded date", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:00:00Z", count: "120",
			tweak:      func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}} },
			wantEvents: append(drift, string(types.EventPipelineExcluded))},
		{name: "drift, default field", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:00:00Z", count: "120",
			field: "sensor_count", tweak: func(c *types.PipelineConfig) { c.PostRun.DriftField = "" }, wantEvents: drift, wantRerun: true},
		{name: "drift, wall clock excluded (guard)", completedAt: "2026-03-11T10:00:00Z", now: "2026-03-11T11:00:00Z", count: "120",
			tweak: func(c *types.PipelineConfig) {
				c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-11"}}
			},
			wantEvents: drift, wantRerun: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threshold, field := 0.0, cmp.Or(tt.field, "count")
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
				PostRun: &types.PostRunConfig{
					Rules:      []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}},
					DriftField: "count", DriftThreshold: &threshold, WatchWindow: "6h",
				},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			row := completedRow(date, tt.completedAt)
			if tt.status != "" {
				row["status"] = &ddbtypes.AttributeValueMemberS{Value: tt.status}
			}
			if !tt.noRow {
				mem.Put("control", row)
			}
			ctx := context.Background()
			if !tt.noBaseline {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{
					"row-count": map[string]interface{}{field: 100},
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"date": date}); err != nil {
				t.Fatal(err)
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }

			if err := handleRecord(ctx, d, postRunRecord(field, tt.count)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != 0 || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want 0 and %v", sfnFake.calls, gotEvents, tt.wantEvents)
			}
			for _, e := range eb.events {
				if e.Type == string(types.EventPostRunDrift) && e.Detail["driftField"] != field {
					t.Errorf("POST_RUN_DRIFT driftField = %v, want %s", e.Detail["driftField"], field)
				}
			}
			req := mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", date))
			if (req != nil) != tt.wantRerun {
				t.Fatalf("rerun request written = %v, want %v", req != nil, tt.wantRerun)
			}
			if req != nil {
				if got := req["reason"].(*ddbtypes.AttributeValueMemberS).Value; got != "data-drift" {
					t.Errorf("rerun request reason = %q, want data-drift", got)
				}
			}
			var wantBaseline map[string]interface{}
			if !tt.noBaseline {
				wantBaseline = map[string]interface{}{"row-count": map[string]interface{}{field: 100.0}}
			}
			if got := baselineEntries(t, d, date); !reflect.DeepEqual(got, wantBaseline) {
				t.Errorf("baseline = %v, want %v", got, wantBaseline)
			}
		})
	}
}

// seededField marks a baseline entry written after the run completed.
const seededField = "_seededAfterCompletion"

// firstWriteWorld is pipeline p with post-run rules on ruleKeys (DriftField
// count, threshold 0, watch window 6h), its 2026-03-10 run COMPLETED at 12:00,
// no baseline row and the clock at 13:00.
func firstWriteWorld(t *testing.T, ruleKeys ...string) (*lambda.Deps, *storetest.Mem, *recordingEventBridge) {
	t.Helper()
	threshold := 0.0
	cfg := types.PipelineConfig{
		Pipeline: types.PipelineIdentity{ID: "p"},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
		Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
		PostRun:  &types.PostRunConfig{DriftField: "count", DriftThreshold: &threshold, WatchWindow: "6h"},
	}
	for _, k := range ruleKeys {
		cfg.PostRun.Rules = append(cfg.PostRun.Rules, types.ValidationRule{Key: k, Check: types.CheckExists})
	}
	d, mem, _, eb := jobWorld(t, cfg)
	mem.Put("control", completedRow("2026-03-10", "2026-03-10T12:00:00Z"))
	now := mustTime(t, "2026-03-10T13:00:00Z")
	d.NowFunc = func() time.Time { return now }
	return d, mem, eb
}

// baselineEntries is pipeline p's postrun-baseline row for date, nil when
// there is none.
func baselineEntries(t *testing.T, d *lambda.Deps, date string) map[string]interface{} {
	t.Helper()
	baseline, err := d.Store.GetSensorData(context.Background(), "p", "postrun-baseline#"+date)
	if err != nil {
		t.Fatal(err)
	}
	return baseline
}

// TestPostRunSensor_FirstWriteAfterCompletion: a post-run sensor first written
// after the run completed (no baseline row) gets the post-run verdict and
// becomes the baseline entry, marked as seeded. A redelivery of that record
// is no drift. A later change to it is drift against that entry, which it does
// not replace.
func TestPostRunSensor_FirstWriteAfterCompletion(t *testing.T) {
	const date = "2026-03-10"
	d, mem, eb := firstWriteWorld(t, "row-count")
	ctx := context.Background()
	if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"date": date, "count": 5000}); err != nil {
		t.Fatal(err)
	}
	wantEntry := map[string]interface{}{"date": date, "count": 5000.0, seededField: true}
	rerun := func() map[string]ddbtypes.AttributeValue {
		return mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", date))
	}

	passed, drift := string(types.EventPostRunPassed), string(types.EventPostRunDrift)
	steps := []struct {
		name, count string
		wantEvents  []string
		wantRerun   bool
	}{
		{name: "first write", count: "5000", wantEvents: []string{passed}},
		{name: "redelivery", count: "5000", wantEvents: []string{passed, passed}},
		{name: "changed", count: "9000", wantEvents: []string{passed, passed, drift}, wantRerun: true},
	}
	for _, st := range steps {
		if err := handleRecord(ctx, d, postRunRecord("count", st.count)); err != nil {
			t.Fatalf("%s: %v", st.name, err)
		}
		if got := eventTypes(eb.events); !slices.Equal(got, st.wantEvents) || (rerun() != nil) != st.wantRerun {
			t.Fatalf("%s: events %v, rerun request %v; want %v and a request %v", st.name, got, rerun(), st.wantEvents, st.wantRerun)
		}
		if got := baselineEntries(t, d, date)["row-count"]; !reflect.DeepEqual(got, wantEntry) {
			t.Fatalf("%s: baseline entry = %v, want %v", st.name, got, wantEntry)
		}
	}
	if detail := eb.events[2].Detail; detail["previousCount"] != 5000.0 || detail["currentCount"] != 9000.0 {
		t.Errorf("drift detail = %v, want previousCount 5000 and currentCount 9000", detail)
	}
	if req := rerun(); strAttr(req["reason"]) != "data-drift" {
		t.Errorf("rerun request = %v, want reason data-drift", req)
	}
}

// TestPostRunSensor_FirstWriteKeepsOtherEntries: seeding one rule's entry
// leaves the entry another rule's sensor got at completion as it was.
func TestPostRunSensor_FirstWriteKeepsOtherEntries(t *testing.T) {
	const date = "2026-03-10"
	d, _, _ := firstWriteWorld(t, "freshness", "row-count")
	ctx := context.Background()
	captured := map[string]interface{}{"count": 1.0}
	if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{"freshness": captured}); err != nil {
		t.Fatal(err)
	}

	if err := handleRecord(ctx, d, postRunRecord("count", "5000")); err != nil {
		t.Fatalf("handleRecord: %v", err)
	}

	want := map[string]interface{}{
		"freshness": captured,
		"row-count": map[string]interface{}{"date": date, "count": 5000.0, seededField: true},
	}
	if got := baselineEntries(t, d, date); !reflect.DeepEqual(got, want) {
		t.Errorf("baseline = %v, want %v", got, want)
	}
}

// TestPostRunSensor_SeedWriteFails: when the seeded baseline cannot be
// written the record fails, so it is redelivered, and nothing is published.
func TestPostRunSensor_SeedWriteFails(t *testing.T) {
	d, _, eb := firstWriteWorld(t, "row-count")
	fake := d.Store.Client.(*storetest.FakeDynamo)
	put := fake.PutItemFn
	fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if sk, ok := in.Item["SK"].(*ddbtypes.AttributeValueMemberS); ok && sk.Value == types.SensorSK("postrun-baseline#2026-03-10") {
			return nil, errors.New("put baseline failed")
		}
		return put(ctx, in)
	}

	if err := handleRecord(context.Background(), d, postRunRecord("count", "5000")); err == nil {
		t.Fatal("handleRecord = nil, want the baseline write error")
	}
	if len(eb.events) != 0 {
		t.Errorf("events = %+v, want none", eb.events)
	}
}

// TestPostRunSensor_Seed: only a write of a key the baseline capture stores
// under the rule's key (the key itself or <key>#<date>, dashed or compact)
// that carries the drift field seeds the baseline, and drift is reported
// against what it seeded. An hourly key under a daily rule, or a longer key
// the rule only prefixes, seeds nothing. A write without the drift field
// ("" here) seeds nothing either, so the next write that has it is the
// baseline.
func TestPostRunSensor_Seed(t *testing.T) {
	tests := []struct {
		name, rule, key string // empty rule or key means row-count
		counts          []string
		wantSeed        bool
		wantDrift       []float64 // previous and current count of the one POST_RUN_DRIFT; nil means none
	}{
		{name: "own key", counts: []string{"5000", "9000"}, wantSeed: true, wantDrift: []float64{5000, 9000}},
		{name: "dashed date", key: "row-count#2026-03-10", counts: []string{"5000", "9000"}, wantSeed: true, wantDrift: []float64{5000, 9000}},
		{name: "compact date", key: "row-count#20260310", counts: []string{"5000", "9000"}, wantSeed: true, wantDrift: []float64{5000, 9000}},
		{name: "hourly key, daily rule", rule: "audit", key: "audit#2026-03-10T01", counts: []string{"5000", "9000"}},
		{name: "longer key", key: "row-count-eu", counts: []string{"5000", "9000"}},
		{name: "no drift field first", counts: []string{"", "5000", "9000"}, wantSeed: true, wantDrift: []float64{5000, 9000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := cmp.Or(tt.rule, "row-count")
			d, _, eb := firstWriteWorld(t, rule)
			for _, count := range tt.counts {
				data := map[string]events.DynamoDBAttributeValue{"date": events.NewStringAttribute("2026-03-10")}
				if count == "" {
					data["status"] = events.NewStringAttribute("RUNNING")
				} else {
					data["count"] = events.NewNumberAttribute(count)
				}
				if err := handleRecord(context.Background(), d, sensorRecordWith(cmp.Or(tt.key, "row-count"), data)); err != nil {
					t.Fatalf("handleRecord(%q): %v", count, err)
				}
			}
			var drift []float64
			for _, e := range eb.events {
				if e.Type == string(types.EventPostRunDrift) {
					drift = append(drift, e.Detail["previousCount"].(float64), e.Detail["currentCount"].(float64))
				}
			}
			_, seeded := baselineEntries(t, d, "2026-03-10")[rule]
			if seeded != tt.wantSeed || !slices.Equal(drift, tt.wantDrift) {
				t.Errorf("seeded %v, drift %v; want %v and %v", seeded, drift, tt.wantSeed, tt.wantDrift)
			}
		})
	}
}

// TestPostRunSensor_SeedLosesRace: when another writer stores the rule's entry
// between the handler's read and its seed (with no row read, or a row without
// the entry), the seed changes nothing and the sensor is compared with that
// entry: one POST_RUN_DRIFT from 100 to 5000.
func TestPostRunSensor_SeedLosesRace(t *testing.T) {
	const date = "2026-03-10"
	theirs := map[string]interface{}{"count": 100.0}
	captured := map[string]interface{}{"count": 1.0}
	tests := []struct {
		name    string
		atRead  map[string]interface{} // the baseline row the handler reads; nil means none
		atWrite map[string]interface{} // the row another writer leaves before the seed
	}{
		{name: "no row read", atWrite: map[string]interface{}{"row-count": theirs}},
		{name: "row without the entry", atRead: map[string]interface{}{"freshness": captured},
			atWrite: map[string]interface{}{"freshness": captured, "row-count": theirs}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, _, eb := firstWriteWorld(t, "freshness", "row-count")
			ctx := context.Background()
			if tt.atRead != nil {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, tt.atRead); err != nil {
					t.Fatal(err)
				}
			}
			// The other writer's row lands just before the seed's conditional write.
			fake := d.Store.Client.(*storetest.FakeDynamo)
			put, update := fake.PutItemFn, fake.UpdateItemFn
			race := func() {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, tt.atWrite); err != nil {
					t.Fatal(err)
				}
			}
			fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
				if in.ConditionExpression != nil && strAttr(in.Item["SK"]) == types.SensorSK("postrun-baseline#"+date) {
					race()
				}
				return put(ctx, in)
			}
			fake.UpdateItemFn = func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				race()
				return update(ctx, in)
			}

			if err := handleRecord(ctx, d, postRunRecord("count", "5000")); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if !slices.Equal(eventTypes(eb.events), []string{string(types.EventPostRunDrift)}) ||
				eb.events[0].Detail["previousCount"] != 100.0 || eb.events[0].Detail["currentCount"] != 5000.0 {
				t.Errorf("events = %+v, want one POST_RUN_DRIFT from 100 to 5000", eb.events)
			}
			if b := baselineEntries(t, d, date); !reflect.DeepEqual(b, tt.atWrite) {
				t.Errorf("baseline = %v, want the other writer's %v", b, tt.atWrite)
			}
		})
	}
}

// TestPostRunSensor_RuleKeyBoundary: a post-run sensor is compared with its
// own rule's baseline entry, not with an earlier rule whose key it merely
// begins with. Rules orders and orders-count, in that order, have baseline
// entries of 100 and 500; an unchanged orders-count write of 500 is no drift
// on a COMPLETED date inside the window and no inflight drift on a RUNNING one.
func TestPostRunSensor_RuleKeyBoundary(t *testing.T) {
	const date = "2026-03-10"
	tests := []struct {
		name       string
		status     string
		wantEvents []string
	}{
		{name: "COMPLETED", status: types.TriggerStatusCompleted, wantEvents: []string{string(types.EventPostRunPassed)}},
		{name: "RUNNING", status: types.TriggerStatusRunning},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, eb := firstWriteWorld(t, "orders", "orders-count")
			row := completedRow(date, "2026-03-10T12:00:00Z")
			row["status"] = &ddbtypes.AttributeValueMemberS{Value: tt.status}
			mem.Put("control", row)
			ctx := context.Background()
			for key, data := range map[string]map[string]interface{}{
				"postrun-baseline#" + date: {
					"orders":       map[string]interface{}{"count": 100},
					"orders-count": map[string]interface{}{"count": 500},
				},
				"orders":       {"date": date},
				"orders-count": {"date": date, "count": 500},
			} {
				if err := d.Store.WriteSensor(ctx, "p", key, data); err != nil {
					t.Fatal(err)
				}
			}
			data := map[string]events.DynamoDBAttributeValue{
				"date": events.NewStringAttribute(date), "count": events.NewNumberAttribute("500")}

			if err := handleRecord(ctx, d, sensorRecordWith("orders-count", data)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if got := eventTypes(eb.events); !slices.Equal(got, tt.wantEvents) {
				t.Errorf("events %v, want %v", got, tt.wantEvents)
			}
			if req := mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", date)); req != nil {
				t.Errorf("rerun request written: %v", req)
			}
		})
	}
}

// eventTypes lists the detail-type of each published event, in order.
func eventTypes(published []publishedEvent) []string {
	var got []string
	for _, e := range published {
		got = append(got, e.Type)
	}
	return got
}
