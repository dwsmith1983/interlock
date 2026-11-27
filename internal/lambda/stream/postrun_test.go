package stream

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
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
// completedAt here). After it, drift from the baseline entry of a date whose
// run succeeded publishes only MANUAL_RERUN_REQUIRED (data-drift, blocked by
// the window), and no drift or no baseline publishes nothing. A RUNNING
// date publishes nothing, drift or not: the baseline is captured when the
// run completes, and only later writes are compared. The baseline count is
// 100 unless a row has none, in DriftField "count" unless a row says
// otherwise. Calendar exclusion is checked on the sensor's date, not the
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
		jobEvent    string // the run's job row; empty means success
		tweak       func(*types.PipelineConfig)
		wantEvents  []string
		wantRerun   bool
	}{
		{name: "inside, drift (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z", count: "120",
			wantEvents: drift, wantRerun: true},
		{name: "at the limit", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T18:00:00Z", count: "120",
			wantEvents: []string{evManual}},
		{name: "after, drift, job failed", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T19:00:00Z", count: "120",
			jobEvent: types.JobEventFail},
		{name: "inside, no drift (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:00:00Z", count: "100",
			wantEvents: []string{string(types.EventPostRunPassed)}},
		{name: "after, no drift", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T19:00:00Z", count: "100"},
		{name: "after, no baseline", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T19:00:00Z", count: "120", noBaseline: true},
		{name: "no completedAt", now: "2026-03-10T13:00:00Z", count: "120"},
		{name: "RUNNING, drift", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "120"},
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
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
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
			mem.Put("joblog", jobRow(date, cmp.Or(tt.jobEvent, types.JobEventSuccess)))
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
				if e.Type == evManual {
					assertDriftDetail(t, e.Detail, "postRun.watchWindow", field, false)
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

// jobRow is pipeline p's job row for date's run, with event.
func jobRow(date, event string) map[string]ddbtypes.AttributeValue {
	return map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
		"event": &ddbtypes.AttributeValueMemberS{Value: event}}
}

// assertDriftDetail checks a MANUAL_RERUN_REQUIRED detail for a row-count
// write of 120 against a baseline of 100 in field (threshold 0), stopped by
// blockedBy. With omitted the baseline must be left out and flagged.
func assertDriftDetail(t *testing.T, got map[string]interface{}, blockedBy, field string, omitted bool) {
	t.Helper()
	want := map[string]interface{}{"reason": "data-drift", "source": "sensor", "blockedBy": blockedBy, "sensorKey": "row-count",
		"driftField": field, "previousCount": 100.0, "currentCount": 120.0, "delta": 20.0, "driftThreshold": 0.0}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("detail %s = %v, want %v", k, got[k], v)
		}
	}
	if _, ok := got["baseline"]; omitted && (ok || got["baselineOmitted"] != true) {
		t.Errorf("detail baseline present %v, baselineOmitted %v; want absent and true", ok, got["baselineOmitted"])
	}
	if b := map[string]interface{}{field: 100.0}; !omitted && !reflect.DeepEqual(got["baseline"], b) {
		t.Errorf("detail baseline = %v, want %v", got["baseline"], b)
	}
}

// TestPostRunSensor_MaxAutoRerunAge: with a 200h watch window and
// job.maxAutoRerunAge 24h, drift 25h after completedAt publishes
// MANUAL_RERUN_REQUIRED with the drift and its baseline, not POST_RUN_DRIFT,
// and writes no data-drift request, so a pending manual request is left as it
// was; a second drifted write publishes MANUAL_RERUN_STILL_REQUIRED. A failed
// run or an excluded date publishes nothing, and a failed job log read
// returns the error for the stream to retry. A baseline entry over 32 KB is
// left out of the event and flagged. Inside the age POST_RUN_DRIFT is
// published and the request is written.
func TestPostRunSensor_MaxAutoRerunAge(t *testing.T) {
	const date = "2026-03-10"
	pk := types.PipelinePK("p")
	manual := map[string]ddbtypes.AttributeValue{
		"PK":          &ddbtypes.AttributeValueMemberS{Value: pk},
		"SK":          &ddbtypes.AttributeValueMemberS{Value: types.RerunRequestSK("stream", date)},
		"reason":      &ddbtypes.AttributeValueMemberS{Value: "manual"},
		"requestedAt": &ddbtypes.AttributeValueMemberS{Value: "2026-03-11T12:30:00Z"},
	}
	const past = "2026-03-11T13:00:00Z"
	tests := []struct {
		name       string
		now        string
		manualRow  bool
		excluded   bool
		jobEvent   string // the run's job row; empty means success
		writes     int    // drifted writes; 0 means 1
		bigEntry   bool   // the baseline entry is over 32 KB
		failJobLog bool   // the job log read fails
		wantEvents []string
		wantReason string // the request row's reason; empty means none
	}{
		{name: "past the age", now: past, wantEvents: []string{evManual}},
		{name: "past the age, second write", now: past, writes: 2, wantEvents: []string{evManual, evStill}},
		{name: "past the age, manual request pending", now: past, manualRow: true, wantEvents: []string{evManual}, wantReason: "manual"},
		{name: "past the age, excluded date", now: past, excluded: true},
		{name: "past the age, job failed", now: past, jobEvent: types.JobEventFail},
		{name: "past the age, large baseline", now: past, bigEntry: true, wantEvents: []string{evManual}},
		{name: "past the age, job log read fails", now: past, failJobLog: true},
		{name: "inside the age (guard)", now: "2026-03-11T11:59:59Z", wantEvents: []string{string(types.EventPostRunDrift)}, wantReason: "data-drift"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threshold := 0.0
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig, MaxAutoRerunAge: "24h"},
				PostRun: &types.PostRunConfig{
					Rules:      []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}},
					DriftField: "count", DriftThreshold: &threshold, WatchWindow: "200h",
				},
			}
			if tt.excluded {
				cfg.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}}
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			mem.Put("control", completedRow(date, "2026-03-10T12:00:00Z"))
			mem.Put("joblog", jobRow(date, cmp.Or(tt.jobEvent, types.JobEventSuccess)))
			if tt.manualRow {
				mem.Put("control", maps.Clone(manual))
			}
			ctx := context.Background()
			entry := map[string]interface{}{"count": 100}
			if tt.bigEntry {
				entry["blob"] = strings.Repeat("x", 32*1024)
			}
			if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{"row-count": entry}); err != nil {
				t.Fatal(err)
			}
			if tt.failJobLog {
				fake := d.Store.Client.(*storetest.FakeDynamo)
				query := fake.QueryFn
				fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
					if *in.TableName == "joblog" {
						return nil, errors.New("query failed")
					}
					return query(ctx, in)
				}
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }
			d.Store.Now = func() time.Time { return now }

			for range max(tt.writes, 1) {
				if err := handleRecord(ctx, d, postRunRecord("count", "120")); (err != nil) != tt.failJobLog {
					t.Fatalf("handleRecord = %v, want an error: %v", err, tt.failJobLog)
				}
			}

			if got := publishedTypes(eb); sfnFake.calls != 0 || !slices.Equal(got, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want 0 and %v", sfnFake.calls, got, tt.wantEvents)
			}
			for _, e := range eb.events {
				if e.Type == evManual || e.Type == evStill {
					assertDriftDetail(t, e.Detail, "job.maxAutoRerunAge", "count", tt.bigEntry)
					if e.Detail["arrivedAt"] != past {
						t.Errorf("detail arrivedAt = %v, want %s", e.Detail["arrivedAt"], past)
					}
				}
			}
			req := mem.Get("control", pk, types.RerunRequestSK("stream", date))
			if got := strAttr(req["reason"]); got != tt.wantReason {
				t.Errorf("rerun request reason = %q, want %q", got, tt.wantReason)
			}
			if tt.manualRow && !reflect.DeepEqual(req, manual) {
				t.Errorf("manual request = %v, want it unchanged: %v", req, manual)
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
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}},
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
	d.Store.Now = func() time.Time { return now }
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
	// Both rows Interlock wrote expire counted from the clock at the write.
	wantTTL := strconv.FormatInt(d.NowFunc().Add(validation.MaxPostRunWatchWindow+24*time.Hour).Unix(), 10)
	rows := map[string]map[string]ddbtypes.AttributeValue{
		"seeded baseline": mem.Get("control", types.PipelinePK("p"), types.SensorSK("postrun-baseline#"+date)),
		"rerun request":   rerun(),
	}
	for name, row := range rows {
		if ttl, ok := row["ttl"].(*ddbtypes.AttributeValueMemberN); !ok || ttl.Value != wantTTL {
			t.Errorf("%s ttl = %v, want %s", name, row["ttl"], wantTTL)
		}
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
// on a COMPLETED date inside the window.
func TestPostRunSensor_RuleKeyBoundary(t *testing.T) {
	const date = "2026-03-10"
	tests := []struct {
		name       string
		status     string
		wantEvents []string
	}{
		{name: "COMPLETED", status: types.TriggerStatusCompleted, wantEvents: []string{string(types.EventPostRunPassed)}},
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
