package watchdog

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/stream"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestDetectMissingPostRunSensors: every trigger row on the pipeline's
// schedule from runKeysDaysBack days before now's UTC date through
// runKeysDaysAhead after it is read. A COMPLETED date whose completedAt plus
// postRun.sensorTimeout (2h by default) passed at most postRunMissingLookback
// ago, with no postrun-check marker, gets one POST_RUN_SENSOR_MISSING for its
// key, with completedAt in the detail, and the alerted marker. Each case runs
// two passes; the second adds nothing. The clock is 14:30 on slaTestDate
// unless a case says otherwise.
func TestDetectMissingPostRunSensors(t *testing.T) {
	type row struct{ key, status, completedAt, schedule string }
	at := func(day, hour, minute int) time.Time { return time.Date(2026, 3, day, hour, minute, 0, 0, time.UTC) }
	const noon, yesterdayNoon = "2026-03-10T12:00:00Z", "2026-03-09T12:00:00Z"
	done := func(key, completedAt string) []row { return []row{{key: key, completedAt: completedAt}} }
	excludeToday := func(c *types.PipelineConfig) {
		c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{slaTestDate}}
	}
	marker := func(field string) map[string]map[string]interface{} {
		return map[string]map[string]interface{}{slaTestDate: {field: "true"}}
	}
	tests := []struct {
		name     string
		rows     []row
		now      time.Time // zero means 14:30 on slaTestDate
		timeout  string    // postRun.sensorTimeout
		markers  map[string]map[string]interface{}
		tweak    func(*types.PipelineConfig)
		want     []string // the dates alerted, in key order
		wantWarn bool
	}{
		{name: "past the timeout (guard)", rows: done(slaTestDate, noon), want: []string{slaTestDate}},
		{name: "within the timeout", rows: done(slaTestDate, noon), now: at(10, 13, 30)},
		{name: "at the deadline", rows: done(slaTestDate, noon), now: at(10, 14, 0), want: []string{slaTestDate}},
		{name: "timeout 1h", rows: done(slaTestDate, noon), now: at(10, 13, 30), timeout: "1h", want: []string{slaTestDate}},
		{name: "T+1 daily key", rows: done("2026-03-09", noon), want: []string{"2026-03-09"}},
		{name: "hourly key", rows: done("2026-03-10T11", noon), want: []string{"2026-03-10T11"}},
		{name: "timeout ends after UTC midnight", rows: done("2026-03-09", "2026-03-09T23:00:00Z"), now: at(10, 1, 30), want: []string{"2026-03-09"}},
		{name: "oldest date read", rows: done("2026-03-02", noon), want: []string{"2026-03-02"}},
		{name: "a day older is not read", rows: done("2026-03-01", noon)},
		{name: "hourly key on the oldest date read", rows: done("2026-03-02T00", noon), want: []string{"2026-03-02T00"}},
		{name: "last hour 2 days ahead read", rows: done("2026-03-12T23", noon), want: []string{"2026-03-12T23"}},
		{name: "3 days ahead is not read", rows: done("2026-03-13", noon)},
		{name: "24h after the deadline", rows: done("2026-03-09", yesterdayNoon), now: at(10, 14, 0), want: []string{"2026-03-09"}},
		{name: "over 24h after the deadline", rows: done("2026-03-09", yesterdayNoon), now: at(10, 14, 1)},
		{name: "arrived marker", rows: done(slaTestDate, noon), markers: marker("arrived")},
		{name: "alerted marker", rows: done(slaTestDate, noon), markers: marker("alerted")},
		{name: "two due dates", rows: append(done("2026-03-09", noon), done(slaTestDate, noon)...), want: []string{"2026-03-09", slaTestDate}},
		{name: "RUNNING", rows: []row{{key: slaTestDate, status: types.TriggerStatusRunning, completedAt: noon}}},
		{name: "FAILED_FINAL", rows: []row{{key: slaTestDate, status: types.TriggerStatusFailedFinal, completedAt: noon}}},
		{name: "malformed key", rows: done("2026-03-10T24", noon), wantWarn: true},
		{name: "no completedAt", rows: done(slaTestDate, "")},
		{name: "completedAt does not parse", rows: done(slaTestDate, "yesterday"), wantWarn: true},
		{name: "excluded dates", rows: append(done("2026-03-10T11", noon), done(slaTestDate, noon)...), tweak: excludeToday},
		{name: "dry run", rows: done(slaTestDate, noon), tweak: dryRunHTTP},
		{name: "no post-run rules", rows: done(slaTestDate, noon), tweak: func(c *types.PipelineConfig) { c.PostRun.Rules = nil }},
		{name: "other schedule ID", rows: []row{{key: slaTestDate, completedAt: noon, schedule: "cron"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.SLA = nil
				c.PostRun = &types.PostRunConfig{SensorTimeout: tt.timeout, Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
				if tt.tweak != nil {
					tt.tweak(c)
				}
			})
			pk := types.PipelinePK("p")
			completedAt := map[string]string{}
			for _, r := range tt.rows {
				item := []string{"PK", pk, "SK", types.TriggerSK(cmp.Or(r.schedule, "stream"), r.key), "status", cmp.Or(r.status, types.TriggerStatusCompleted)}
				if r.completedAt != "" {
					item = append(item, "completedAt", r.completedAt)
				}
				mem.Put("control", sItem(item...))
				completedAt[r.key] = r.completedAt
			}
			ctx := context.Background()
			for date, m := range tt.markers {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-check#"+date, m); err != nil {
					t.Fatal(err)
				}
			}
			now := cmp.Or(tt.now, at(10, 14, 30))
			d.NowFunc = func() time.Time { return now }
			logs := &bytes.Buffer{}
			d.Logger = slog.New(slog.NewTextHandler(logs, nil))

			for pass := 0; pass < 2; pass++ {
				if err := detectMissingPostRunSensors(ctx, d); err != nil {
					t.Fatalf("pass %d: %v", pass, err)
				}
			}
			var dates []string
			for i, e := range rec.details {
				dates = append(dates, e.Date)
				if rec.events[i] != string(types.EventPostRunSensorMissing) || e.Detail["completedAt"] != completedAt[e.Date] {
					t.Errorf("event %d = %s with completedAt %v, want POST_RUN_SENSOR_MISSING with %q", i, rec.events[i], e.Detail["completedAt"], completedAt[e.Date])
				}
			}
			if !slices.Equal(dates, tt.want) {
				t.Errorf("event dates = %v, want %v", dates, tt.want)
			}
			for key := range completedAt {
				got, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+key)
				if err != nil {
					t.Fatal(err)
				}
				want := tt.markers[key]
				if slices.Contains(tt.want, key) {
					want = map[string]interface{}{"alerted": "true"}
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("postrun-check marker for %s = %v, want %v", key, got, want)
				}
			}
			if warned := strings.Contains(logs.String(), "level=WARN"); warned != tt.wantWarn {
				t.Errorf("warned = %v, want %v; logs: %s", warned, tt.wantWarn, logs)
			}
		})
	}
}

// TestDetectMissingPostRunSensors_PublishFailureRetries: no marker after a
// failed publish, so the next pass alerts, and the pass after adds nothing.
func TestDetectMissingPostRunSensors_PublishFailureRetries(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA = nil
		c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
	})
	mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("stream", slaTestDate),
		"status", types.TriggerStatusCompleted, "completedAt", "2026-03-10T12:00:00Z"))
	ctx := context.Background()
	d.NowFunc = func() time.Time { return time.Date(2026, 3, 10, 14, 30, 0, 0, time.UTC) }
	rec.failEvents = 1
	for pass, want := range []int{0, 1, 1} {
		if err := detectMissingPostRunSensors(ctx, d); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		marker, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+slaTestDate)
		if err != nil {
			t.Fatal(err)
		}
		if marked := marker != nil; len(rec.events) != want || marked != (want == 1) ||
			(want == 1 && rec.events[0] != string(types.EventPostRunSensorMissing)) {
			t.Fatalf("pass %d: events %v, marker %v; want %d POST_RUN_SENSOR_MISSING and marker %v", pass, rec.events, marked, want, want == 1)
		}
	}
}

// postRunDeps is slaDeps with a post-run rule on row-count, no SLA and the
// clock at 14:30 on slaTestDate, and a COMPLETED trigger row completed at
// 12:00 that day for each of the pipelines and dates given.
func postRunDeps(t *testing.T, pipelines, dates []string) (*lambda.Deps, *storetest.FakeDynamo, *slaRecorder, *bytes.Buffer) {
	t.Helper()
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA = nil
		c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
	})
	var cfg types.PipelineConfig
	if err := json.Unmarshal([]byte(mem.Get("control", types.PipelinePK("p"), types.ConfigSK)["config"].(*ddbtypes.AttributeValueMemberS).Value), &cfg); err != nil {
		t.Fatal(err)
	}
	for _, id := range pipelines {
		cfg.Pipeline.ID = id
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		mem.Put("control", sItem("PK", types.PipelinePK(id), "SK", types.ConfigSK, "config", string(data)))
		for _, date := range dates {
			mem.Put("control", sItem("PK", types.PipelinePK(id), "SK", types.TriggerSK("stream", date),
				"status", types.TriggerStatusCompleted, "completedAt", "2026-03-10T12:00:00Z"))
		}
	}
	d.NowFunc = func() time.Time { return time.Date(2026, 3, 10, 14, 30, 0, 0, time.UTC) }
	logs := &bytes.Buffer{}
	d.Logger = slog.New(slog.NewTextHandler(logs, nil))
	return d, d.Store.Client.(*storetest.FakeDynamo), rec, logs
}

// TestDetectMissingPostRunSensors_TriggerQueryFails: when the first
// pipeline's trigger query fails, the error is logged for it and the pass
// goes on: the other pipeline is alerted and the pass returns no error.
func TestDetectMissingPostRunSensors_TriggerQueryFails(t *testing.T) {
	d, fake, rec, logs := postRunDeps(t, []string{"p", "q"}, []string{slaTestDate})
	query, failed := fake.QueryFn, ""
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if from, ok := in.ExpressionAttributeValues[":from"].(*ddbtypes.AttributeValueMemberS); ok && failed == "" &&
			strings.HasPrefix(from.Value, types.TriggerSK("stream", "")) {
			failed = strings.TrimPrefix(in.ExpressionAttributeValues[":pk"].(*ddbtypes.AttributeValueMemberS).Value, types.PipelinePK(""))
			return nil, errors.New("query failed")
		}
		return query(ctx, in)
	}

	if err := detectMissingPostRunSensors(context.Background(), d); err != nil {
		t.Fatalf("detectMissingPostRunSensors: %v", err)
	}
	other := map[string]string{"p": "q", "q": "p"}[failed]
	if len(rec.details) != 1 || rec.details[0].PipelineID != other {
		t.Errorf("events %+v; want one POST_RUN_SENSOR_MISSING for %q (the query failed for %q)", rec.details, other, failed)
	}
	if out := logs.String(); !strings.Contains(out, "level=ERROR msg=\"trigger lookup failed in post-run sensor check\" pipelineId="+failed) {
		t.Errorf("want the trigger lookup error logged for %q; logs: %s", failed, out)
	}
}

// TestDetectMissingPostRunSensors_MarkerErrors: two due dates. The first's
// marker read fails: the error is logged and that date publishes nothing.
// The second publishes, and its marker write fails: a warning is logged and
// the alert is not logged as recorded.
func TestDetectMissingPostRunSensors_MarkerErrors(t *testing.T) {
	d, fake, rec, logs := postRunDeps(t, []string{"p"}, []string{"2026-03-09", slaTestDate})
	get, put := fake.GetItemFn, fake.PutItemFn
	fake.GetItemFn = func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
		if in.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value == types.SensorSK("postrun-check#2026-03-09") {
			return nil, errors.New("get failed")
		}
		return get(ctx, in)
	}
	fake.PutItemFn = func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
		if in.Item["SK"].(*ddbtypes.AttributeValueMemberS).Value == types.SensorSK("postrun-check#"+slaTestDate) {
			return nil, errors.New("put failed")
		}
		return put(ctx, in)
	}

	if err := detectMissingPostRunSensors(context.Background(), d); err != nil {
		t.Fatalf("detectMissingPostRunSensors: %v", err)
	}
	if len(rec.details) != 1 || rec.details[0].Date != slaTestDate {
		t.Errorf("events %+v; want one POST_RUN_SENSOR_MISSING for %s", rec.details, slaTestDate)
	}
	out := logs.String()
	for _, want := range []string{
		`level=ERROR msg="dedup marker lookup failed in post-run sensor check" pipelineId=p date=2026-03-09`,
		`level=WARN msg="failed to write post-run dedup marker"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("want %q logged; logs: %s", want, out)
		}
	}
	if strings.Contains(out, "detected missing post-run sensor") {
		t.Errorf("the alert whose marker write failed is logged as recorded; logs: %s", out)
	}
}

// TestDetectMissingPostRunSensors_DriftRerun: a post-run sensor written with
// drift before the first deadline leaves the stream-router's arrived marker
// and a data-drift rerun. The rerun's completion resets completedAt and
// recaptures the baseline with the drifted value, and no post-run write
// follows. A pass past the rerun's deadline publishes no
// POST_RUN_SENSOR_MISSING and leaves the marker as it was.
func TestDetectMissingPostRunSensors_DriftRerun(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA = nil
		c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
	})
	ctx := context.Background()
	now := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return now }
	complete := func() {
		t.Helper()
		mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("stream", slaTestDate),
			"status", types.TriggerStatusCompleted, "completedAt", now.Format(time.RFC3339)))
		if err := lambda.CapturePostRunBaseline(ctx, d, "p", "stream", slaTestDate); err != nil {
			t.Fatalf("CapturePostRunBaseline: %v", err)
		}
	}
	if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"date": slaTestDate, "sensor_count": 100}); err != nil {
		t.Fatal(err)
	}
	complete()

	now = time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"date": slaTestDate, "sensor_count": 5000}); err != nil {
		t.Fatal(err)
	}
	record := events.DynamoDBEventRecord{
		EventName: "MODIFY",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "1",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute(types.SensorSK("row-count")),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{
				"date":         events.NewStringAttribute(slaTestDate),
				"sensor_count": events.NewNumberAttribute("5000"),
			})},
		},
	}
	resp, err := stream.HandleStreamEvent(ctx, d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{record}})
	if err != nil || len(resp.BatchItemFailures) != 0 {
		t.Fatalf("HandleStreamEvent = %+v, %v; want no failures", resp, err)
	}
	if !slices.Contains(rec.events, string(types.EventPostRunDrift)) {
		t.Fatalf("events = %v, want POST_RUN_DRIFT", rec.events)
	}

	now = time.Date(2026, 3, 10, 13, 30, 0, 0, time.UTC)
	complete()
	rec.events = nil
	now = time.Date(2026, 3, 10, 15, 45, 0, 0, time.UTC)
	if err := detectMissingPostRunSensors(ctx, d); err != nil {
		t.Fatalf("detectMissingPostRunSensors: %v", err)
	}
	marker, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+slaTestDate)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.events) != 0 || !reflect.DeepEqual(marker, map[string]interface{}{"arrived": "true"}) {
		t.Errorf("events %v, postrun-check marker %v; want none and arrived", rec.events, marker)
	}
}
