package watchdog

import (
	"bytes"
	"cmp"
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/stream"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestDetectMissingPostRunSensors: POST_RUN_SENSOR_MISSING fires once
// postRun.sensorTimeout (2h by default) has passed since the trigger row's
// completedAt and no post-run sensor has arrived. A sensor has arrived when
// it is present now and its value differs from its entry in the baseline
// captured at completion, or the baseline has no entry for it. The job rows
// do not set the anchor. Completion 12:00 on slaTestDate unless a row says
// otherwise. An alert writes the postrun-check marker for the date.
func TestDetectMissingPostRunSensors(t *testing.T) {
	type sensors = map[string]map[string]interface{}
	at := func(hour, minute int) time.Time { return time.Date(2026, 3, 10, hour, minute, 0, 0, time.UTC) }
	ms := func(tm time.Time) string { return strconv.FormatInt(tm.UnixMilli(), 10) }
	missing := []string{string(types.EventPostRunSensorMissing)}
	rows := func(n int) map[string]interface{} {
		return map[string]interface{}{"count": n, "tags": []string{"a", "b"}}
	}
	same := sensors{"row-count": rows(100)}
	changed := sensors{"row-count": rows(120)}
	twoRules := []string{"row-count", "freshness"}
	fresh := map[string]interface{}{"lagMinutes": 5}
	tests := []struct {
		name         string
		completedAt  string
		jobAt        time.Time // a success job row at this time; zero means none
		atCompletion sensors   // sensors when the baseline is captured
		current      sensors   // sensors when the watchdog runs
		rules        []string  // post-run rule keys; nil means row-count
		now          time.Time
		marked       bool // the postrun-check marker is already there
		setSensor    bool // a row-count sensor with a string set, put raw and never changed
		noPostRun    bool
		dryRun       bool
		status       string // of the trigger row; "" means COMPLETED
		timeout      string // postRun.sensorTimeout
		wantEvents   []string
		wantMarker   bool
		wantWarn     bool // a warning about completedAt
	}{
		{name: "past the timeout (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), atCompletion: same, current: same, now: at(14, 30), wantEvents: missing, wantMarker: true},
		{name: "within the timeout (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), atCompletion: same, current: same, now: at(13, 30)},
		{name: "sensor changed after completion (guard)", completedAt: "2026-03-10T12:00:00Z", jobAt: at(12, 0), atCompletion: same, current: changed, now: at(14, 30)},
		{name: "completedAt set, no success job row", completedAt: "2026-03-10T12:00:00Z", atCompletion: same, current: same, now: at(14, 0), wantEvents: missing, wantMarker: true},
		{name: "anchor is completedAt, not the job row", completedAt: "2026-03-10T12:00:00Z", jobAt: at(8, 0), atCompletion: same, current: same, now: at(13, 0)},
		{name: "no completedAt", jobAt: at(12, 0), atCompletion: same, current: same, now: at(14, 30)},
		{name: "completedAt does not parse", completedAt: "yesterday", jobAt: at(12, 0), atCompletion: same, current: same, now: at(14, 30), wantWarn: true},
		{name: "already alerted", completedAt: "2026-03-10T12:00:00Z", atCompletion: same, current: same, marked: true, now: at(14, 30)},
		{name: "no sensor then or now, no baseline", completedAt: "2026-03-10T12:00:00Z", now: at(14, 30), wantEvents: missing, wantMarker: true},
		{name: "trigger RUNNING", completedAt: "2026-03-10T12:00:00Z", status: types.TriggerStatusRunning, atCompletion: same, current: same, now: at(14, 30)},
		{name: "timeout 1h", completedAt: "2026-03-10T12:00:00Z", timeout: "1h", atCompletion: same, current: same, now: at(13, 30), wantEvents: missing, wantMarker: true},
		{name: "no postRun", completedAt: "2026-03-10T12:00:00Z", noPostRun: true, atCompletion: same, current: same, now: at(14, 30)},
		{name: "dry run", completedAt: "2026-03-10T12:00:00Z", dryRun: true, atCompletion: same, current: same, now: at(14, 30)},
		{name: "per-period sensor unchanged", completedAt: "2026-03-10T12:00:00Z", atCompletion: sensors{"row-count#2026-03-10": rows(100)}, current: sensors{"row-count#2026-03-10": rows(100)}, now: at(14, 30), wantEvents: missing, wantMarker: true},
		{name: "only yesterday's per-period sensor now", completedAt: "2026-03-10T12:00:00Z", current: sensors{"row-count#2026-03-09": rows(100)}, now: at(14, 30), wantEvents: missing, wantMarker: true},
		{name: "only updatedAt differs", completedAt: "2026-03-10T12:00:00Z",
			atCompletion: sensors{"row-count": {"count": 100, "updatedAt": ms(at(11, 0))}},
			current:      sensors{"row-count": {"count": 100, "updatedAt": ms(at(13, 0))}}, now: at(14, 30)},
		{name: "a field added", completedAt: "2026-03-10T12:00:00Z", atCompletion: same,
			current: sensors{"row-count": {"count": 100, "tags": []string{"a", "b"}, "status": "final"}}, now: at(14, 30)},
		{name: "absent at completion, present now, no baseline", completedAt: "2026-03-10T12:00:00Z", current: same, now: at(14, 30)},
		{name: "absent at completion, baseline holds another rule", completedAt: "2026-03-10T12:00:00Z", rules: twoRules,
			atCompletion: sensors{"freshness": fresh}, current: sensors{"freshness": fresh, "row-count": rows(100)}, now: at(14, 30)},
		{name: "per-period sensor arrives after completion", completedAt: "2026-03-10T12:00:00Z", current: sensors{"row-count#2026-03-10": rows(100)}, now: at(14, 30)},
		{name: "compact per-period sensor arrives after completion", completedAt: "2026-03-10T12:00:00Z", current: sensors{"row-count#20260310": rows(100)}, now: at(14, 30)},
		{name: "two rules, one unchanged, one changed", completedAt: "2026-03-10T12:00:00Z", rules: twoRules,
			atCompletion: sensors{"row-count": rows(100), "freshness": fresh},
			current:      sensors{"row-count": rows(100), "freshness": {"lagMinutes": 7}}, now: at(14, 30)},
		{name: "string-set sensor unchanged", completedAt: "2026-03-10T12:00:00Z", setSensor: true, now: at(14, 30), wantEvents: missing, wantMarker: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ruleKeys := tt.rules
			if ruleKeys == nil {
				ruleKeys = []string{"row-count"}
			}
			d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
				c.SLA = nil
				c.PostRun = &types.PostRunConfig{SensorTimeout: tt.timeout}
				for _, k := range ruleKeys {
					c.PostRun.Rules = append(c.PostRun.Rules, types.ValidationRule{Key: k, Check: types.CheckExists})
				}
				if tt.noPostRun {
					c.PostRun = nil
				}
				if tt.dryRun {
					c.DryRun, c.Job.Type, c.Job.Config = true, types.TriggerHTTP, httpJobConfig
				}
			})
			pk := types.PipelinePK("p")
			trigger := []string{"PK", pk, "SK", types.TriggerSK("stream", slaTestDate), "status", cmp.Or(tt.status, types.TriggerStatusCompleted)}
			if tt.completedAt != "" {
				trigger = append(trigger, "completedAt", tt.completedAt)
			}
			mem.Put("control", sItem(trigger...))
			if !tt.jobAt.IsZero() {
				mem.Put("joblog", sItem("PK", pk, "SK", types.JobSK("stream", slaTestDate, ms(tt.jobAt)), "event", types.JobEventSuccess))
			}
			ctx := context.Background()
			if tt.setSensor {
				mem.Put("control", map[string]ddbtypes.AttributeValue{
					"PK": &ddbtypes.AttributeValueMemberS{Value: pk},
					"SK": &ddbtypes.AttributeValueMemberS{Value: types.SensorSK("row-count")},
					"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
						"tags": &ddbtypes.AttributeValueMemberSS{Value: []string{"a", "b"}},
					}},
				})
			}
			for k, v := range tt.atCompletion {
				if err := d.Store.WriteSensor(ctx, "p", k, v); err != nil {
					t.Fatal(err)
				}
			}
			if err := lambda.CapturePostRunBaseline(ctx, d, "p", "stream", slaTestDate); err != nil {
				t.Fatalf("CapturePostRunBaseline: %v", err)
			}
			rec.events = nil
			for k := range tt.atCompletion {
				if err := d.Store.DeleteSensor(ctx, "p", k); err != nil {
					t.Fatal(err)
				}
			}
			for k, v := range tt.current {
				if err := d.Store.WriteSensor(ctx, "p", k, v); err != nil {
					t.Fatal(err)
				}
			}
			if tt.marked {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-check#"+slaTestDate, map[string]interface{}{"alerted": "true"}); err != nil {
					t.Fatal(err)
				}
			}
			d.NowFunc = func() time.Time { return tt.now }
			logs := &bytes.Buffer{}
			d.Logger = slog.New(slog.NewTextHandler(logs, nil))

			if err := detectMissingPostRunSensors(ctx, d); err != nil {
				t.Fatalf("detectMissingPostRunSensors: %v", err)
			}
			if !slices.Equal(rec.events, tt.wantEvents) {
				t.Errorf("events = %v, want %v", rec.events, tt.wantEvents)
			}
			marker, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+slaTestDate)
			if err != nil {
				t.Fatal(err)
			}
			if wantMarker := tt.wantMarker || tt.marked; (marker != nil) != wantMarker || (marker != nil && marker["alerted"] != "true") {
				t.Errorf("postrun-check marker = %v, want present %v with alerted true", marker, wantMarker)
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
	if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+slaTestDate, map[string]interface{}{"row-count": map[string]interface{}{"count": 1}}); err != nil {
		t.Fatal(err)
	}
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

// TestPostRunSensorArrived_UncomparableSensor: a sensor that cannot be
// encoded again (an empty field name) counts as arrived, with a warning
// naming the pipeline and sensor.
func TestPostRunSensorArrived_UncomparableSensor(t *testing.T) {
	logs := &bytes.Buffer{}
	rules := []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}
	sensors := map[string]map[string]interface{}{"row-count": {"": "x"}}
	baseline := map[string]interface{}{"row-count": map[string]interface{}{"count": float64(100)}}

	if !postRunSensorArrived(slog.New(slog.NewTextHandler(logs, nil)), "p", rules, sensors, baseline) {
		t.Error("arrived = false, want true")
	}
	if out := logs.String(); !strings.Contains(out, "level=WARN") || !strings.Contains(out, "pipelineId=p") || !strings.Contains(out, "sensor=row-count") {
		t.Errorf("want a warning naming pipeline p and sensor row-count; logs: %s", out)
	}
}

// TestDetectMissingPostRunSensors_SeededBaseline: a post-run sensor first
// written after completion, with no baseline row, is seeded as the baseline
// entry by the stream-router. That entry still reads as arrived, so a pass
// past the sensor timeout publishes no POST_RUN_SENSOR_MISSING.
func TestDetectMissingPostRunSensors_SeededBaseline(t *testing.T) {
	d, mem, rec := slaDeps(t, "", "", func(c *types.PipelineConfig) {
		c.SLA = nil
		c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}}}
	})
	mem.Put("control", sItem("PK", types.PipelinePK("p"), "SK", types.TriggerSK("stream", slaTestDate),
		"status", types.TriggerStatusCompleted, "completedAt", "2026-03-10T12:00:00Z"))
	ctx := context.Background()
	now := time.Date(2026, 3, 10, 13, 0, 0, 0, time.UTC)
	d.NowFunc = func() time.Time { return now }
	if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"date": slaTestDate, "sensor_count": 5000}); err != nil {
		t.Fatal(err)
	}
	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
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
	baseline, err := d.Store.GetSensorData(ctx, "p", "postrun-baseline#"+slaTestDate)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := baseline["row-count"]; !ok {
		t.Fatalf("baseline = %v, want a seeded row-count entry", baseline)
	}
	rec.events = nil

	now = time.Date(2026, 3, 10, 14, 30, 0, 0, time.UTC)
	if err := detectMissingPostRunSensors(ctx, d); err != nil {
		t.Fatalf("detectMissingPostRunSensors: %v", err)
	}
	marker, err := d.Store.GetSensorData(ctx, "p", "postrun-check#"+slaTestDate)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.events) != 0 || marker != nil {
		t.Errorf("events %v, postrun-check marker %v; want none and none", rec.events, marker)
	}
}
