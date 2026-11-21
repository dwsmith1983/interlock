package stream

import (
	"cmp"
	"context"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestStreamDryRunRerunRequest: a dry-run pipeline's rerun request runs the
// live checks in order (calendar, budget, circuit breaker) and publishes the
// verdict, but never starts a run or writes a rerun or job row. The request
// has an empty reason, so it counts as manual. The "upstream" sensor was last
// updated at 00:00, before the job row (08:00) when there is one. Now is
// 2026-03-10T13:00:00Z unless a row says otherwise; calendar exclusion is
// checked on the request's date, not the wall-clock date.
func TestStreamDryRunRerunRequest(t *testing.T) {
	const date = "2026-03-10"
	wouldRerun, rejected := string(types.EventDryRunWouldRerun), string(types.EventDryRunRerunRejected)
	tests := []struct {
		name       string
		job        string // the last job row's event; empty means none
		now        string // empty means 2026-03-10T13:00:00Z
		tweak      func(*types.PipelineConfig)
		wantType   string
		wantDetail map[string]interface{}
	}{
		{name: "would rerun", job: types.JobEventFail, wantType: wouldRerun,
			wantDetail: map[string]interface{}{"reason": "manual", "circuitBreaker": "passed", "rerunCount": 0.0, "budget": 1.0}},
		{name: "no history", wantType: wouldRerun,
			wantDetail: map[string]interface{}{"reason": "manual", "circuitBreaker": "skipped (no job history)", "rerunCount": 0.0, "budget": 1.0}},
		{name: "excluded", job: types.JobEventFail, wantType: rejected,
			tweak:      func(c *types.PipelineConfig) { c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{date}} },
			wantDetail: map[string]interface{}{"reason": "excluded by calendar"}},
		{name: "wall clock excluded, request date not (guard)", job: types.JobEventFail, now: "2026-03-11T13:00:00Z", wantType: wouldRerun,
			tweak: func(c *types.PipelineConfig) {
				c.Schedule.Exclude = &types.ExclusionConfig{Dates: []string{"2026-03-11"}}
			},
			wantDetail: map[string]interface{}{"reason": "manual", "circuitBreaker": "passed", "rerunCount": 0.0, "budget": 1.0}},
		{name: "limit", job: types.JobEventFail, wantType: rejected,
			tweak:      func(c *types.PipelineConfig) { zero := 0; c.Job.MaxManualReruns = &zero },
			wantDetail: map[string]interface{}{"reason": "limit exceeded", "rerunCount": 0.0, "budget": 0.0}},
		{name: "circuit breaker", job: types.JobEventSuccess, wantType: rejected,
			wantDetail: map[string]interface{}{"reason": "circuit breaker", "circuitBreaker": "rejected"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				DryRun:   true,
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			var wantJobs []string
			if tt.job != "" {
				mem.Put("joblog", map[string]ddbtypes.AttributeValue{
					"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
					"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK("stream", date, "1773129600000")},
					"event": &ddbtypes.AttributeValueMemberS{Value: tt.job}})
				wantJobs = []string{tt.job}
			}
			ctx := context.Background()
			if err := d.Store.WriteSensor(ctx, "p", "upstream", map[string]interface{}{"status": "ready", "updatedAt": "1773100800000"}); err != nil {
				t.Fatal(err)
			}
			rerunCountQuery(d.Store.Client.(*storetest.FakeDynamo), mem)
			now := mustTime(t, cmp.Or(tt.now, "2026-03-10T13:00:00Z"))
			d.NowFunc = func() time.Time { return now }
			record := events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{
				SequenceNumber: "1",
				Keys: map[string]events.DynamoDBAttributeValue{
					"PK": events.NewStringAttribute(types.PipelinePK("p")),
					"SK": events.NewStringAttribute(types.RerunRequestSK("stream", date)),
				},
				NewImage: map[string]events.DynamoDBAttributeValue{"reason": events.NewStringAttribute("")},
			}}

			if err := handleRecord(ctx, d, record); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if len(eb.events) != 1 || eb.events[0].Type != tt.wantType || !maps.Equal(eb.events[0].Detail, tt.wantDetail) {
				t.Errorf("events = %+v, want one %s with detail %v", eb.events, tt.wantType, tt.wantDetail)
			}
			if sfnFake.calls != 0 || len(mem.Items("rerun")) != 0 || !slices.Equal(jobEvents(mem), wantJobs) {
				t.Errorf("StartExecution calls %d, rerun rows %d, job rows %v; want 0, 0 and %v",
					sfnFake.calls, len(mem.Items("rerun")), jobEvents(mem), wantJobs)
			}
		})
	}
}

// dryRunEvent is a published event's type and detail.
func dryRunEvent(typ types.EventDetailType, detail map[string]interface{}) publishedEvent {
	return publishedEvent{Type: string(typ), Detail: detail}
}

// sameEvents reports whether got has want's types and details, in order.
func sameEvents(got, want []publishedEvent) bool {
	return slices.EqualFunc(got, want, func(g, w publishedEvent) bool {
		return g.Type == w.Type && maps.Equal(g.Detail, w.Detail)
	})
}

// dryRunMarkerAt is the triggeredAt of pipeline p's DRY_RUN# marker for date,
// or "" when there is none.
func dryRunMarkerAt(mem *storetest.Mem, date string) string {
	row := mem.Get("control", types.PipelinePK("p"), types.DryRunSK("stream", date))
	if row == nil {
		return ""
	}
	return row["data"].(*ddbtypes.AttributeValueMemberM).Value["triggeredAt"].(*ddbtypes.AttributeValueMemberS).Value
}

// seedDryRunMarker writes pipeline p's DRY_RUN# marker for date.
func seedDryRunMarker(mem *storetest.Mem, date, triggeredAt string) {
	mem.Put("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK("p")},
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.DryRunSK("stream", date)},
		"data": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"triggeredAt": &ddbtypes.AttributeValueMemberS{Value: triggeredAt}}},
	})
}

// TestStreamDryRunTrigger: a ready trigger sensor on a dry-run pipeline
// publishes what the live path would do (trigger, SLA projection, baseline
// capture, late data) and records a DRY_RUN# marker, but never starts a run
// or writes a TRIGGER or job row. The "upstream" sensor is ready and the
// "audit-result" sensor holds sensor_count 500. Now is 2026-03-11T01:15:00Z
// unless a row says otherwise.
func TestStreamDryRunTrigger(t *testing.T) {
	const date, at = "2026-03-11", "2026-03-11T01:15:00Z"
	wouldTrigger := dryRunEvent(types.EventDryRunWouldTrigger, map[string]interface{}{"triggeredAt": at, "rulesEvaluated": 1.0})
	sla := func(deadline string, offset int) func(*types.PipelineConfig) {
		return func(c *types.PipelineConfig) {
			c.SLA = &types.SLAConfig{Deadline: deadline, DeadlineDayOffset: offset, ExpectedDuration: "30m"}
		}
	}
	projection := func(triggeredAt, estimated, deadline string, margin float64, status string) []publishedEvent {
		return []publishedEvent{
			dryRunEvent(types.EventDryRunWouldTrigger, map[string]interface{}{"triggeredAt": triggeredAt, "rulesEvaluated": 1.0}),
			dryRunEvent(types.EventDryRunSLAProjection, map[string]interface{}{"triggeredAt": triggeredAt,
				"estimatedCompletion": estimated, "expectedDuration": "30m", "deadline": deadline, "marginSeconds": margin, "status": status}),
			dryRunEvent(types.EventDryRunCompleted, map[string]interface{}{"triggeredAt": triggeredAt,
				"slaStatus": status, "estimatedCompletion": estimated, "deadline": deadline}),
		}
	}
	tests := []struct {
		name         string
		now          string // empty means 2026-03-11T01:15:00Z
		marker       string // a seeded DRY_RUN# marker's triggeredAt; empty means none
		lostRace     bool   // the marker read misses the seeded marker, as when another write lands first
		tweak        func(*types.PipelineConfig)
		wantEvents   []publishedEvent
		wantMarker   string // the marker's triggeredAt afterwards; empty means none
		wantBaseline map[string]interface{}
	}{
		{name: "would trigger", wantMarker: at, wantEvents: []publishedEvent{wouldTrigger,
			dryRunEvent(types.EventDryRunCompleted, map[string]interface{}{"triggeredAt": at, "slaStatus": "n/a"})}},
		{name: "late data", marker: at, now: "2026-03-11T01:30:00Z", wantMarker: at, wantEvents: []publishedEvent{
			dryRunEvent(types.EventDryRunLateData, map[string]interface{}{"triggeredAt": at, "lateBy": "15m0s"})}},
		{name: "marker written by another event first", marker: "2026-03-11T01:10:00Z", lostRace: true, wantMarker: "2026-03-11T01:10:00Z"},
		{name: "validation not met", tweak: func(c *types.PipelineConfig) {
			c.Validation.Rules = append(c.Validation.Rules, types.ValidationRule{Key: "other-required-sensor", Check: types.CheckExists})
		}},
		{name: "captures baseline", wantMarker: at,
			tweak: func(c *types.PipelineConfig) {
				c.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}}}
			},
			wantEvents: []publishedEvent{dryRunEvent(types.EventPostRunBaselineCaptured, nil), wouldTrigger,
				dryRunEvent(types.EventDryRunCompleted, map[string]interface{}{"triggeredAt": at, "slaStatus": "n/a"})},
			wantBaseline: map[string]interface{}{"audit-result": map[string]interface{}{"sensor_count": 500.0}}},
		{name: "SLA met", tweak: sla("08:00", 0), wantMarker: at,
			wantEvents: projection(at, "2026-03-11T01:45:00Z", "2026-03-11T08:00:00Z", 22500, "met")},
		{name: "SLA breach", now: "2026-03-11T01:50:00Z", tweak: sla("02:00", 0), wantMarker: "2026-03-11T01:50:00Z",
			wantEvents: projection("2026-03-11T01:50:00Z", "2026-03-11T02:20:00Z", "2026-03-11T02:00:00Z", -1200, "breach")},
		{name: "SLA deadline a day later", tweak: sla("08:00", 1), wantMarker: at,
			wantEvents: projection(at, "2026-03-11T01:45:00Z", "2026-03-12T08:00:00Z", 108900, "met")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline:   types.PipelineIdentity{ID: "p"},
				DryRun:     true,
				Schedule:   types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Validation: types.ValidationConfig{Trigger: "ALL", Rules: []types.ValidationRule{{Key: "upstream", Check: types.CheckExists}}},
				Job:        types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
			}
			if tt.tweak != nil {
				tt.tweak(&cfg)
			}
			d, mem, sfnFake, eb := jobWorld(t, cfg)
			ctx := context.Background()
			for key, data := range map[string]map[string]interface{}{"upstream": {"status": "ready"}, "audit-result": {"sensor_count": 500}} {
				if err := d.Store.WriteSensor(ctx, "p", key, data); err != nil {
					t.Fatal(err)
				}
			}
			if tt.marker != "" {
				seedDryRunMarker(mem, date, tt.marker)
			}
			if tt.lostRace {
				fake := d.Store.Client.(*storetest.FakeDynamo)
				get := fake.GetItemFn
				fake.GetItemFn = func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
					if in.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value == types.DryRunSK("stream", date) {
						return &dynamodb.GetItemOutput{}, nil
					}
					return get(ctx, in)
				}
			}
			now := mustTime(t, cmp.Or(tt.now, at))
			d.NowFunc = func() time.Time { return now }
			record := sensorRecordWith("upstream", map[string]events.DynamoDBAttributeValue{
				"status": events.NewStringAttribute("ready"), "date": events.NewStringAttribute(date)})

			if err := handleRecord(ctx, d, record); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if !sameEvents(eb.events, tt.wantEvents) {
				t.Errorf("events = %+v, want %+v", eb.events, tt.wantEvents)
			}
			if got := dryRunMarkerAt(mem, date); got != tt.wantMarker {
				t.Errorf("dry-run marker triggeredAt = %q, want %q", got, tt.wantMarker)
			}
			baseline, err := d.Store.GetSensorData(ctx, "p", "postrun-baseline#"+date)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(baseline, tt.wantBaseline) {
				t.Errorf("baseline = %v, want %v", baseline, tt.wantBaseline)
			}
			trigger := mem.Get("control", types.PipelinePK("p"), types.TriggerSK("stream", date))
			if sfnFake.calls != 0 || trigger != nil || len(jobEvents(mem)) != 0 {
				t.Errorf("StartExecution calls %d, trigger row %v, job rows %v; want 0, none and none",
					sfnFake.calls, trigger, jobEvents(mem))
			}
		})
	}
}

// TestStreamDryRunPostRunSensor: a post-run sensor on a dry-run pipeline
// reports drift against the baseline once the dry run would have triggered
// (its DRY_RUN# marker exists), and never starts a run or writes a rerun
// request. The baseline holds sensor_count 500 for 2026-03-11, the sensor's
// date. The wall clock is a day later, so the run date must come from the
// sensor.
func TestStreamDryRunPostRunSensor(t *testing.T) {
	const date = "2026-03-11"
	tests := []struct {
		name       string
		noMarker   bool
		count      string
		wantEvents []publishedEvent
	}{
		{name: "drift", count: "520", wantEvents: []publishedEvent{dryRunEvent(types.EventDryRunDrift, map[string]interface{}{
			"previousCount": 500.0, "currentCount": 520.0, "delta": 20.0, "driftThreshold": 0.0,
			"driftField": "sensor_count", "sensorKey": "audit-result"})}},
		{name: "no drift", count: "500"},
		{name: "no marker", noMarker: true, count: "520"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				DryRun:   true,
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP, Config: httpJobConfig},
				PostRun:  &types.PostRunConfig{Rules: []types.ValidationRule{{Key: "audit-result", Check: types.CheckExists}}},
			})
			ctx := context.Background()
			if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{
				"audit-result": map[string]interface{}{"sensor_count": 500}}); err != nil {
				t.Fatal(err)
			}
			if !tt.noMarker {
				seedDryRunMarker(mem, date, "2026-03-11T01:15:00Z")
			}
			now := mustTime(t, "2026-03-12T01:15:00Z")
			d.NowFunc = func() time.Time { return now }
			record := sensorRecordWith("audit-result", map[string]events.DynamoDBAttributeValue{
				"sensor_count": events.NewNumberAttribute(tt.count), "date": events.NewStringAttribute(date)})

			if err := handleRecord(ctx, d, record); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			if !sameEvents(eb.events, tt.wantEvents) {
				t.Errorf("events = %+v, want %+v", eb.events, tt.wantEvents)
			}
			rerun := mem.Get("control", types.PipelinePK("p"), types.RerunRequestSK("stream", date))
			if sfnFake.calls != 0 || rerun != nil || len(jobEvents(mem)) != 0 {
				t.Errorf("StartExecution calls %d, rerun request %v, job rows %v; want 0, none and none",
					sfnFake.calls, rerun, jobEvents(mem))
			}
		})
	}
}
