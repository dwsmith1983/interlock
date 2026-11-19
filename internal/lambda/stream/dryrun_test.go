package stream

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
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
				Job:      types.JobConfig{Type: types.TriggerHTTP},
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
