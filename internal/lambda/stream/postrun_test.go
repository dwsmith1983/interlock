package stream

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// postRunRecord is the stream record of the "row-count" post-run sensor for
// 2026-03-10 with the given count.
func postRunRecord(count string) events.DynamoDBEventRecord {
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
					"date":  events.NewStringAttribute("2026-03-10"),
					"count": events.NewNumberAttribute(count),
				}),
			},
		},
	}
}

// TestPostRunSensor_WatchWindow: drift and the post-run verdicts for a
// COMPLETED date are published only inside its post-run watch window (6h from
// completedAt here). A RUNNING date's inflight drift is not bounded by it, and
// a RUNNING date with no drift or no baseline publishes nothing. The baseline
// count is 100 unless a row has none.
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
		wantEvents  []string
		wantRerun   bool
	}{
		{name: "inside, drift (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T17:59:59Z", count: "120",
			wantEvents: drift, wantRerun: true},
		{name: "at the limit", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T18:00:00Z", count: "120"},
		{name: "inside, no drift (guard)", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T13:00:00Z", count: "100",
			wantEvents: []string{string(types.EventPostRunPassed)}},
		{name: "after, no drift", completedAt: "2026-03-10T12:00:00Z", now: "2026-03-10T19:00:00Z", count: "100"},
		{name: "no completedAt", now: "2026-03-10T13:00:00Z", count: "120"},
		{name: "RUNNING row (inflight, guard)", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "120",
			wantEvents: []string{string(types.EventPostRunDriftInflight)}},
		{name: "RUNNING, no drift", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "100"},
		{name: "RUNNING, no baseline", status: types.TriggerStatusRunning, now: "2026-03-10T13:00:00Z", count: "500", noBaseline: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			threshold := 0.0
			d, mem, sfnFake, eb := jobWorld(t, types.PipelineConfig{
				Pipeline: types.PipelineIdentity{ID: "p"},
				Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists}},
				Job:      types.JobConfig{Type: types.TriggerHTTP},
				PostRun: &types.PostRunConfig{
					Rules:      []types.ValidationRule{{Key: "row-count", Check: types.CheckExists}},
					DriftField: "count", DriftThreshold: &threshold, WatchWindow: "6h",
				},
			})
			row := completedRow(date, tt.completedAt)
			if tt.status != "" {
				row["status"] = &ddbtypes.AttributeValueMemberS{Value: tt.status}
			}
			mem.Put("control", row)
			ctx := context.Background()
			if !tt.noBaseline {
				if err := d.Store.WriteSensor(ctx, "p", "postrun-baseline#"+date, map[string]interface{}{
					"row-count": map[string]interface{}{"count": 100},
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Store.WriteSensor(ctx, "p", "row-count", map[string]interface{}{"date": date}); err != nil {
				t.Fatal(err)
			}
			now := mustTime(t, tt.now)
			d.NowFunc = func() time.Time { return now }

			if err := handleRecord(ctx, d, postRunRecord(tt.count)); err != nil {
				t.Fatalf("handleRecord: %v", err)
			}

			var gotEvents []string
			for _, e := range eb.events {
				gotEvents = append(gotEvents, e.Type)
			}
			if sfnFake.calls != 0 || !slices.Equal(gotEvents, tt.wantEvents) {
				t.Errorf("StartExecution calls %d, events %v; want 0 and %v", sfnFake.calls, gotEvents, tt.wantEvents)
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
		})
	}
}
