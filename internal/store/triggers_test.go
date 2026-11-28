package store_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestListTriggers pins the TRIGGER# range query for one schedule ID: both
// bounds inclusive, hourly keys on the last day included, the other schedule
// ID, other row types and other pipelines left out, SK order kept across
// pages, status and completedAt returned.
func TestListTriggers(t *testing.T) {
	fake, mem := storetest.NewMem()
	trigger := func(pipeline, schedule, date, status, completedAt string) {
		item := map[string]ddbtypes.AttributeValue{
			"PK": sAttr(types.PipelinePK(pipeline)), "SK": sAttr(types.TriggerSK(schedule, date)), "status": sAttr(status)}
		if completedAt != "" {
			item["completedAt"] = sAttr(completedAt)
		}
		mem.Put("control", item)
	}
	trigger("p", "stream", "2026-03-09", types.TriggerStatusCompleted, "2026-03-10T01:00:00Z")
	trigger("p", "stream", "2026-03-10", types.TriggerStatusRunning, "")
	trigger("p", "stream", "2026-03-10T05", types.TriggerStatusCompleted, "2026-03-10T06:00:00Z")
	trigger("p", "stream", "2026-03-12", types.TriggerStatusFailedFinal, "")
	trigger("p", "cron", "2026-03-10", types.TriggerStatusCompleted, "2026-03-10T02:00:00Z")
	trigger("q", "stream", "2026-03-11", types.TriggerStatusCompleted, "2026-03-11T02:00:00Z")
	mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": sAttr(types.PipelinePK("p")), "SK": sAttr(types.RunSK("2026-03-11"))})
	mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": sAttr(types.PipelinePK("p")), "SK": sAttr(types.SensorSK("upstream"))})
	// One row per page, so every multi-row result crosses a page boundary.
	// The read is eventually consistent.
	query := fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if aws.ToBool(in.ConsistentRead) {
			t.Error("ListTriggers asked for a consistent read")
		}
		in.Limit = aws.Int32(1)
		return query(ctx, in)
	}
	s := storetest.NewStore(fake)

	type row struct{ sk, status, completedAt string }
	sk := func(date string) string { return types.TriggerSK("stream", date) }
	tests := []struct {
		name, from, to string
		want           []row
	}{
		{"one day with its hourly key", "2026-03-10", "2026-03-10", []row{
			{sk("2026-03-10"), types.TriggerStatusRunning, ""},
			{sk("2026-03-10T05"), types.TriggerStatusCompleted, "2026-03-10T06:00:00Z"}}},
		{"whole month in order", "2026-03-01", "2026-03-31", []row{
			{sk("2026-03-09"), types.TriggerStatusCompleted, "2026-03-10T01:00:00Z"},
			{sk("2026-03-10"), types.TriggerStatusRunning, ""},
			{sk("2026-03-10T05"), types.TriggerStatusCompleted, "2026-03-10T06:00:00Z"},
			{sk("2026-03-12"), types.TriggerStatusFailedFinal, ""}}},
		{"bounds inclusive", "2026-03-09", "2026-03-09", []row{
			{sk("2026-03-09"), types.TriggerStatusCompleted, "2026-03-10T01:00:00Z"}}},
		{"no rows in range", "2026-04-01", "2026-04-30", nil},
		{"other pipeline's day", "2026-03-11", "2026-03-11", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recs, err := s.ListTriggers(context.Background(), "p", "stream", tt.from, tt.to)
			if err != nil {
				t.Fatalf("ListTriggers: %v", err)
			}
			var got []row
			for _, r := range recs {
				got = append(got, row{r.SK, r.Status, r.CompletedAt})
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ListTriggers(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}
