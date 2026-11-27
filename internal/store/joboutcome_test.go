package store_test

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestGetLatestJobOutcome: the newest JOB# row for the date whose event is
// not one the stream-router writes, or once wrote, about reruns and late data
// (rerun-accepted, rerun-rejected, late-data-arrival), or nil. Rows are listed
// oldest first and get SK timestamps 1, 2, 3, ...
func TestGetLatestJobOutcome(t *testing.T) {
	audit := []string{types.JobEventLateDataArrival, types.JobEventRerunRejected, types.JobEventRerunAccepted}
	manyAudit := []string{types.JobEventSuccess}
	for i := range 160 {
		manyAudit = append(manyAudit, audit[i%len(audit)])
	}
	tests := []struct {
		name       string
		events     []string
		badTTL     int    // 1-based index of a row whose ttl is a string; 0 for none
		wantEvent  string // "" means nil
		wantMillis int
		wantCalls  int
	}{
		{name: "no rows", wantCalls: 1},
		{name: "success alone", events: []string{types.JobEventSuccess}, wantEvent: types.JobEventSuccess, wantMillis: 1, wantCalls: 1},
		{name: "success under audit rows", events: append([]string{types.JobEventSuccess}, audit...),
			wantEvent: types.JobEventSuccess, wantMillis: 1, wantCalls: 1},
		{name: "only audit rows", events: audit, wantCalls: 1},
		{name: "fail above an older success", events: []string{types.JobEventSuccess, types.JobEventFail, types.JobEventRerunRejected},
			wantEvent: types.JobEventFail, wantMillis: 2, wantCalls: 1},
		{name: "unknown event on top", events: []string{types.JobEventSuccess, "some-future-event"},
			wantEvent: "some-future-event", wantMillis: 2, wantCalls: 1},
		{name: "160 audit rows above a success", events: manyAudit,
			wantEvent: types.JobEventSuccess, wantMillis: 1, wantCalls: 2},
		{name: "older row with a string ttl", events: []string{types.JobEventFail, types.JobEventSuccess, types.JobEventRerunRejected}, badTTL: 1,
			wantEvent: types.JobEventSuccess, wantMillis: 2, wantCalls: 1},
		{name: "audit row with a string ttl on top", events: []string{types.JobEventSuccess, types.JobEventRerunRejected}, badTTL: 2,
			wantEvent: types.JobEventSuccess, wantMillis: 1, wantCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			for i, event := range tt.events {
				r := jobRow(i+1, event, "run-a")
				r["ttl"] = &ddbtypes.AttributeValueMemberN{Value: "1800000000"}
				if i+1 == tt.badTTL {
					r["ttl"] = &ddbtypes.AttributeValueMemberS{Value: "soon"}
				}
				mem.Put("joblog", r)
			}
			// Another date's success must not be read as this date's.
			other := jobRow(99, types.JobEventSuccess, "run-z")
			other["SK"] = &ddbtypes.AttributeValueMemberS{Value: types.JobSK("s", "2026-03-02", "99")}
			mem.Put("joblog", other)
			var seen []*dynamodb.QueryInput
			query := fake.QueryFn
			fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
				seen = append(seen, in)
				return query(ctx, in)
			}
			s := storetest.NewStore(fake)

			rec, err := s.GetLatestJobOutcome(context.Background(), "p", "s", "2026-03-01")
			if err != nil {
				t.Fatalf("GetLatestJobOutcome: %v", err)
			}

			if tt.wantEvent == "" {
				if rec != nil {
					t.Fatalf("record = %+v, want nil", rec)
				}
			} else {
				wantSK := types.JobSK("s", "2026-03-01", strconv.Itoa(tt.wantMillis))
				if rec == nil || rec.Event != tt.wantEvent || rec.SK != wantSK || rec.RunID != "run-a" {
					t.Fatalf("record = %+v, want event %q SK %q", rec, tt.wantEvent, wantSK)
				}
			}
			if len(seen) != tt.wantCalls {
				t.Errorf("Query calls = %d, want %d", len(seen), tt.wantCalls)
			}
			for _, in := range seen {
				if in.ScanIndexForward == nil || *in.ScanIndexForward {
					t.Errorf("ScanIndexForward = %v, want false (newest first)", in.ScanIndexForward)
				}
				if in.Limit == nil || *in.Limit != 100 || in.FilterExpression != nil {
					t.Errorf("Limit = %d (set: %v), FilterExpression = %v; want 100 and none", aws.ToInt32(in.Limit), in.Limit != nil, in.FilterExpression)
				}
			}
		})
	}
}

func TestGetLatestJobOutcome_QueryError(t *testing.T) {
	injected := errors.New("dynamodb: throttled")
	s := storetest.NewStore(&storetest.FakeDynamo{QueryFn: func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		return nil, injected
	}})

	rec, err := s.GetLatestJobOutcome(context.Background(), "p", "s", "2026-03-01")
	if rec != nil || !errors.Is(err, injected) {
		t.Fatalf("GetLatestJobOutcome = (%+v, %v), want (nil, an error wrapping %v)", rec, err, injected)
	}
}

func TestGetLatestJobOutcome_UnmarshalError(t *testing.T) {
	fake, mem := storetest.NewMem()
	r := jobRow(1, types.JobEventSuccess, "run-a")
	r["ttl"] = &ddbtypes.AttributeValueMemberS{Value: "soon"}
	mem.Put("joblog", r)

	rec, err := storetest.NewStore(fake).GetLatestJobOutcome(context.Background(), "p", "s", "2026-03-01")
	if rec != nil || err == nil {
		t.Fatalf("GetLatestJobOutcome = (%+v, %v), want (nil, error)", rec, err)
	}
}
