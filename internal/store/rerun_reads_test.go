package store_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
)

// TestRerunRequestReadsAreConsistent: the stream-router decides a rerun
// request on the trigger row and the rerun budget, both of which another
// writer may have just changed, so both reads are strongly consistent, like
// CountReruns.
func TestRerunRequestReadsAreConsistent(t *testing.T) {
	var gets []*dynamodb.GetItemInput
	var queries []*dynamodb.QueryInput
	fake := &storetest.FakeDynamo{
		GetItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			gets = append(gets, in)
			return &dynamodb.GetItemOutput{}, nil
		},
		QueryFn: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			queries = append(queries, in)
			return &dynamodb.QueryOutput{}, nil
		},
	}
	s := storetest.NewStore(fake)
	ctx := context.Background()

	if _, err := s.GetTrigger(ctx, "p", "s", "2026-03-01"); err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if _, err := s.CountRerunsBySource(ctx, "p", "s", "2026-03-01", []string{"manual"}); err != nil {
		t.Fatalf("CountRerunsBySource: %v", err)
	}

	if len(gets) != 1 || gets[0].ConsistentRead == nil || !*gets[0].ConsistentRead {
		t.Errorf("GetTrigger requests = %+v, want one with ConsistentRead true", gets)
	}
	if len(queries) != 1 || queries[0].ConsistentRead == nil || !*queries[0].ConsistentRead {
		t.Errorf("CountRerunsBySource requests = %+v, want one with ConsistentRead true", queries)
	}
}
