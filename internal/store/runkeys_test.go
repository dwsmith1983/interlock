package store_test

import (
	"context"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestListRunKeys pins the RUN# range query: both bounds inclusive, hourly
// keys on the last day included, other row types and pipelines left out,
// SK order kept across pages.
func TestListRunKeys(t *testing.T) {
	fake, mem := storetest.NewMem()
	row := func(pipeline, sk string) {
		mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": sAttr(types.PipelinePK(pipeline)), "SK": sAttr(sk)})
	}
	for _, key := range []string{"2026-03-09", "2026-03-10", "2026-03-10T05", "2026-03-12"} {
		row("p", types.RunSK(key))
	}
	row("p", types.SensorSK("upstream"))
	row("p", types.TriggerSK("stream", "2026-03-10"))
	row("q", types.RunSK("2026-03-11"))
	// One row per page, so every multi-row result crosses a page boundary.
	query := fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		in.Limit = aws.Int32(1)
		return query(ctx, in)
	}
	s := storetest.NewStore(fake)

	tests := []struct {
		name, from, to string
		want           []string
	}{
		{"one day with its hourly key", "2026-03-10", "2026-03-10", []string{"2026-03-10", "2026-03-10T05"}},
		{"whole month in order", "2026-03-01", "2026-03-31", []string{"2026-03-09", "2026-03-10", "2026-03-10T05", "2026-03-12"}},
		{"no rows in range", "2026-04-01", "2026-04-30", nil},
		{"other pipeline's day", "2026-03-11", "2026-03-11", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.ListRunKeys(context.Background(), "p", tt.from, tt.to)
			if err != nil {
				t.Fatalf("ListRunKeys: %v", err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("ListRunKeys(%s, %s) = %v, want %v", tt.from, tt.to, got, tt.want)
			}
		})
	}
}
