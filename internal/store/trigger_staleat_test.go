package store_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestTriggerLock_StaleAtNotTTL: every writer that leaves a trigger row
// RUNNING stamps its stale deadline as staleAt and leaves no ttl, so DynamoDB
// never deletes a running row; a ttl left by an older writer is removed.
func TestTriggerLock_StaleAtNotTTL(t *testing.T) {
	const staleAfter = 90 * time.Minute
	now := time.Unix(1_000_000, 0)
	acquire := func(ctx context.Context, s *store.Store) (bool, error) {
		return s.AcquireTriggerLock(ctx, "p", "s", "2026-03-01", staleAfter)
	}
	reset := func(ctx context.Context, s *store.Store) (bool, error) {
		return s.ResetTriggerLock(ctx, "p", "s", "2026-03-01", staleAfter)
	}
	refresh := func(ctx context.Context, s *store.Store) (bool, error) {
		return s.RefreshRunningTriggerLock(ctx, "p", "s", "2026-03-01", staleAfter)
	}
	tests := []struct {
		name   string
		status string // status of the seeded row; "" means no row
		ttl    bool   // seed the row with ttl 1
		write  func(context.Context, *store.Store) (bool, error)
	}{
		{name: "acquire", write: acquire},
		{name: "reset of a COMPLETED row", status: types.TriggerStatusCompleted, write: reset},
		{name: "refresh of a RUNNING row", status: types.TriggerStatusRunning, write: refresh},
		{name: "reset of a row with ttl", status: types.TriggerStatusCompleted, ttl: true, write: reset},
		{name: "refresh of a row with ttl", status: types.TriggerStatusRunning, ttl: true, write: refresh},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			s := storetest.NewStore(fake)
			s.Now = func() time.Time { return now }
			pk, sk := types.PipelinePK("p"), types.TriggerSK("s", "2026-03-01")
			if tt.status != "" {
				row := map[string]ddbtypes.AttributeValue{
					"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: sk},
					"status": &ddbtypes.AttributeValueMemberS{Value: tt.status},
				}
				if tt.ttl {
					row["ttl"] = &ddbtypes.AttributeValueMemberN{Value: "1"}
				}
				mem.Put("control", row)
			}

			if ok, err := tt.write(context.Background(), s); !ok || err != nil {
				t.Fatalf("write = %v, %v, want true, nil", ok, err)
			}
			item := mem.Get("control", pk, sk)
			want := strconv.FormatInt(now.Add(staleAfter).Unix(), 10)
			staleAt, _ := item["staleAt"].(*ddbtypes.AttributeValueMemberN)
			if _, hasTTL := item["ttl"]; staleAt == nil || staleAt.Value != want || hasTTL {
				t.Errorf("row %v, want staleAt %s and no ttl", item, want)
			}
		})
	}
}
