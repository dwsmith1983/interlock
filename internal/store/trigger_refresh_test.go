package store_test

import (
	"context"
	"maps"
	"strconv"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestRefreshRunningTriggerLock: a retry may refresh the lock of a run that
// is still RUNNING, but must not revive a finished row or create a missing one.
func TestRefreshRunningTriggerLock(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	for _, status := range []string{types.TriggerStatusRunning, types.TriggerStatusCompleted, types.TriggerStatusFailedFinal, ""} {
		t.Run("status "+status, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			s := storetest.NewStore(fake)
			s.Now = func() time.Time { return now }
			pk, sk := types.PipelinePK("p"), types.TriggerSK("s", "2026-03-01")
			seeded := map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: sk},
				"status": &ddbtypes.AttributeValueMemberS{Value: status}, "staleAt": &ddbtypes.AttributeValueMemberN{Value: "1"},
			}
			if status != "" {
				mem.Put("control", seeded)
			}

			refreshed, err := s.RefreshRunningTriggerLock(context.Background(), "p", "s", "2026-03-01", time.Hour)
			if err != nil || refreshed != (status == types.TriggerStatusRunning) {
				t.Fatalf("RefreshRunningTriggerLock = %v, %v, want %v, nil", refreshed, err, status == types.TriggerStatusRunning)
			}
			item := mem.Get("control", pk, sk)
			if status == "" {
				if item != nil {
					t.Fatalf("a missing row was created: %v", item)
				}
				return
			}
			if !refreshed {
				if !maps.Equal(item, seeded) {
					t.Errorf("refused refresh changed the row to %v, want %v", item, seeded)
				}
				return
			}
			want := strconv.FormatInt(now.Add(time.Hour).Unix(), 10)
			got := [2]string{item["status"].(*ddbtypes.AttributeValueMemberS).Value, item["staleAt"].(*ddbtypes.AttributeValueMemberN).Value}
			if got != [2]string{status, want} {
				t.Errorf("row status/staleAt = %v, want [%s %s]", got, status, want)
			}
		})
	}
}
