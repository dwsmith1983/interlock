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

// TestResetTriggerLock_RunningIsRefused: a rerun may take over a finished
// date, but not one whose run is still RUNNING, and never a missing row. A
// refused reset leaves the row as it was.
func TestResetTriggerLock_RunningIsRefused(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	tests := []struct {
		status    string // empty means no row
		wantReset bool
	}{
		{status: types.TriggerStatusRunning},
		{status: types.TriggerStatusCompleted, wantReset: true},   // guard
		{status: types.TriggerStatusFailedFinal, wantReset: true}, // guard
		{},
	}
	for _, tt := range tests {
		t.Run("status "+tt.status, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			s := storetest.NewStore(fake)
			s.Now = func() time.Time { return now }
			pk, sk := types.PipelinePK("p"), types.TriggerSK("s", "2026-03-01")
			seeded := map[string]ddbtypes.AttributeValue{
				"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: sk},
				"status": &ddbtypes.AttributeValueMemberS{Value: tt.status}, "staleAt": &ddbtypes.AttributeValueMemberN{Value: "1"},
			}
			if tt.status != "" {
				mem.Put("control", seeded)
			}

			reset, err := s.ResetTriggerLock(context.Background(), "p", "s", "2026-03-01", time.Hour)
			if err != nil || reset != tt.wantReset {
				t.Fatalf("ResetTriggerLock = %v, %v, want %v, nil", reset, err, tt.wantReset)
			}
			item := mem.Get("control", pk, sk)
			if tt.status == "" {
				if item != nil {
					t.Fatalf("a missing row was created: %v", item)
				}
				return
			}
			if !reset {
				if !maps.Equal(item, seeded) {
					t.Errorf("refused reset changed the row to %v, want %v", item, seeded)
				}
				return
			}
			want := [2]string{types.TriggerStatusRunning, strconv.FormatInt(now.Add(time.Hour).Unix(), 10)}
			got := [2]string{item["status"].(*ddbtypes.AttributeValueMemberS).Value, item["staleAt"].(*ddbtypes.AttributeValueMemberN).Value}
			if got != want {
				t.Errorf("row status/staleAt = %v, want %v", got, want)
			}
		})
	}
}
