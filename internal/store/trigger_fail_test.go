package store_test

import (
	"context"
	"testing"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestFailTriggerUnlessCompleted: a retry that could not start ends the run,
// but never demotes a row the next attempt already completed.
func TestFailTriggerUnlessCompleted(t *testing.T) {
	for _, status := range []string{"", types.TriggerStatusRunning, types.TriggerStatusFailedFinal, types.TriggerStatusCompleted} {
		t.Run("status "+status, func(t *testing.T) {
			fake, mem := storetest.NewMem()
			pk, sk := types.PipelinePK("p"), types.TriggerSK("s", "2026-03-01")
			if status != "" {
				mem.Put("control", map[string]ddbtypes.AttributeValue{
					"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: sk},
					"status":  &ddbtypes.AttributeValueMemberS{Value: status},
					"staleAt": &ddbtypes.AttributeValueMemberN{Value: "1"}, "ttl": &ddbtypes.AttributeValueMemberN{Value: "1"},
				})
			}
			written, err := storetest.NewStore(fake).FailTriggerUnlessCompleted(context.Background(), "p", "s", "2026-03-01")
			wantWritten := status != types.TriggerStatusCompleted
			if err != nil || written != wantWritten {
				t.Fatalf("FailTriggerUnlessCompleted = %v, %v, want %v, nil", written, err, wantWritten)
			}
			item := mem.Get("control", pk, sk)
			_, hasStaleAt := item["staleAt"]
			_, hasTTL := item["ttl"]
			want, wantKept := types.TriggerStatusFailedFinal, false
			if !wantWritten {
				want, wantKept = types.TriggerStatusCompleted, true
			}
			if got := item["status"].(*ddbtypes.AttributeValueMemberS).Value; got != want || hasStaleAt != wantKept || hasTTL != wantKept {
				t.Errorf("row status %q staleAt %v ttl %v, want %q with both %v", got, hasStaleAt, hasTTL, want, wantKept)
			}
		})
	}
}
