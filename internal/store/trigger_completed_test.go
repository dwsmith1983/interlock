package store_test

import (
	"context"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestMarkTriggerCompleted: the row becomes COMPLETED with the completion time
// in UTC and no TTL, keeping startedAt; a later completion moves completedAt.
func TestMarkTriggerCompleted(t *testing.T) {
	ctx := context.Background()
	pk, sk := types.PipelinePK("p"), types.TriggerSK("stream", "2026-03-10")
	fake, mem := storetest.NewMem()
	s := storetest.NewStore(fake)
	mem.Put("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		"status":    &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning},
		"ttl":       &ddbtypes.AttributeValueMemberN{Value: "1"},
		"startedAt": &ddbtypes.AttributeValueMemberS{Value: "2026-03-10T11:00:00Z"},
	})

	at := time.Date(2026, 3, 10, 21, 0, 0, 900_000_000, time.FixedZone("JST", 9*3600))
	if err := s.MarkTriggerCompleted(ctx, "p", "stream", "2026-03-10", at); err != nil {
		t.Fatalf("MarkTriggerCompleted: %v", err)
	}
	rec, err := s.GetTrigger(ctx, "p", "stream", "2026-03-10")
	if err != nil || rec == nil {
		t.Fatalf("GetTrigger = %v, %v", rec, err)
	}
	if rec.Status != types.TriggerStatusCompleted || rec.CompletedAt != "2026-03-10T12:00:00Z" || rec.TTL != 0 || rec.StartedAt != "2026-03-10T11:00:00Z" {
		t.Errorf("row status/completedAt/ttl/startedAt = %q %q %d %q, want COMPLETED 2026-03-10T12:00:00Z 0 2026-03-10T11:00:00Z",
			rec.Status, rec.CompletedAt, rec.TTL, rec.StartedAt)
	}

	if err := s.MarkTriggerCompleted(ctx, "p", "stream", "2026-03-10", time.Date(2026, 3, 11, 8, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("second MarkTriggerCompleted: %v", err)
	}
	rec, err = s.GetTrigger(ctx, "p", "stream", "2026-03-10")
	if err != nil || rec == nil {
		t.Fatalf("GetTrigger = %v, %v", rec, err)
	}
	if rec.CompletedAt != "2026-03-11T08:00:00Z" {
		t.Errorf("completedAt after the second completion = %q, want 2026-03-11T08:00:00Z", rec.CompletedAt)
	}
}

// TestMarkTriggerCompleted_NoRow: like SetTriggerStatus, a missing row is
// created COMPLETED with completedAt and no TTL.
func TestMarkTriggerCompleted_NoRow(t *testing.T) {
	ctx := context.Background()
	fake, _ := storetest.NewMem()
	s := storetest.NewStore(fake)

	if err := s.MarkTriggerCompleted(ctx, "p", "stream", "2026-03-10", time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("MarkTriggerCompleted: %v", err)
	}
	rec, err := s.GetTrigger(ctx, "p", "stream", "2026-03-10")
	if err != nil || rec == nil {
		t.Fatalf("GetTrigger = %v, %v, want the created row", rec, err)
	}
	if rec.Status != types.TriggerStatusCompleted || rec.CompletedAt != "2026-03-10T12:00:00Z" || rec.TTL != 0 {
		t.Errorf("row status/completedAt/ttl = %q %q %d, want COMPLETED 2026-03-10T12:00:00Z 0", rec.Status, rec.CompletedAt, rec.TTL)
	}
}
