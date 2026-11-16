package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestMarkTriggerStarted: the start time goes on an existing trigger row in UTC,
// keeping its status and TTL; a missing row is reported and not created.
func TestMarkTriggerStarted(t *testing.T) {
	ctx, at := context.Background(), time.Date(2026, 3, 10, 17, 5, 0, 0, time.FixedZone("JST", 9*3600))
	pk, sk := types.PipelinePK("p"), types.TriggerSK("cron", "2026-03-10")
	fake, mem := storetest.NewMem()
	s := storetest.NewStore(fake)

	if err := s.MarkTriggerStarted(ctx, "p", "cron", "2026-03-10", at); !errors.Is(err, store.ErrTriggerNotFound) {
		t.Fatalf("MarkTriggerStarted without a row = %v, want ErrTriggerNotFound", err)
	}
	if item := mem.Get("control", pk, sk); item != nil {
		t.Fatalf("a missing row was created: %v", item)
	}

	mem.Put("control", map[string]ddbtypes.AttributeValue{
		"PK": &ddbtypes.AttributeValueMemberS{Value: pk}, "SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusRunning}, "ttl": &ddbtypes.AttributeValueMemberN{Value: "1"},
	})
	if err := s.MarkTriggerStarted(ctx, "p", "cron", "2026-03-10", at); err != nil {
		t.Fatalf("MarkTriggerStarted: %v", err)
	}
	rec, err := s.GetTrigger(ctx, "p", "cron", "2026-03-10")
	if err != nil || rec == nil {
		t.Fatalf("GetTrigger = %v, %v", rec, err)
	}
	if rec.StartedAt != "2026-03-10T08:05:00Z" || rec.Status != types.TriggerStatusRunning || rec.TTL != 1 {
		t.Errorf("row startedAt/status/ttl = %q %q %d, want 2026-03-10T08:05:00Z RUNNING 1", rec.StartedAt, rec.Status, rec.TTL)
	}
}
