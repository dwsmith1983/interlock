package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestAlertPutList validates alert storage and retrieval.
func TestAlertPutList(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	a1 := types.Alert{
		AlertID:    "a1",
		Level:      types.AlertLevelError,
		PipelineID: "pipe-alert-test",
		Message:    "first alert",
		Timestamp:  time.Now().Add(-2 * time.Minute),
	}
	a2 := types.Alert{
		AlertID:    "a2",
		Level:      types.AlertLevelWarning,
		PipelineID: "pipe-alert-test",
		Message:    "second alert",
		Timestamp:  time.Now().Add(-1 * time.Minute),
	}
	a3 := types.Alert{
		AlertID:    "a3",
		Level:      types.AlertLevelInfo,
		PipelineID: "pipe-alert-other",
		Message:    "other pipeline",
		Timestamp:  time.Now(),
	}

	for _, a := range []types.Alert{a1, a2, a3} {
		if err := prov.PutAlert(ctx, a); err != nil {
			t.Fatalf("PutAlert: %v", err)
		}
	}

	// List for specific pipeline.
	alerts, err := prov.ListAlerts(ctx, "pipe-alert-test", 10)
	if err != nil {
		t.Fatalf("ListAlerts: %v", err)
	}
	if len(alerts) != 2 {
		t.Errorf("expected 2 alerts, got %d", len(alerts))
	}

	// List all.
	all, err := prov.ListAllAlerts(ctx, 10)
	if err != nil {
		t.Fatalf("ListAllAlerts: %v", err)
	}
	if len(all) < 3 {
		t.Errorf("expected at least 3 alerts, got %d", len(all))
	}
}
