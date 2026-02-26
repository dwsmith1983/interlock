package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestTraitHistory validates trait history accumulation via PutTrait dual-write.
func TestTraitHistory(t *testing.T, prov provider.Provider) {
	ctx := context.Background()
	now := time.Now()

	// Write 3 evaluations for the same trait.
	for i := 0; i < 3; i++ {
		te := types.TraitEvaluation{
			PipelineID:  "pipe-hist-test",
			TraitType:   "freshness",
			Status:      types.TraitPass,
			EvaluatedAt: now.Add(time.Duration(i) * time.Minute),
		}
		if err := prov.PutTrait(ctx, "pipe-hist-test", te, 0); err != nil {
			t.Fatalf("PutTrait %d: %v", i, err)
		}
	}

	// List history.
	history, err := prov.ListTraitHistory(ctx, "pipe-hist-test", "freshness", 10)
	if err != nil {
		t.Fatalf("ListTraitHistory: %v", err)
	}
	if len(history) != 3 {
		t.Errorf("expected 3 history entries, got %d", len(history))
	}

	// Verify newest first.
	if len(history) >= 2 && history[0].EvaluatedAt.Before(history[1].EvaluatedAt) {
		t.Error("expected newest first ordering")
	}
}
