package providertest

import (
	"context"
	"sort"
	"testing"

	"github.com/dwsmith1983/interlock/internal/provider"
)

// TestDependencyCRUD validates dependency index operations.
func TestDependencyCRUD(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Add dependencies.
	if err := prov.PutDependency(ctx, "upstream-a", "downstream-1"); err != nil {
		t.Fatalf("PutDependency: %v", err)
	}
	if err := prov.PutDependency(ctx, "upstream-a", "downstream-2"); err != nil {
		t.Fatalf("PutDependency: %v", err)
	}

	// List dependents.
	deps, err := prov.ListDependents(ctx, "upstream-a")
	if err != nil {
		t.Fatalf("ListDependents: %v", err)
	}
	sort.Strings(deps)
	if len(deps) != 2 {
		t.Fatalf("expected 2 dependents, got %d", len(deps))
	}
	if deps[0] != "downstream-1" || deps[1] != "downstream-2" {
		t.Errorf("unexpected dependents: %v", deps)
	}

	// Remove one.
	if err := prov.RemoveDependency(ctx, "upstream-a", "downstream-1"); err != nil {
		t.Fatalf("RemoveDependency: %v", err)
	}

	deps, err = prov.ListDependents(ctx, "upstream-a")
	if err != nil {
		t.Fatalf("ListDependents after remove: %v", err)
	}
	if len(deps) != 1 || deps[0] != "downstream-2" {
		t.Errorf("expected [downstream-2], got %v", deps)
	}

	// Empty upstream returns empty slice.
	empty, err := prov.ListDependents(ctx, "nonexistent-upstream")
	if err != nil {
		t.Fatalf("ListDependents empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected empty, got %v", empty)
	}
}
