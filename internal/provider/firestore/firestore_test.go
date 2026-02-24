//go:build integration

package firestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/dwsmith1983/interlock/internal/provider/providertest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

func setupTestProvider(t *testing.T) *FirestoreProvider {
	t.Helper()
	ctx := context.Background()
	collection := fmt.Sprintf("interlock-test-%d", time.Now().UnixNano())
	cfg := &types.FirestoreConfig{
		ProjectID:  "test-project",
		Collection: collection,
		Emulator:   "localhost:8681",
	}
	prov, err := New(cfg)
	if err != nil {
		t.Skipf("Firestore emulator not available: %v", err)
	}
	if err := prov.Start(ctx); err != nil {
		t.Skipf("Firestore emulator not available: %v", err)
	}
	t.Cleanup(func() {
		_ = prov.Stop(context.Background())
	})
	return prov
}

func TestConformance(t *testing.T) {
	prov := setupTestProvider(t)
	providertest.RunAll(t, prov)
}
