package providertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
)

// TestWriteCascadeMarker verifies that WriteCascadeMarker does not error.
func TestWriteCascadeMarker(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	err := prov.WriteCascadeMarker(ctx, "pipeline-a", "daily", "2026-02-23", "pipeline-b")
	require.NoError(t, err)

	// Write a second marker for idempotency
	err = prov.WriteCascadeMarker(ctx, "pipeline-a", "h14", "2026-02-23", "pipeline-c")
	assert.NoError(t, err)
}
