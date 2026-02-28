package providertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
)

// TestWriteCascadeMarker verifies that WriteCascadeMarker does not error and
// is idempotent for the same (pipeline, schedule, date, source) tuple.
func TestWriteCascadeMarker(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	err := prov.WriteCascadeMarker(ctx, "pipeline-a", "daily", "2026-02-23", "pipeline-b")
	require.NoError(t, err)

	// Idempotency: writing the exact same marker again should not error.
	err = prov.WriteCascadeMarker(ctx, "pipeline-a", "daily", "2026-02-23", "pipeline-b")
	assert.NoError(t, err)

	// Different schedule for the same pipeline should not error.
	err = prov.WriteCascadeMarker(ctx, "pipeline-a", "h14", "2026-02-23", "pipeline-c")
	assert.NoError(t, err)

	// Different source for the same pipeline + schedule should not error.
	err = prov.WriteCascadeMarker(ctx, "pipeline-a", "daily", "2026-02-23", "pipeline-d")
	assert.NoError(t, err)
}
