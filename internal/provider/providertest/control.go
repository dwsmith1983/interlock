package providertest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
)

// TestControlStatus verifies that GetControlStatus returns nil for a
// nonexistent pipeline without error.
func TestControlStatus(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	got, err := prov.GetControlStatus(ctx, "pipe-control-nonexistent")
	require.NoError(t, err)
	assert.Nil(t, got)
}
