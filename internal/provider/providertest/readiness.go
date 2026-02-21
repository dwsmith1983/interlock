package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/interlock-systems/interlock/internal/provider"
	"github.com/interlock-systems/interlock/pkg/types"
)

// TestReadinessPutGet verifies get-when-not-set, put, and get with traits.
func TestReadinessPutGet(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	// Get when not set
	got, err := prov.GetReadiness(ctx, "ct-readiness")
	require.NoError(t, err)
	assert.Nil(t, got)

	// Put
	result := types.ReadinessResult{
		PipelineID:  "ct-readiness",
		Status:      types.Ready,
		EvaluatedAt: time.Now(),
		Traits: []types.TraitEvaluation{
			{TraitType: "freshness", Status: types.TraitPass},
		},
	}
	err = prov.PutReadiness(ctx, result)
	require.NoError(t, err)

	// Get
	got, err = prov.GetReadiness(ctx, "ct-readiness")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, types.Ready, got.Status)
	assert.Len(t, got.Traits, 1)
}
