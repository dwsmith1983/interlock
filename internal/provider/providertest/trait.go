package providertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/provider"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// TestTraitPutGet verifies put, get single, get all, and multi-trait operations.
func TestTraitPutGet(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	trait := types.TraitEvaluation{
		PipelineID:  "ct-trait-pg",
		TraitType:   "freshness",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}

	// Put with long TTL
	err := prov.PutTrait(ctx, "ct-trait-pg", trait, 1*time.Hour)
	require.NoError(t, err)

	// GetTrait
	got, err := prov.GetTrait(ctx, "ct-trait-pg", "freshness")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, types.TraitPass, got.Status)

	// GetTraits
	traits, err := prov.GetTraits(ctx, "ct-trait-pg")
	require.NoError(t, err)
	assert.Len(t, traits, 1)

	// Put second trait
	trait2 := types.TraitEvaluation{
		PipelineID:  "ct-trait-pg",
		TraitType:   "completeness",
		Status:      types.TraitFail,
		Reason:      "missing rows",
		EvaluatedAt: time.Now(),
	}
	err = prov.PutTrait(ctx, "ct-trait-pg", trait2, 1*time.Hour)
	require.NoError(t, err)

	traits, err = prov.GetTraits(ctx, "ct-trait-pg")
	require.NoError(t, err)
	assert.Len(t, traits, 2)
}

// TestTraitTTLExpiry verifies traits expire after their TTL.
func TestTraitTTLExpiry(t *testing.T, prov provider.Provider) {
	ctx := context.Background()

	trait := types.TraitEvaluation{
		PipelineID:  "ct-trait-ttl",
		TraitType:   "freshness",
		Status:      types.TraitPass,
		EvaluatedAt: time.Now(),
	}

	// Put with 2s TTL
	err := prov.PutTrait(ctx, "ct-trait-ttl", trait, 2*time.Second)
	require.NoError(t, err)

	// Should exist immediately
	got, err := prov.GetTrait(ctx, "ct-trait-ttl", "freshness")
	require.NoError(t, err)
	assert.NotNil(t, got)

	// Wait for TTL
	time.Sleep(3 * time.Second)

	// Should be gone
	got, err = prov.GetTrait(ctx, "ct-trait-ttl", "freshness")
	require.NoError(t, err)
	assert.Nil(t, got)

	traits, err := prov.GetTraits(ctx, "ct-trait-ttl")
	require.NoError(t, err)
	assert.Empty(t, traits)
}
