package archetype

import (
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryLoadDir(t *testing.T) {
	reg := NewRegistry()
	err := reg.LoadDir("../../testdata/archetypes")
	require.NoError(t, err)

	arch, err := reg.Get("test-archetype")
	require.NoError(t, err)
	assert.Equal(t, "test-archetype", arch.Name)
	assert.Len(t, arch.RequiredTraits, 2)
	assert.Len(t, arch.OptionalTraits, 1)
}

func TestRegistryLoadDir_StarterArchetypes(t *testing.T) {
	reg := NewRegistry()
	err := reg.LoadDir("../../archetypes")
	require.NoError(t, err)

	names := []string{"batch-ingestion", "streaming-enrichment", "ml-feature-pipeline"}
	for _, name := range names {
		arch, err := reg.Get(name)
		require.NoError(t, err, "archetype %q should exist", name)
		assert.NotEmpty(t, arch.RequiredTraits)
		assert.Equal(t, types.AllRequiredPass, arch.ReadinessRule.Type)
	}
}

func TestRegistryGetNotFound(t *testing.T) {
	reg := NewRegistry()
	_, err := reg.Get("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestResolveTraits_WithOverrides(t *testing.T) {
	arch := &types.Archetype{
		Name: "test",
		RequiredTraits: []types.TraitDefinition{
			{
				Type:           "freshness",
				DefaultConfig:  map[string]interface{}{"maxLagSeconds": 300},
				DefaultTTL:     300,
				DefaultTimeout: 30,
			},
		},
		OptionalTraits: []types.TraitDefinition{
			{Type: "quality"},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}

	pipeline := &types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test",
		Traits: map[string]types.TraitConfig{
			"freshness": {
				Evaluator: "./eval",
				Config:    map[string]interface{}{"maxLagSeconds": 60},
				TTL:       120,
				Timeout:   15,
			},
			"quality": {
				Evaluator: "./quality-eval",
			},
		},
	}

	resolved := ResolveTraits(arch, pipeline)

	// Required trait merged
	assert.Len(t, resolved, 2)
	fresh := resolved[0]
	assert.Equal(t, "freshness", fresh.Type)
	assert.True(t, fresh.Required)
	assert.Equal(t, "./eval", fresh.Evaluator)
	assert.Equal(t, 60, fresh.Config["maxLagSeconds"])
	assert.Equal(t, 120, fresh.TTL)
	assert.Equal(t, 15, fresh.Timeout)

	// Optional trait included because pipeline has config
	qual := resolved[1]
	assert.Equal(t, "quality", qual.Type)
	assert.False(t, qual.Required)
	assert.Equal(t, "./quality-eval", qual.Evaluator)
}

func TestResolveTraits_NoPipelineOverrides(t *testing.T) {
	arch := &types.Archetype{
		Name: "test",
		RequiredTraits: []types.TraitDefinition{
			{
				Type:           "freshness",
				DefaultConfig:  map[string]interface{}{"maxLagSeconds": 300},
				DefaultTTL:     300,
				DefaultTimeout: 30,
			},
		},
		OptionalTraits: []types.TraitDefinition{
			{Type: "quality"},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}

	resolved := ResolveTraits(arch, nil)

	// Only required traits, optional skipped without pipeline config
	assert.Len(t, resolved, 1)
	assert.Equal(t, "freshness", resolved[0].Type)
	assert.Equal(t, 300, resolved[0].Config["maxLagSeconds"])
}
