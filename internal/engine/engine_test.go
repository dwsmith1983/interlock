package engine

import (
	"context"
	"testing"

	"github.com/dwsmith1983/interlock/internal/archetype"
	"github.com/dwsmith1983/interlock/internal/evaluator"
	"github.com/dwsmith1983/interlock/internal/testutil"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_EvaluateAllPass(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultConfig: map[string]interface{}{"maxLagSeconds": 60}, DefaultTTL: 120, DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	var alerts []types.Alert
	eng := New(prov, reg, runner, func(_ context.Context, a types.Alert) { alerts = append(alerts, a) })

	result, err := eng.Evaluate(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.Ready, result.Status)
	assert.Empty(t, result.Blocking)
	assert.Len(t, result.Traits, 1)
	assert.Equal(t, types.TraitPass, result.Traits[0].Status)
}

func TestEngine_EvaluateWithFailure(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := New(prov, reg, runner, nil)

	result, err := eng.Evaluate(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Contains(t, result.Blocking, "freshness")
}

func TestEngine_EvaluateNoEvaluator(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
		// No traits configured = no evaluator
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := New(prov, reg, runner, nil)

	result, err := eng.Evaluate(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Contains(t, result.Blocking, "freshness")
}

func TestEngine_CheckReadiness_StaleTrait(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "test-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "test-pipe",
		Archetype: "test-arch",
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := New(prov, reg, runner, nil)

	// No trait stored = STALE
	result, err := eng.CheckReadiness(context.Background(), "test-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Len(t, result.Blocking, 1)
	assert.Contains(t, result.Blocking[0], "STALE")
}
