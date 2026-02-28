package engine

import (
	"context"
	"sync"
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

func TestEngine_EvaluateMultiTraitMixed(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "multi-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
			{Type: "completeness", DefaultTimeout: 10},
			{Type: "accuracy", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "multi-pipe",
		Archetype: "multi-arch",
		Traits: map[string]types.TraitConfig{
			"freshness":    {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
			"completeness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
			"accuracy":     {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := New(prov, reg, runner, nil)

	result, err := eng.Evaluate(context.Background(), "multi-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	assert.Contains(t, result.Blocking, "accuracy")
	assert.Len(t, result.Traits, 3)
}

func TestEngine_EvaluatePipelineNotFound(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()
	runner := evaluator.NewRunner()
	eng := New(prov, reg, runner, nil)

	_, err := eng.Evaluate(context.Background(), "nonexistent-pipe")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "nonexistent-pipe")
}

func TestEngine_EvaluateAlertCallback(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "alert-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "alert-pipe",
		Archetype: "alert-arch",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/fail", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	var alerts []types.Alert
	eng := New(prov, reg, runner, func(_ context.Context, a types.Alert) {
		alerts = append(alerts, a)
	})

	result, err := eng.Evaluate(context.Background(), "alert-pipe")
	require.NoError(t, err)
	assert.Equal(t, types.NotReady, result.Status)
	require.NotEmpty(t, alerts)
	assert.Equal(t, "alert-pipe", alerts[0].PipelineID)
	assert.Equal(t, types.AlertLevelWarning, alerts[0].Level)
}

func TestEngine_ConcurrentEvaluation(t *testing.T) {
	prov := testutil.NewMockProvider()
	reg := archetype.NewRegistry()

	arch := &types.Archetype{
		Name: "conc-arch",
		RequiredTraits: []types.TraitDefinition{
			{Type: "freshness", DefaultTimeout: 10},
		},
		ReadinessRule: types.ReadinessRule{Type: types.AllRequiredPass},
	}
	require.NoError(t, reg.Register(arch))

	pipeline := types.PipelineConfig{
		Name:      "conc-pipe",
		Archetype: "conc-arch",
		Traits: map[string]types.TraitConfig{
			"freshness": {Evaluator: "../../testdata/evaluators/pass", Timeout: 5},
		},
	}
	require.NoError(t, prov.RegisterPipeline(context.Background(), pipeline))

	runner := evaluator.NewRunner()
	eng := New(prov, reg, runner, nil)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	results := make([]*types.ReadinessResult, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = eng.Evaluate(context.Background(), "conc-pipe")
		}(i)
	}

	wg.Wait()

	for i := 0; i < 2; i++ {
		require.NoError(t, errs[i], "goroutine %d returned error", i)
		assert.Equal(t, types.Ready, results[i].Status, "goroutine %d not ready", i)
	}
}
