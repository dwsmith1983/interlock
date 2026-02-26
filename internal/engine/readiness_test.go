package engine

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func TestEvaluateReadiness_AllRequired_AllPass(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: true, Status: types.TraitPass},
		{TraitType: "t3", Required: true, Status: types.TraitPass},
	}

	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: types.AllRequiredPass}, results)
	assert.Equal(t, types.Ready, status)
	assert.Empty(t, blocking)
}

func TestEvaluateReadiness_AllRequired_OneBlocking(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: true, Status: types.TraitFail},
		{TraitType: "t3", Required: true, Status: types.TraitPass},
	}

	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: types.AllRequiredPass}, results)
	assert.Equal(t, types.NotReady, status)
	assert.Equal(t, []string{"t2"}, blocking)
}

func TestEvaluateReadiness_AllRequired_OptionalIgnored(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: false, Status: types.TraitFail}, // optional — should not block
	}

	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: types.AllRequiredPass}, results)
	assert.Equal(t, types.Ready, status)
	assert.Empty(t, blocking)
}

func TestEvaluateReadiness_Majority_SimpleThreshold(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: true, Status: types.TraitPass},
		{TraitType: "t3", Required: true, Status: types.TraitFail},
	}

	// 3 required, majority = ceil(3/2) = 2. 2 pass → Ready.
	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: types.MajorityPass}, results)
	assert.Equal(t, types.Ready, status)
	assert.Empty(t, blocking)
}

func TestEvaluateReadiness_Majority_BelowThreshold(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: true, Status: types.TraitFail},
		{TraitType: "t3", Required: true, Status: types.TraitFail},
	}

	// 3 required, majority = 2. Only 1 pass → NotReady.
	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: types.MajorityPass}, results)
	assert.Equal(t, types.NotReady, status)
	assert.ElementsMatch(t, []string{"t2", "t3"}, blocking)
}

func TestEvaluateReadiness_Majority_CustomThreshold(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: true, Status: types.TraitFail},
		{TraitType: "t3", Required: true, Status: types.TraitFail},
	}

	// Custom threshold of 1: only need 1 pass.
	rule := types.ReadinessRule{
		Type:       types.MajorityPass,
		Parameters: map[string]interface{}{"threshold": float64(1)},
	}
	status, blocking := EvaluateReadiness(rule, results)
	assert.Equal(t, types.Ready, status)
	assert.Empty(t, blocking)
}

func TestEvaluateReadiness_Majority_NoRequired(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: false, Status: types.TraitFail},
	}

	// 0 required traits → Ready by default.
	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: types.MajorityPass}, results)
	assert.Equal(t, types.Ready, status)
	assert.Empty(t, blocking)
}

func TestEvaluateReadiness_UnknownRule_FallsBackToAllRequired(t *testing.T) {
	results := []traitResult{
		{TraitType: "t1", Required: true, Status: types.TraitPass},
		{TraitType: "t2", Required: true, Status: types.TraitFail},
	}

	// Unknown rule type falls back to all-required-pass.
	status, blocking := EvaluateReadiness(types.ReadinessRule{Type: "custom-rule"}, results)
	assert.Equal(t, types.NotReady, status)
	assert.Equal(t, []string{"t2"}, blocking)
}
