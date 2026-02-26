package engine

import (
	"math"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ReadinessEvaluator is a function that evaluates readiness from trait results.
type ReadinessEvaluator func(rule types.ReadinessRule, traitResults []traitResult) (types.ReadinessStatus, []string)

// traitResult pairs a trait evaluation with its required/optional status.
type traitResult struct {
	TraitType string
	Required  bool
	Status    types.TraitStatus
}

// readinessEvaluators maps readiness rule types to their evaluation functions.
var readinessEvaluators = map[types.ReadinessRuleType]ReadinessEvaluator{
	types.AllRequiredPass: evalAllRequiredPass,
	types.MajorityPass:    evalMajorityPass,
}

// EvaluateReadiness dispatches to the appropriate readiness evaluator for the given rule.
func EvaluateReadiness(rule types.ReadinessRule, results []traitResult) (status types.ReadinessStatus, blocking []string) {
	eval, ok := readinessEvaluators[rule.Type]
	if !ok {
		// Fallback to all-required-pass for unknown rule types.
		eval = evalAllRequiredPass
	}
	return eval(rule, results)
}

// evalAllRequiredPass returns READY if all required traits pass.
func evalAllRequiredPass(_ types.ReadinessRule, results []traitResult) (status types.ReadinessStatus, blocking []string) {
	for _, r := range results {
		if r.Required && r.Status != types.TraitPass {
			blocking = append(blocking, r.TraitType)
		}
	}
	if len(blocking) > 0 {
		return types.NotReady, blocking
	}
	return types.Ready, nil
}

// evalMajorityPass returns READY if N of M required traits pass.
// The threshold comes from rule.Parameters["threshold"] (int). If not set,
// defaults to simple majority (> 50%).
func evalMajorityPass(rule types.ReadinessRule, results []traitResult) (status types.ReadinessStatus, blocking []string) {
	var requiredTotal int
	var requiredPassed int

	for _, r := range results {
		if !r.Required {
			continue
		}
		requiredTotal++
		if r.Status == types.TraitPass {
			requiredPassed++
		} else {
			blocking = append(blocking, r.TraitType)
		}
	}

	if requiredTotal == 0 {
		return types.Ready, nil
	}

	// Determine threshold.
	threshold := int(math.Ceil(float64(requiredTotal) / 2.0)) // simple majority default
	if rule.Parameters != nil {
		if v, ok := rule.Parameters["threshold"]; ok {
			switch n := v.(type) {
			case float64:
				threshold = int(n)
			case int:
				threshold = n
			}
		}
	}

	if requiredPassed >= threshold {
		return types.Ready, nil
	}
	return types.NotReady, blocking
}
