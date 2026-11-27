package validation

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// checkOps is every operator EvaluateRule knows, in the order errors list
// them. A test fails when the engine answers "unknown operator" for one.
var checkOps = []types.CheckOp{
	types.CheckExists, types.CheckEquals, types.CheckGT, types.CheckGTE, types.CheckLT, types.CheckLTE, types.CheckAgeLT,
}

// reservedSensorKeys are the <name>#<date> rows Interlock writes under
// SENSOR# for its own bookkeeping. A rule key equal to one is rejected, as it
// would match those rows. A key with # is rejected too: it matches one fixed
// period's row instead of the per-period sensor.
var reservedSensorKeys = []string{
	"postrun-baseline", "postrun-check", "irregular-missed-check", "schedule-missed-check",
	"first-sensor-arrival", "relative-sla-warning-check", "relative-sla-breach-check",
	"manual-rerun-alert-late-data", "manual-rerun-alert-data-drift",
}

// ruleErrors reports what is wrong with rule r at path: a missing key, a key
// with # or a reserved name, an operator EvaluateRule does not know, or a
// field or value the operator cannot use. An unknown operator gets no field
// or value errors, as what it needs is not known; key errors still apply.
func ruleErrors(path string, r types.ValidationRule) []string {
	var errs []string
	switch {
	case r.Key == "":
		errs = append(errs, path+".key is required")
	case strings.Contains(r.Key, "#"):
		errs = append(errs, fmt.Sprintf("%s.key %q must not contain #", path, r.Key))
	case slices.Contains(reservedSensorKeys, r.Key):
		errs = append(errs, fmt.Sprintf("%s.key %q is a name Interlock uses for its own rows", path, r.Key))
	}
	if !slices.Contains(checkOps, r.Check) {
		ops := make([]string, len(checkOps))
		for i, op := range checkOps {
			ops[i] = string(op)
		}
		return append(errs, fmt.Sprintf("%s.check %q must be one of %s", path, r.Check, strings.Join(ops, ", ")))
	}
	if r.Check == types.CheckExists {
		return errs
	}
	if r.Field == "" {
		errs = append(errs, fmt.Sprintf("%s.field is required for check %q", path, r.Check))
	}
	switch r.Check {
	case types.CheckEquals:
		if r.Value == nil {
			errs = append(errs, path+".value is required for check equals")
		}
	case types.CheckAgeLT:
		if s, ok := r.Value.(string); !ok || !positiveDuration(s) {
			errs = append(errs, fmt.Sprintf("%s.value %v must be a positive Go duration such as 2h for check age_lt", path, r.Value))
		}
	case types.CheckGT, types.CheckGTE, types.CheckLT, types.CheckLTE:
		if f, ok := toFloat64(r.Value); !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			errs = append(errs, fmt.Sprintf("%s.value %v must be a finite number for check %q", path, r.Value, r.Check))
		}
	}
	return errs
}

// positiveDuration reports whether s is a Go duration above zero.
func positiveDuration(s string) bool {
	d, err := time.ParseDuration(s)
	return err == nil && d > 0
}

// validationErrors checks the validation block: its mode, compared in upper
// case as EvaluateRules does (empty is ALL), and each rule. ANY with no rules
// can never pass; ALL with none passes as soon as the trigger fires.
func validationErrors(v types.ValidationConfig) []string {
	var errs []string
	switch strings.ToUpper(v.Trigger) {
	case "", "ALL":
	case "ANY":
		if len(v.Rules) == 0 {
			errs = append(errs, "validation.rules must not be empty with validation.trigger ANY")
		}
	default:
		errs = append(errs, fmt.Sprintf("validation.trigger %q must be ALL or ANY", v.Trigger))
	}
	for i, r := range v.Rules {
		errs = append(errs, ruleErrors(fmt.Sprintf("validation.rules[%d]", i), r)...)
	}
	return errs
}
