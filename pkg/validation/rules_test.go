package validation

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ruleAt returns a config with r at path: the schedule.trigger condition
// (with a cutoff after any breach), validation.rules[0] or postRun.rules[0].
func ruleAt(path string, r types.ValidationRule) types.PipelineConfig {
	cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "orders"}}
	switch path {
	case "schedule.trigger":
		cfg.Schedule.Trigger = &types.TriggerCondition{Key: r.Key, Check: r.Check, Field: r.Field, Value: r.Value, Cutoff: "23:59", CutoffDayOffset: 7}
	case "validation.rules[0]":
		cfg.Validation.Rules = []types.ValidationRule{r}
	case "postRun.rules[0]":
		cfg.PostRun = &types.PostRunConfig{Rules: []types.ValidationRule{r}}
	}
	return cfg
}

func TestRuleErrors(t *testing.T) {
	type rule = types.ValidationRule
	type ruleCase struct {
		name string
		rule rule
		want []string // <p> stands for the path
	}
	tests := []ruleCase{
		{"unknown check eq", rule{Key: "orders", Check: "eq", Field: "status", Value: "ready"},
			[]string{`<p>.check "eq" must be one of exists, equals, gt, gte, lt, lte, age_lt`}},
		{"unknown check, no field or value", rule{Key: "orders", Check: "eq"},
			[]string{`<p>.check "eq" must be one of exists, equals, gt, gte, lt, lte, age_lt`}},
		{"empty check", rule{Key: "orders"},
			[]string{`<p>.check "" must be one of exists, equals, gt, gte, lt, lte, age_lt`}},
		{"age_lt 1d", rule{Key: "orders", Check: types.CheckAgeLT, Field: "updatedAt", Value: "1d"},
			[]string{"<p>.value 1d must be a positive Go duration such as 2h for check age_lt"}},
		{"empty key", rule{Check: types.CheckExists}, []string{"<p>.key is required"}},
		{"key with #", rule{Key: "orders#audit", Check: types.CheckExists},
			[]string{`<p>.key "orders#audit" must not contain #`}},
		{"equals without field", rule{Key: "orders", Check: types.CheckEquals, Value: "ready"},
			[]string{`<p>.field is required for check "equals"`}},
		{"equals without value", rule{Key: "orders", Check: types.CheckEquals, Field: "status"},
			[]string{"<p>.value is required for check equals"}},
		{"gte abc", rule{Key: "orders", Check: types.CheckGTE, Field: "count", Value: "abc"},
			[]string{`<p>.value abc must be a finite number for check "gte"`}},
		{"gte true", rule{Key: "orders", Check: types.CheckGTE, Field: "count", Value: true},
			[]string{`<p>.value true must be a finite number for check "gte"`}},
		{"gte NaN", rule{Key: "orders", Check: types.CheckGTE, Field: "count", Value: "NaN"},
			[]string{`<p>.value NaN must be a finite number for check "gte"`}},
		{"lt Inf", rule{Key: "orders", Check: types.CheckLT, Field: "count", Value: "Inf"},
			[]string{`<p>.value Inf must be a finite number for check "lt"`}},
		{"gte 1000", rule{Key: "orders", Check: types.CheckGTE, Field: "count", Value: 1000}, nil},
		{"gte float 1000", rule{Key: "orders", Check: types.CheckGTE, Field: "count", Value: 1000.0}, nil},
		{"gte string 1000", rule{Key: "orders", Check: types.CheckGTE, Field: "count", Value: "1000"}, nil},
		{"age_lt 2h", rule{Key: "orders", Check: types.CheckAgeLT, Field: "updatedAt", Value: "2h"}, nil},
		{"age_lt 7200", rule{Key: "orders", Check: types.CheckAgeLT, Field: "updatedAt", Value: 7200},
			[]string{"<p>.value 7200 must be a positive Go duration such as 2h for check age_lt"}},
		{"age_lt 0s", rule{Key: "orders", Check: types.CheckAgeLT, Field: "updatedAt", Value: "0s"},
			[]string{"<p>.value 0s must be a positive Go duration such as 2h for check age_lt"}},
		{"age_lt -1h", rule{Key: "orders", Check: types.CheckAgeLT, Field: "updatedAt", Value: "-1h"},
			[]string{"<p>.value -1h must be a positive Go duration such as 2h for check age_lt"}},
		{"exists without field", rule{Key: "orders", Check: types.CheckExists}, nil},
		{"equals true", rule{Key: "orders", Check: types.CheckEquals, Field: "complete", Value: true}, nil},
		{"key error with an unknown check", rule{Key: "a#b", Check: "eq"},
			[]string{`<p>.key "a#b" must not contain #`, `<p>.check "eq" must be one of exists, equals, gt, gte, lt, lte, age_lt`}},
		{"key and field errors together", rule{Key: "a#b", Check: types.CheckLTE, Value: 5},
			[]string{`<p>.key "a#b" must not contain #`, `<p>.field is required for check "lte"`}},
	}
	for _, name := range []string{"postrun-baseline", "postrun-check", "irregular-missed-check", "schedule-missed-check",
		"first-sensor-arrival", "relative-sla-warning-check", "relative-sla-breach-check"} {
		tests = append(tests, ruleCase{"reserved " + name, rule{Key: name, Check: types.CheckExists},
			[]string{`<p>.key "` + name + `" is a name Interlock uses for its own rows`}})
	}
	for _, path := range []string{"schedule.trigger", "validation.rules[0]", "postRun.rules[0]"} {
		for _, tt := range tests {
			t.Run(path+"/"+tt.name, func(t *testing.T) {
				var want []string
				for _, w := range tt.want {
					want = append(want, strings.ReplaceAll(w, "<p>", path))
				}
				cfg := ruleAt(path, tt.rule)
				assert.Equal(t, want, validate(&cfg))
			})
		}
	}
}

func TestValidate_ValidationMode(t *testing.T) {
	exists := []types.ValidationRule{{Key: "orders", Check: types.CheckExists}}
	tests := []struct {
		name, mode string
		rules      []types.ValidationRule
		want       []string
	}{
		{"OR", "OR", exists, []string{`validation.trigger "OR" must be ALL or ANY`}},
		{"any", "any", exists, nil},
		{"ANY", "ANY", exists, nil},
		{"all", "all", exists, nil},
		{"empty", "", exists, nil},
		{"empty, no rules", "", nil, nil},
		{"ALL, no rules", "ALL", nil, nil},
		{"ANY, no rules", "ANY", nil, []string{"validation.rules must not be empty with validation.trigger ANY"}},
		{"any, no rules", "any", nil, []string{"validation.rules must not be empty with validation.trigger ANY"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := types.PipelineConfig{
				Pipeline:   types.PipelineIdentity{ID: "orders"},
				Validation: types.ValidationConfig{Trigger: tt.mode, Rules: tt.rules},
			}
			assert.Equal(t, tt.want, validate(&cfg))
		})
	}
}

// TestCheckOps_MatchEngine ties checkOps to the operators EvaluateRule knows.
func TestCheckOps_MatchEngine(t *testing.T) {
	sensor := map[string]interface{}{"f": 1}
	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	assert.Len(t, checkOps, 7)
	for _, op := range checkOps {
		r := EvaluateRule(types.ValidationRule{Key: "k", Check: op, Field: "f", Value: 1}, sensor, now)
		assert.NotContains(t, r.Reason, "unknown operator", "check %q", op)
	}
	r := EvaluateRule(types.ValidationRule{Key: "k", Check: "eq", Field: "f", Value: 1}, sensor, now)
	assert.Contains(t, r.Reason, "unknown operator")
}
