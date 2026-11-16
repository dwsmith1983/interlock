package lambda_test

import (
	"testing"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// TestTriggerRegistryTypesPassValidation: a config may name any registered
// trigger type, so adding one to the registry alone cannot reject its configs.
func TestTriggerRegistryTypesPassValidation(t *testing.T) {
	for typ := range lambda.TriggerUnmarshalers {
		cfg := types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: "p"}, Job: types.JobConfig{Type: typ}}
		if errs := validation.ValidatePipelineConfig(&cfg); len(errs) > 0 {
			t.Errorf("job.type %q: %v", typ, errs)
		}
	}
}
