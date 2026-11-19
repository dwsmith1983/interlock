package lambda_test

import (
	"reflect"
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

// TestTriggerUnmarshalers_FillTypedConfig: each registry entry decodes into
// its own typed field on TriggerConfig.
func TestTriggerUnmarshalers_FillTypedConfig(t *testing.T) {
	tests := []struct {
		typ  types.TriggerType
		json string
		want types.TriggerConfig
	}{
		{types.TriggerStepFunction, `{"stateMachineArn":"arn:sm"}`,
			types.TriggerConfig{StepFunction: &types.StepFunctionTriggerConfig{StateMachineARN: "arn:sm"}}},
		{types.TriggerEMR, `{"clusterId":"j-1"}`,
			types.TriggerConfig{EMR: &types.EMRTriggerConfig{ClusterID: "j-1"}}},
		{types.TriggerEMRServerless, `{"applicationId":"app-1"}`,
			types.TriggerConfig{EMRServerless: &types.EMRServerlessTriggerConfig{ApplicationID: "app-1"}}},
		{types.TriggerDatabricks, `{"workspaceUrl":"https://db"}`,
			types.TriggerConfig{Databricks: &types.DatabricksTriggerConfig{WorkspaceURL: "https://db"}}},
	}
	for _, tt := range tests {
		t.Run(string(tt.typ), func(t *testing.T) {
			var tc types.TriggerConfig
			if err := lambda.TriggerUnmarshalers[tt.typ]([]byte(tt.json), &tc); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !reflect.DeepEqual(tc, tt.want) {
				t.Errorf("TriggerConfig = %+v, want %+v", tc, tt.want)
			}
		})
	}
}
