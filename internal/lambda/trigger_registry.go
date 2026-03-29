package lambda

import (
	"encoding/json"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TriggerUnmarshalers maps each trigger type to a function that unmarshals
// raw JSON into the corresponding typed field on TriggerConfig.
// Exported so that sub-packages (e.g. orchestrator) can reuse the single
// canonical registry without duplicating it.
var TriggerUnmarshalers = map[types.TriggerType]func([]byte, *types.TriggerConfig) error{
	types.TriggerHTTP:          UnmarshalTo(func(tc *types.TriggerConfig, c *types.HTTPTriggerConfig) { tc.HTTP = c }),
	types.TriggerCommand:       UnmarshalTo(func(tc *types.TriggerConfig, c *types.CommandTriggerConfig) { tc.Command = c }),
	types.TriggerAirflow:       UnmarshalTo(func(tc *types.TriggerConfig, c *types.AirflowTriggerConfig) { tc.Airflow = c }),
	types.TriggerGlue:          UnmarshalTo(func(tc *types.TriggerConfig, c *types.GlueTriggerConfig) { tc.Glue = c }),
	types.TriggerEMR:           UnmarshalTo(func(tc *types.TriggerConfig, c *types.EMRTriggerConfig) { tc.EMR = c }),
	types.TriggerEMRServerless: UnmarshalTo(func(tc *types.TriggerConfig, c *types.EMRServerlessTriggerConfig) { tc.EMRServerless = c }),
	types.TriggerStepFunction:  UnmarshalTo(func(tc *types.TriggerConfig, c *types.StepFunctionTriggerConfig) { tc.StepFunction = c }),
	types.TriggerDatabricks:    UnmarshalTo(func(tc *types.TriggerConfig, c *types.DatabricksTriggerConfig) { tc.Databricks = c }),
	types.TriggerLambda:        UnmarshalTo(func(tc *types.TriggerConfig, c *types.LambdaTriggerConfig) { tc.Lambda = c }),
}

// UnmarshalTo returns an unmarshaler that decodes JSON into a typed config
// struct and assigns it to the appropriate TriggerConfig field.
// Exported so that sub-packages can reference it if they need to extend the
// registry with additional trigger types.
func UnmarshalTo[T any](assign func(*types.TriggerConfig, *T)) func([]byte, *types.TriggerConfig) error {
	return func(data []byte, tc *types.TriggerConfig) error {
		var c T
		if err := json.Unmarshal(data, &c); err != nil {
			return err
		}
		assign(tc, &c)
		return nil
	}
}
