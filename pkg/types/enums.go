// Package types defines the public domain types for the Interlock STAMP-based safety framework.
package types

// TriggerType defines how a pipeline is triggered.
type TriggerType string

// TriggerType values enumerate the supported pipeline trigger mechanisms.
const (
	TriggerHTTP          TriggerType = "http"
	TriggerCommand       TriggerType = "command"
	TriggerAirflow       TriggerType = "airflow"
	TriggerGlue          TriggerType = "glue"
	TriggerEMR           TriggerType = "emr"
	TriggerEMRServerless TriggerType = "emr-serverless"
	TriggerDatabricks    TriggerType = "databricks"
	TriggerStepFunction  TriggerType = "step-function"
)

// FailureCategory classifies why a trait evaluation or trigger failed.
type FailureCategory string

const (
	FailureTransient      FailureCategory = "TRANSIENT"
	FailurePermanent      FailureCategory = "PERMANENT"
	FailureTimeout        FailureCategory = "TIMEOUT"
	FailureEvaluatorCrash FailureCategory = "EVALUATOR_CRASH"
)
