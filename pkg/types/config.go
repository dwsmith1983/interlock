package types

// TriggerConfig defines how to trigger a pipeline.
// Only the variant pointer matching Type should be non-nil.
type TriggerConfig struct {
	Type          TriggerType                 `yaml:"type" json:"type"`
	PollInterval  string                      `yaml:"pollInterval,omitempty" json:"pollInterval,omitempty"`
	HTTP          *HTTPTriggerConfig          `yaml:"http,omitempty" json:"http,omitempty"`
	Airflow       *AirflowTriggerConfig       `yaml:"airflow,omitempty" json:"airflow,omitempty"`
	Glue          *GlueTriggerConfig          `yaml:"glue,omitempty" json:"glue,omitempty"`
	EMR           *EMRTriggerConfig           `yaml:"emr,omitempty" json:"emr,omitempty"`
	EMRServerless *EMRServerlessTriggerConfig `yaml:"emr_serverless,omitempty" json:"emrServerless,omitempty"`
	StepFunction  *StepFunctionTriggerConfig  `yaml:"step_function,omitempty" json:"stepFunction,omitempty"`
	Databricks    *DatabricksTriggerConfig    `yaml:"databricks,omitempty" json:"databricks,omitempty"`
	Lambda        *LambdaTriggerConfig        `yaml:"lambda,omitempty" json:"lambda,omitempty"`
}

// HTTPTriggerConfig holds configuration for HTTP triggers.
type HTTPTriggerConfig struct {
	Method  string            `yaml:"method" json:"method"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	// Run identifies the launch. The orchestrator sets it; it is never
	// configured. Each field is sent as an X-Interlock-* header and expands
	// as an ${INTERLOCK_*} variable, so the job can build its JOB# row.
	Run RunIdentity `yaml:"-" json:"-"`
	// BodyGenerated marks a Body Interlock built itself (the default body).
	// It is sent as is, without variable expansion.
	BodyGenerated bool `yaml:"-" json:"-"`
}

// RunIdentity is what a job needs to report its result: the JOB# row's PK
// (pipeline), SK (schedule and date, e.g. 2026-03-01 or 2026-03-01T05) and
// runId.
type RunIdentity struct {
	PipelineID string
	ScheduleID string
	Date       string
	RunID      string
}

// AirflowTriggerConfig holds configuration for Airflow triggers.
type AirflowTriggerConfig struct {
	DagID   string            `yaml:"dag_id" json:"dagId"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
	// APIVersion is the Airflow major version whose REST API is called: 2
	// (the default when unset) or 3.
	APIVersion int `yaml:"api_version,omitempty" json:"apiVersion,omitempty"`
	// Run identifies the launch. The orchestrator sets it; it is never
	// configured. Its fields expand as ${INTERLOCK_*} variables in Body.
	Run RunIdentity `yaml:"-" json:"-"`
	// BodyGenerated marks a Body Interlock built itself (the default conf).
	// It is sent as is, without variable expansion.
	BodyGenerated bool `yaml:"-" json:"-"`
}

// GlueTriggerConfig holds configuration for AWS Glue triggers.
type GlueTriggerConfig struct {
	JobName   string            `yaml:"job_name" json:"jobName"`
	Arguments map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// EMRTriggerConfig holds configuration for AWS EMR triggers.
type EMRTriggerConfig struct {
	ClusterID string            `yaml:"cluster_id" json:"clusterId"`
	StepName  string            `yaml:"step_name,omitempty" json:"stepName,omitempty"`
	Command   string            `yaml:"command,omitempty" json:"command,omitempty"`
	Arguments map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// EMRServerlessTriggerConfig holds configuration for AWS EMR Serverless
// triggers. The job run is a spark-submit of EntryPoint under the
// ExecutionRoleARN runtime role.
type EMRServerlessTriggerConfig struct {
	ApplicationID         string   `yaml:"application_id" json:"applicationId"`
	JobName               string   `yaml:"job_name,omitempty" json:"jobName,omitempty"`
	ExecutionRoleARN      string   `yaml:"execution_role_arn" json:"executionRoleArn"`
	EntryPoint            string   `yaml:"entry_point" json:"entryPoint"`
	EntryPointArguments   []string `yaml:"entry_point_arguments,omitempty" json:"entryPointArguments,omitempty"`
	SparkSubmitParameters string   `yaml:"spark_submit_parameters,omitempty" json:"sparkSubmitParameters,omitempty"`
	// Run identifies the launch. The orchestrator sets it; it is never
	// configured. Its run ID makes the job run's client token.
	Run RunIdentity `yaml:"-" json:"-"`
}

// StepFunctionTriggerConfig holds configuration for AWS Step Functions triggers.
type StepFunctionTriggerConfig struct {
	StateMachineARN string            `yaml:"state_machine_arn" json:"stateMachineArn"`
	Arguments       map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// DatabricksTriggerConfig holds configuration for Databricks triggers. JobID
// is the numeric ID of the job run-now starts, kept as a string so an ID above
// 2^53 stays exact.
type DatabricksTriggerConfig struct {
	WorkspaceURL string            `yaml:"workspace_url" json:"workspaceUrl"`
	JobID        string            `yaml:"job_id,omitempty" json:"jobId,omitempty"`
	Headers      map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Arguments    map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
	// Run identifies the launch. The orchestrator sets it; it is never
	// configured. Its run ID makes the run-now idempotency token.
	Run RunIdentity `yaml:"-" json:"-"`
}

// LambdaTriggerConfig holds configuration for direct AWS Lambda invocation.
type LambdaTriggerConfig struct {
	FunctionName string `yaml:"functionName" json:"functionName"`
	Payload      string `yaml:"payload,omitempty" json:"payload,omitempty"`
	// Run identifies the launch. The orchestrator sets it; it is never
	// configured. Its fields expand as ${INTERLOCK_*} variables in Payload.
	Run RunIdentity `yaml:"-" json:"-"`
	// PayloadGenerated marks a Payload Interlock built itself (the default
	// event). It is sent as is, without variable expansion.
	PayloadGenerated bool `yaml:"-" json:"-"`
}

// TriggerArguments returns the arguments map for the active trigger variant, or nil.
func (tc *TriggerConfig) TriggerArguments() map[string]string {
	if tc == nil {
		return nil
	}
	switch tc.Type {
	case TriggerGlue:
		if tc.Glue != nil {
			return tc.Glue.Arguments
		}
	case TriggerEMR:
		if tc.EMR != nil {
			return tc.EMR.Arguments
		}
	case TriggerStepFunction:
		if tc.StepFunction != nil {
			return tc.StepFunction.Arguments
		}
	case TriggerDatabricks:
		if tc.Databricks != nil {
			return tc.Databricks.Arguments
		}
	}
	return nil
}

// TriggerHeaders returns the headers map for the active trigger variant, or nil.
func (tc *TriggerConfig) TriggerHeaders() map[string]string {
	if tc == nil {
		return nil
	}
	switch tc.Type {
	case TriggerHTTP:
		if tc.HTTP != nil {
			return tc.HTTP.Headers
		}
	case TriggerAirflow:
		if tc.Airflow != nil {
			return tc.Airflow.Headers
		}
	case TriggerDatabricks:
		if tc.Databricks != nil {
			return tc.Databricks.Headers
		}
	}
	return nil
}
