package types

// TriggerConfig defines how to trigger a pipeline.
// Only the variant pointer matching Type should be non-nil.
type TriggerConfig struct {
	Type          TriggerType                 `yaml:"type" json:"type"`
	PollInterval  string                      `yaml:"pollInterval,omitempty" json:"pollInterval,omitempty"`
	HTTP          *HTTPTriggerConfig          `yaml:"http,omitempty" json:"http,omitempty"`
	Command       *CommandTriggerConfig       `yaml:"command,omitempty" json:"command,omitempty"`
	Airflow       *AirflowTriggerConfig       `yaml:"airflow,omitempty" json:"airflow,omitempty"`
	Glue          *GlueTriggerConfig          `yaml:"glue,omitempty" json:"glue,omitempty"`
	EMR           *EMRTriggerConfig           `yaml:"emr,omitempty" json:"emr,omitempty"`
	EMRServerless *EMRServerlessTriggerConfig `yaml:"emr_serverless,omitempty" json:"emrServerless,omitempty"`
	StepFunction  *StepFunctionTriggerConfig  `yaml:"step_function,omitempty" json:"stepFunction,omitempty"`
	Databricks    *DatabricksTriggerConfig    `yaml:"databricks,omitempty" json:"databricks,omitempty"`
}

// HTTPTriggerConfig holds configuration for HTTP triggers.
type HTTPTriggerConfig struct {
	Method  string            `yaml:"method" json:"method"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
}

// CommandTriggerConfig holds configuration for command triggers.
type CommandTriggerConfig struct {
	Command string `yaml:"command" json:"command"`
}

// AirflowTriggerConfig holds configuration for Airflow triggers.
type AirflowTriggerConfig struct {
	DagID   string            `yaml:"dag_id" json:"dagId"`
	URL     string            `yaml:"url" json:"url"`
	Headers map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty" json:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty" json:"timeout,omitempty"`
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

// EMRServerlessTriggerConfig holds configuration for AWS EMR Serverless triggers.
type EMRServerlessTriggerConfig struct {
	ApplicationID string `yaml:"application_id" json:"applicationId"`
	JobName       string `yaml:"job_name,omitempty" json:"jobName,omitempty"`
}

// StepFunctionTriggerConfig holds configuration for AWS Step Functions triggers.
type StepFunctionTriggerConfig struct {
	StateMachineARN string            `yaml:"state_machine_arn" json:"stateMachineArn"`
	Arguments       map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
}

// DatabricksTriggerConfig holds configuration for Databricks triggers.
type DatabricksTriggerConfig struct {
	WorkspaceURL string            `yaml:"workspace_url" json:"workspaceUrl"`
	JobID        string            `yaml:"job_id,omitempty" json:"jobId,omitempty"`
	Headers      map[string]string `yaml:"headers,omitempty" json:"headers,omitempty"`
	Arguments    map[string]string `yaml:"arguments,omitempty" json:"arguments,omitempty"`
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

// Calendar defines a named set of exclusion days and dates.
type Calendar struct {
	Name  string   `yaml:"name" json:"name"`
	Days  []string `yaml:"days,omitempty" json:"days,omitempty"`   // "saturday", "sunday"
	Dates []string `yaml:"dates,omitempty" json:"dates,omitempty"` // "2025-12-25"
}
