package orchestrator_test

import (
	"encoding/json"
	"testing"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/orchestrator"
)

// TestExtractRunID covers the metadata shape emitted by every trigger type in
// internal/trigger. The Step Functions CheckJob state dereferences
// $.triggerResult.runId unconditionally, so a shape that yields no run ID
// must still be visible here rather than silently producing States.Runtime.
func TestExtractRunID(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]interface{}
		want     string
	}{
		{
			name:     "glue",
			metadata: map[string]interface{}{"glue_job_name": "my-etl", "glue_job_run_id": "jr_abc"},
			want:     "jr_abc",
		},
		{
			name:     "emr",
			metadata: map[string]interface{}{"emr_cluster_id": "j-123", "emr_step_id": "s-456"},
			want:     "s-456",
		},
		{
			name:     "emr-serverless",
			metadata: map[string]interface{}{"emr_sl_application_id": "app-1", "emr_sl_job_run_id": "run-9"},
			want:     "run-9",
		},
		{
			name:     "step-function",
			metadata: map[string]interface{}{"sfn_execution_arn": "arn:aws:states:us-east-1:1:execution:sm:e1"},
			want:     "arn:aws:states:us-east-1:1:execution:sm:e1",
		},
		{
			name: "airflow",
			metadata: map[string]interface{}{
				"airflow_dag_run_id": "manual__2026-03-01",
				"airflow_dag_id":     "dag-1",
				"airflow_url":        "https://airflow.example.com",
			},
			want: "manual__2026-03-01",
		},
		{
			name:     "databricks",
			metadata: map[string]interface{}{"databricks_workspace_url": "https://dbc", "databricks_run_id": "778899"},
			want:     "778899",
		},
		{
			name:     "command returns nil metadata",
			metadata: nil,
			want:     "",
		},
		{
			name:     "http returns nil metadata",
			metadata: nil,
			want:     "",
		},
		{
			name:     "lambda returns nil metadata",
			metadata: nil,
			want:     "",
		},
		{
			name:     "legacy generic runId key",
			metadata: map[string]interface{}{"runId": "abc-123"},
			want:     "abc-123",
		},
		{
			name:     "empty value falls through to the next key",
			metadata: map[string]interface{}{"runId": "", "glue_job_run_id": "jr-fallback"},
			want:     "jr-fallback",
		},
		{
			name:     "non-string value is ignored",
			metadata: map[string]interface{}{"glue_job_run_id": 42},
			want:     "",
		},
		{
			name:     "unrecognised shape yields empty run id",
			metadata: map[string]interface{}{"statusCode": float64(200), "responseBody": "OK"},
			want:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := orchestrator.ExtractRunID(tt.metadata); got != tt.want {
				t.Errorf("ExtractRunID() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestOrchestratorOutput_AlwaysMarshalsRunID guards the CheckJob and
// JobPollExhausted Parameters, which dereference $.triggerResult.runId.
// An omitted key raises States.Runtime after the external job already started.
func TestOrchestratorOutput_AlwaysMarshalsRunID(t *testing.T) {
	data, err := json.Marshal(lambda.OrchestratorOutput{Mode: "trigger"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := decoded["runId"]; !ok {
		t.Errorf("marshaled OrchestratorOutput = %s, want a runId key even when empty", data)
	}
}
