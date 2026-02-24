package trigger

import (
	"context"
	"testing"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockCloudWorkflowsClient struct {
	createOut *executionspb.Execution
	createErr error
	getOut    *executionspb.Execution
	getErr    error
}

func (m *mockCloudWorkflowsClient) CreateExecution(_ context.Context, _ *executionspb.CreateExecutionRequest, _ ...interface{}) (*executionspb.Execution, error) {
	return m.createOut, m.createErr
}

func (m *mockCloudWorkflowsClient) GetExecution(_ context.Context, _ *executionspb.GetExecutionRequest, _ ...interface{}) (*executionspb.Execution, error) {
	return m.getOut, m.getErr
}

func TestExecuteCloudWorkflows_Success(t *testing.T) {
	client := &mockCloudWorkflowsClient{
		createOut: &executionspb.Execution{
			Name: "projects/p/locations/r/workflows/w/executions/exec-123",
		},
	}

	cfg := &types.TriggerConfig{
		Type:       types.TriggerCloudWorkflows,
		ProjectID:  "my-project",
		Region:     "us-central1",
		WorkflowID: "my-workflow",
	}

	meta, err := ExecuteCloudWorkflows(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "projects/p/locations/r/workflows/w/executions/exec-123", meta["cloud_workflows_execution_name"])
}

func TestExecuteCloudWorkflows_MissingProjectID(t *testing.T) {
	client := &mockCloudWorkflowsClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerCloudWorkflows, Region: "r", WorkflowID: "w"}
	_, err := ExecuteCloudWorkflows(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "projectId is required")
}

func TestExecuteCloudWorkflows_MissingRegion(t *testing.T) {
	client := &mockCloudWorkflowsClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerCloudWorkflows, ProjectID: "p", WorkflowID: "w"}
	_, err := ExecuteCloudWorkflows(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "region is required")
}

func TestExecuteCloudWorkflows_MissingWorkflowID(t *testing.T) {
	client := &mockCloudWorkflowsClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerCloudWorkflows, ProjectID: "p", Region: "r"}
	_, err := ExecuteCloudWorkflows(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "workflowId is required")
}

func TestExecuteCloudWorkflows_APIError(t *testing.T) {
	client := &mockCloudWorkflowsClient{createErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:       types.TriggerCloudWorkflows,
		ProjectID:  "p",
		Region:     "r",
		WorkflowID: "w",
	}
	_, err := ExecuteCloudWorkflows(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CreateExecution failed")
}

func TestCheckCloudWorkflowsStatus(t *testing.T) {
	tests := []struct {
		name     string
		state    executionspb.Execution_State
		expected RunCheckState
	}{
		{"succeeded", executionspb.Execution_SUCCEEDED, RunCheckSucceeded},
		{"failed", executionspb.Execution_FAILED, RunCheckFailed},
		{"cancelled", executionspb.Execution_CANCELLED, RunCheckFailed},
		{"active", executionspb.Execution_ACTIVE, RunCheckRunning},
		{"queued", executionspb.Execution_QUEUED, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockCloudWorkflowsClient{
				getOut: &executionspb.Execution{State: tt.state},
			}

			r := NewRunner(WithCloudWorkflowsClient(client))
			result, err := r.checkCloudWorkflowsStatus(context.Background(), map[string]interface{}{
				"cloud_workflows_execution_name": "projects/p/locations/r/workflows/w/executions/e-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
		})
	}
}

func TestCheckCloudWorkflowsStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkCloudWorkflowsStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
