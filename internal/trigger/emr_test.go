package trigger

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEMRClient struct {
	addOut      *emr.AddJobFlowStepsOutput
	addErr      error
	describeOut *emr.DescribeStepOutput
	describeErr error
}

func (m *mockEMRClient) AddJobFlowSteps(ctx context.Context, params *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error) {
	return m.addOut, m.addErr
}

func (m *mockEMRClient) DescribeStep(ctx context.Context, params *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error) {
	return m.describeOut, m.describeErr
}

func TestExecuteEMR_Success(t *testing.T) {
	client := &mockEMRClient{
		addOut: &emr.AddJobFlowStepsOutput{StepIds: []string{"s-ABC123"}},
	}

	cfg := &types.TriggerConfig{
		Type:      types.TriggerEMR,
		ClusterID: "j-CLUSTER1",
		JobName:   "my-step",
		Command:   "s3://bucket/my-jar.jar",
	}

	meta, err := ExecuteEMR(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "j-CLUSTER1", meta["emr_cluster_id"])
	assert.Equal(t, "s-ABC123", meta["emr_step_id"])
}

func TestExecuteEMR_MissingClusterID(t *testing.T) {
	client := &mockEMRClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerEMR, JobName: "step", Command: "jar"}
	_, err := ExecuteEMR(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "clusterId is required")
}

func TestExecuteEMR_MissingJobName(t *testing.T) {
	client := &mockEMRClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerEMR, ClusterID: "j-1", Command: "jar"}
	_, err := ExecuteEMR(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jobName is required")
}

func TestExecuteEMR_MissingCommand(t *testing.T) {
	client := &mockEMRClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerEMR, ClusterID: "j-1", JobName: "step"}
	_, err := ExecuteEMR(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "command is required")
}

func TestExecuteEMR_APIError(t *testing.T) {
	client := &mockEMRClient{addErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:      types.TriggerEMR,
		ClusterID: "j-1",
		JobName:   "step",
		Command:   "jar",
	}
	_, err := ExecuteEMR(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "AddJobFlowSteps failed")
}

func TestCheckEMRStatus(t *testing.T) {
	tests := []struct {
		name            string
		state           emrtypes.StepState
		expected        RunCheckState
		failureCategory types.FailureCategory
	}{
		{"completed", emrtypes.StepStateCompleted, RunCheckSucceeded, ""},
		{"failed", emrtypes.StepStateFailed, RunCheckFailed, types.FailureTransient},
		{"cancelled", emrtypes.StepStateCancelled, RunCheckFailed, types.FailureTransient},
		{"interrupted", emrtypes.StepStateInterrupted, RunCheckFailed, types.FailureTransient},
		{"running", emrtypes.StepStateRunning, RunCheckRunning, ""},
		{"pending", emrtypes.StepStatePending, RunCheckRunning, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockEMRClient{
				describeOut: &emr.DescribeStepOutput{
					Step: &emrtypes.Step{
						Status: &emrtypes.StepStatus{
							State: tt.state,
						},
					},
				},
			}

			r := NewRunner(WithEMRClient(client))
			result, err := r.checkEMRStatus(context.Background(), map[string]interface{}{
				"emr_cluster_id": "j-1",
				"emr_step_id":    "s-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
			assert.Equal(t, string(tt.state), result.Message)
			assert.Equal(t, tt.failureCategory, result.FailureCategory)
		})
	}
}

func TestCheckEMRStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkEMRStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
