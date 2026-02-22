package trigger

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunner_Execute_UnknownType(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), &types.TriggerConfig{
		Type: "unknown-type",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown trigger type")
}

func TestRunner_Execute_NilConfig(t *testing.T) {
	r := NewRunner()
	_, err := r.Execute(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no trigger configured")
}

func TestRunner_CheckStatus_NonPollingTypes(t *testing.T) {
	r := NewRunner()

	for _, tt := range []types.TriggerType{types.TriggerCommand, types.TriggerHTTP} {
		result, err := r.CheckStatus(context.Background(), tt, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, RunCheckRunning, result.State, "trigger type %s", tt)
	}
}

func TestRunner_CheckStatus_UnknownType(t *testing.T) {
	r := NewRunner()
	_, err := r.CheckStatus(context.Background(), "bogus", nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown trigger type for status check")
}

func TestRunner_CheckStatus_GlueDispatch(t *testing.T) {
	client := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	r := NewRunner(WithGlueClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerGlue, map[string]interface{}{
		"glue_job_name":   "job",
		"glue_job_run_id": "run",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
}

func TestRunner_CheckStatus_EMRDispatch(t *testing.T) {
	client := &mockEMRClient{
		describeOut: &emr.DescribeStepOutput{
			Step: &emrtypes.Step{
				Status: &emrtypes.StepStatus{
					State: emrtypes.StepStateFailed,
				},
			},
		},
	}
	r := NewRunner(WithEMRClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerEMR, map[string]interface{}{
		"emr_cluster_id": "j-1",
		"emr_step_id":    "s-1",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckFailed, result.State)
}

func TestRunner_CheckStatus_SFNDispatch(t *testing.T) {
	client := &mockSFNClient{
		describeOut: &sfn.DescribeExecutionOutput{
			Status: sfntypes.ExecutionStatusRunning,
		},
	}
	r := NewRunner(WithSFNClient(client))

	result, err := r.CheckStatus(context.Background(), types.TriggerStepFunction, map[string]interface{}{
		"sfn_execution_arn": "arn:exec",
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

func TestPackageLevelExecute_NilConfig(t *testing.T) {
	_, err := Execute(context.Background(), nil)
	assert.Error(t, err)
}

func TestPackageLevelCheckStatus_NonPolling(t *testing.T) {
	result, err := CheckStatus(context.Background(), types.TriggerCommand, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
