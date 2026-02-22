package trigger

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockSFNClient struct {
	startOut    *sfn.StartExecutionOutput
	startErr    error
	describeOut *sfn.DescribeExecutionOutput
	describeErr error
}

func (m *mockSFNClient) StartExecution(ctx context.Context, params *sfn.StartExecutionInput, optFns ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	return m.startOut, m.startErr
}

func (m *mockSFNClient) DescribeExecution(ctx context.Context, params *sfn.DescribeExecutionInput, optFns ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error) {
	return m.describeOut, m.describeErr
}

func TestExecuteSFN_Success(t *testing.T) {
	arn := "arn:aws:states:us-east-1:123456789:execution:my-sm:exec-1"
	client := &mockSFNClient{
		startOut: &sfn.StartExecutionOutput{ExecutionArn: &arn},
	}

	cfg := &types.TriggerConfig{
		Type:            types.TriggerStepFunction,
		StateMachineARN: "arn:aws:states:us-east-1:123456789:stateMachine:my-sm",
	}

	meta, err := ExecuteSFN(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, arn, meta["sfn_execution_arn"])
}

func TestExecuteSFN_WithArguments(t *testing.T) {
	arn := "arn:aws:states:us-east-1:123:execution:sm:exec-2"
	client := &mockSFNClient{
		startOut: &sfn.StartExecutionOutput{ExecutionArn: &arn},
	}

	cfg := &types.TriggerConfig{
		Type:            types.TriggerStepFunction,
		StateMachineARN: "arn:aws:states:us-east-1:123:stateMachine:sm",
		Arguments:       map[string]string{"key": "value"},
	}

	meta, err := ExecuteSFN(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, arn, meta["sfn_execution_arn"])
}

func TestExecuteSFN_MissingARN(t *testing.T) {
	client := &mockSFNClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerStepFunction}
	_, err := ExecuteSFN(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "stateMachineArn is required")
}

func TestExecuteSFN_APIError(t *testing.T) {
	client := &mockSFNClient{startErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:            types.TriggerStepFunction,
		StateMachineARN: "arn:aws:states:us-east-1:123:stateMachine:sm",
	}
	_, err := ExecuteSFN(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "StartExecution failed")
}

func TestCheckSFNStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   sfntypes.ExecutionStatus
		expected RunCheckState
	}{
		{"succeeded", sfntypes.ExecutionStatusSucceeded, RunCheckSucceeded},
		{"failed", sfntypes.ExecutionStatusFailed, RunCheckFailed},
		{"timed_out", sfntypes.ExecutionStatusTimedOut, RunCheckFailed},
		{"aborted", sfntypes.ExecutionStatusAborted, RunCheckFailed},
		{"running", sfntypes.ExecutionStatusRunning, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockSFNClient{
				describeOut: &sfn.DescribeExecutionOutput{
					Status: tt.status,
				},
			}

			r := NewRunner(WithSFNClient(client))
			result, err := r.checkSFNStatus(context.Background(), map[string]interface{}{
				"sfn_execution_arn": "arn:aws:states:us-east-1:123:execution:sm:exec-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
			assert.Equal(t, string(tt.status), result.Message)
		})
	}
}

func TestCheckSFNStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkSFNStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
