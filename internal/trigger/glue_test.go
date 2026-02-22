package trigger

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockGlueClient struct {
	startOut *glue.StartJobRunOutput
	startErr error
	getOut   *glue.GetJobRunOutput
	getErr   error
}

func (m *mockGlueClient) StartJobRun(ctx context.Context, params *glue.StartJobRunInput, optFns ...func(*glue.Options)) (*glue.StartJobRunOutput, error) {
	return m.startOut, m.startErr
}

func (m *mockGlueClient) GetJobRun(ctx context.Context, params *glue.GetJobRunInput, optFns ...func(*glue.Options)) (*glue.GetJobRunOutput, error) {
	return m.getOut, m.getErr
}

func TestExecuteGlue_Success(t *testing.T) {
	runID := "jr_abc123"
	client := &mockGlueClient{
		startOut: &glue.StartJobRunOutput{JobRunId: &runID},
	}

	cfg := &types.TriggerConfig{
		Type:    types.TriggerGlue,
		JobName: "my-etl-job",
	}

	meta, err := ExecuteGlue(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "my-etl-job", meta["glue_job_name"])
	assert.Equal(t, "jr_abc123", meta["glue_job_run_id"])
}

func TestExecuteGlue_MissingJobName(t *testing.T) {
	client := &mockGlueClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerGlue}

	_, err := ExecuteGlue(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jobName is required")
}

func TestExecuteGlue_APIError(t *testing.T) {
	client := &mockGlueClient{
		startErr: assert.AnError,
	}

	cfg := &types.TriggerConfig{
		Type:    types.TriggerGlue,
		JobName: "my-job",
	}

	_, err := ExecuteGlue(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "StartJobRun failed")
}

func TestCheckGlueStatus(t *testing.T) {
	tests := []struct {
		name     string
		state    gluetypes.JobRunState
		expected RunCheckState
	}{
		{"succeeded", gluetypes.JobRunStateSucceeded, RunCheckSucceeded},
		{"failed", gluetypes.JobRunStateFailed, RunCheckFailed},
		{"timeout", gluetypes.JobRunStateTimeout, RunCheckFailed},
		{"stopped", gluetypes.JobRunStateStopped, RunCheckFailed},
		{"error", gluetypes.JobRunStateError, RunCheckFailed},
		{"running", gluetypes.JobRunStateRunning, RunCheckRunning},
		{"starting", gluetypes.JobRunStateStarting, RunCheckRunning},
		{"waiting", gluetypes.JobRunStateWaiting, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockGlueClient{
				getOut: &glue.GetJobRunOutput{
					JobRun: &gluetypes.JobRun{
						JobRunState: tt.state,
					},
				},
			}

			r := NewRunner(WithGlueClient(client))
			result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
				"glue_job_name":   "my-job",
				"glue_job_run_id": "jr_123",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
			assert.Equal(t, string(tt.state), result.Message)
		})
	}
}

func TestCheckGlueStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
