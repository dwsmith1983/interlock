package trigger

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/interlock-systems/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEMRServerlessClient struct {
	startOut *emrserverless.StartJobRunOutput
	startErr error
	getOut   *emrserverless.GetJobRunOutput
	getErr   error
}

func (m *mockEMRServerlessClient) StartJobRun(ctx context.Context, params *emrserverless.StartJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.StartJobRunOutput, error) {
	return m.startOut, m.startErr
}

func (m *mockEMRServerlessClient) GetJobRun(ctx context.Context, params *emrserverless.GetJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.GetJobRunOutput, error) {
	return m.getOut, m.getErr
}

func TestExecuteEMRServerless_Success(t *testing.T) {
	runID := "run-abc"
	client := &mockEMRServerlessClient{
		startOut: &emrserverless.StartJobRunOutput{JobRunId: &runID},
	}

	cfg := &types.TriggerConfig{
		Type:          types.TriggerEMRServerless,
		ApplicationID: "app-123",
		JobName:       "my-spark-job",
	}

	meta, err := ExecuteEMRServerless(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "app-123", meta["emr_sl_application_id"])
	assert.Equal(t, "run-abc", meta["emr_sl_job_run_id"])
}

func TestExecuteEMRServerless_MissingApplicationID(t *testing.T) {
	client := &mockEMRServerlessClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerEMRServerless, JobName: "job"}
	_, err := ExecuteEMRServerless(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "applicationId is required")
}

func TestExecuteEMRServerless_MissingJobName(t *testing.T) {
	client := &mockEMRServerlessClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerEMRServerless, ApplicationID: "app-1"}
	_, err := ExecuteEMRServerless(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jobName is required")
}

func TestExecuteEMRServerless_APIError(t *testing.T) {
	client := &mockEMRServerlessClient{startErr: assert.AnError}
	cfg := &types.TriggerConfig{
		Type:          types.TriggerEMRServerless,
		ApplicationID: "app-1",
		JobName:       "job",
	}
	_, err := ExecuteEMRServerless(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "StartJobRun failed")
}

func TestCheckEMRServerlessStatus(t *testing.T) {
	tests := []struct {
		name     string
		state    emrtypes.JobRunState
		expected RunCheckState
	}{
		{"success", emrtypes.JobRunStateSuccess, RunCheckSucceeded},
		{"failed", emrtypes.JobRunStateFailed, RunCheckFailed},
		{"cancelled", emrtypes.JobRunStateCancelled, RunCheckFailed},
		{"running", emrtypes.JobRunStateRunning, RunCheckRunning},
		{"pending", emrtypes.JobRunStatePending, RunCheckRunning},
		{"submitted", emrtypes.JobRunStateSubmitted, RunCheckRunning},
		{"scheduled", emrtypes.JobRunStateScheduled, RunCheckRunning},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockEMRServerlessClient{
				getOut: &emrserverless.GetJobRunOutput{
					JobRun: &emrtypes.JobRun{
						State: tt.state,
					},
				},
			}

			r := NewRunner(WithEMRServerlessClient(client))
			result, err := r.checkEMRServerlessStatus(context.Background(), map[string]interface{}{
				"emr_sl_application_id": "app-1",
				"emr_sl_job_run_id":     "run-1",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
			assert.Equal(t, string(tt.state), result.Message)
		})
	}
}

func TestCheckEMRServerlessStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkEMRServerlessStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
