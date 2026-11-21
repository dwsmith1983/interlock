package trigger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockEMRServerlessClient struct {
	startOut *emrserverless.StartJobRunOutput
	startErr error
	getOut   *emrserverless.GetJobRunOutput
	getErr   error
	started  []*emrserverless.StartJobRunInput
}

func (m *mockEMRServerlessClient) StartJobRun(ctx context.Context, params *emrserverless.StartJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.StartJobRunOutput, error) {
	m.started = append(m.started, params)
	return m.startOut, m.startErr
}

func (m *mockEMRServerlessClient) GetJobRun(ctx context.Context, params *emrserverless.GetJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.GetJobRunOutput, error) {
	return m.getOut, m.getErr
}

// emrServerlessConfig is a complete config whose run has the given run ID.
func emrServerlessConfig(runID string) *types.EMRServerlessTriggerConfig {
	return &types.EMRServerlessTriggerConfig{
		ApplicationID:    "app-123",
		JobName:          "my-spark-job",
		ExecutionRoleARN: "arn:aws:iam::123456789012:role/emr-job",
		EntryPoint:       "s3://bucket/etl.py",
		Run:              types.RunIdentity{PipelineID: "p", ScheduleID: "s", Date: "2026-03-03", RunID: runID},
	}
}

var clientTokenRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// TestExecuteEMRServerless_StartsJobRun: the job run gets the runtime role and
// a spark-submit driver, and its client token comes from the run ID, so a
// retried start of the same run reuses the token while another run gets its
// own.
func TestExecuteEMRServerless_StartsJobRun(t *testing.T) {
	jobRunID := "run-abc"
	client := &mockEMRServerlessClient{startOut: &emrserverless.StartJobRunOutput{JobRunId: &jobRunID}}
	// Execution names run up to 80 characters, more than a client token allows.
	runID := "pipeline-with-a-long-name-hourly-2026-03-03T07-rerun12-1772521200-r3" + strings.Repeat("x", 12)
	cfg := emrServerlessConfig(runID)
	cfg.EntryPointArguments = []string{"--env", "prod", "--par_day", "20260303"}
	cfg.SparkSubmitParameters = "--conf spark.executor.cores=2"

	meta, err := ExecuteEMRServerless(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "app-123", meta["emr_sl_application_id"])
	assert.Equal(t, "run-abc", meta["emr_sl_job_run_id"])
	_, err = ExecuteEMRServerless(context.Background(), cfg, client)
	require.NoError(t, err)
	_, err = ExecuteEMRServerless(context.Background(), emrServerlessConfig(runID+"-r1"), client)
	require.NoError(t, err)

	require.Len(t, client.started, 3)
	in := client.started[0]
	assert.Equal(t, "app-123", aws.ToString(in.ApplicationId))
	assert.Equal(t, "my-spark-job", aws.ToString(in.Name))
	assert.Equal(t, "arn:aws:iam::123456789012:role/emr-job", aws.ToString(in.ExecutionRoleArn))
	driver, ok := in.JobDriver.(*emrtypes.JobDriverMemberSparkSubmit)
	require.True(t, ok, "job driver = %T, want spark-submit", in.JobDriver)
	assert.Equal(t, "s3://bucket/etl.py", aws.ToString(driver.Value.EntryPoint))
	assert.Equal(t, []string{"--env", "prod", "--par_day", "20260303"}, driver.Value.EntryPointArguments)
	assert.Equal(t, "--conf spark.executor.cores=2", aws.ToString(driver.Value.SparkSubmitParameters))

	sum := sha256.Sum256([]byte(runID))
	token := aws.ToString(in.ClientToken)
	assert.Equal(t, hex.EncodeToString(sum[:]), token)
	assert.Len(t, token, 64)
	assert.Regexp(t, clientTokenRE, token)
	assert.Equal(t, token, aws.ToString(client.started[1].ClientToken), "a retried start must reuse the token")
	assert.NotEqual(t, token, aws.ToString(client.started[2].ClientToken), "another run must get its own token")
}

// An unset SparkSubmitParameters is left out: the API rejects an empty string.
func TestExecuteEMRServerless_OmitsEmptySparkSubmitParameters(t *testing.T) {
	jobRunID := "run-abc"
	client := &mockEMRServerlessClient{startOut: &emrserverless.StartJobRunOutput{JobRunId: &jobRunID}}
	_, err := ExecuteEMRServerless(context.Background(), emrServerlessConfig("exec-1"), client)
	require.NoError(t, err)
	require.Len(t, client.started, 1)
	driver, ok := client.started[0].JobDriver.(*emrtypes.JobDriverMemberSparkSubmit)
	require.True(t, ok, "job driver = %T, want spark-submit", client.started[0].JobDriver)
	assert.Nil(t, driver.Value.SparkSubmitParameters)
}

// A config missing a required field, or a launch with no run ID to make the
// client token from, fails before any StartJobRun call.
func TestExecuteEMRServerless_RejectsIncompleteConfig(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*types.EMRServerlessTriggerConfig)
		wantErr string
	}{
		{"application ID", func(c *types.EMRServerlessTriggerConfig) { c.ApplicationID = "" }, "applicationId is required"},
		{"job name", func(c *types.EMRServerlessTriggerConfig) { c.JobName = "" }, "jobName is required"},
		{"execution role", func(c *types.EMRServerlessTriggerConfig) { c.ExecutionRoleARN = "" }, "executionRoleArn is required"},
		{"entry point", func(c *types.EMRServerlessTriggerConfig) { c.EntryPoint = "" }, "entryPoint is required"},
		{"run ID", func(c *types.EMRServerlessTriggerConfig) { c.Run.RunID = "" }, "run ID is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockEMRServerlessClient{}
			cfg := emrServerlessConfig("exec-1")
			tt.mutate(cfg)
			_, err := ExecuteEMRServerless(context.Background(), cfg, client)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.Empty(t, client.started, "StartJobRun must not be called")
		})
	}
}

func TestExecuteEMRServerless_APIError(t *testing.T) {
	client := &mockEMRServerlessClient{startErr: assert.AnError}
	_, err := ExecuteEMRServerless(context.Background(), emrServerlessConfig("exec-1"), client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "StartJobRun failed")
}

func TestCheckEMRServerlessStatus(t *testing.T) {
	tests := []struct {
		name            string
		state           emrtypes.JobRunState
		expected        RunCheckState
		failureCategory types.FailureCategory
	}{
		{"success", emrtypes.JobRunStateSuccess, RunCheckSucceeded, ""},
		{"failed", emrtypes.JobRunStateFailed, RunCheckFailed, types.FailureTransient},
		{"cancelled", emrtypes.JobRunStateCancelled, RunCheckFailed, types.FailureTransient},
		{"running", emrtypes.JobRunStateRunning, RunCheckRunning, ""},
		{"pending", emrtypes.JobRunStatePending, RunCheckRunning, ""},
		{"submitted", emrtypes.JobRunStateSubmitted, RunCheckRunning, ""},
		{"scheduled", emrtypes.JobRunStateScheduled, RunCheckRunning, ""},
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
			assert.Equal(t, tt.failureCategory, result.FailureCategory)
		})
	}
}

func TestCheckEMRServerlessStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkEMRServerlessStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

func TestCheckEMRServerlessStatus_APIError(t *testing.T) {
	client := &mockEMRServerlessClient{getErr: assert.AnError}
	r := NewRunner(WithEMRServerlessClient(client))
	_, err := r.checkEMRServerlessStatus(context.Background(), map[string]interface{}{
		"emr_sl_application_id": "app-1",
		"emr_sl_job_run_id":     "run-1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "GetJobRun failed")
}

func TestCheckEMRServerlessStatus_NilJobRun(t *testing.T) {
	client := &mockEMRServerlessClient{
		getOut: &emrserverless.GetJobRunOutput{JobRun: nil},
	}
	r := NewRunner(WithEMRServerlessClient(client))
	_, err := r.checkEMRServerlessStatus(context.Background(), map[string]interface{}{
		"emr_sl_application_id": "app-1",
		"emr_sl_job_run_id":     "run-1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "returned nil JobRun")
}

func TestCheckEMRServerlessStatus_MissingAppID(t *testing.T) {
	r := NewRunner()
	result, err := r.checkEMRServerlessStatus(context.Background(), map[string]interface{}{
		"emr_sl_job_run_id": "run-1",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
	assert.Equal(t, "missing emr-serverless metadata", result.Message)
}
