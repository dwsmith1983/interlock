package trigger

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
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

	cfg := &types.GlueTriggerConfig{
		JobName: "my-etl-job",
	}

	meta, err := ExecuteGlue(context.Background(), cfg, client)
	require.NoError(t, err)
	assert.Equal(t, "my-etl-job", meta["glue_job_name"])
	assert.Equal(t, "jr_abc123", meta["glue_job_run_id"])
}

func TestExecuteGlue_MissingJobName(t *testing.T) {
	client := &mockGlueClient{}
	cfg := &types.GlueTriggerConfig{}

	_, err := ExecuteGlue(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "jobName is required")
}

func TestExecuteGlue_APIError(t *testing.T) {
	client := &mockGlueClient{
		startErr: assert.AnError,
	}

	cfg := &types.GlueTriggerConfig{
		JobName: "my-job",
	}

	_, err := ExecuteGlue(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "StartJobRun failed")
}

func TestCheckGlueStatus(t *testing.T) {
	tests := []struct {
		name            string
		state           gluetypes.JobRunState
		expected        RunCheckState
		failureCategory types.FailureCategory
	}{
		{"succeeded", gluetypes.JobRunStateSucceeded, RunCheckSucceeded, ""},
		{"failed", gluetypes.JobRunStateFailed, RunCheckFailed, types.FailureTransient},
		{"timeout", gluetypes.JobRunStateTimeout, RunCheckFailed, types.FailureTimeout},
		{"stopped", gluetypes.JobRunStateStopped, RunCheckFailed, types.FailureTransient},
		{"error", gluetypes.JobRunStateError, RunCheckFailed, types.FailureTransient},
		{"running", gluetypes.JobRunStateRunning, RunCheckRunning, ""},
		{"starting", gluetypes.JobRunStateStarting, RunCheckRunning, ""},
		{"waiting", gluetypes.JobRunStateWaiting, RunCheckRunning, ""},
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
			cwClient := &mockCWLogsClient{
				filterOut: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
			}

			r := NewRunner(WithGlueClient(client), WithCloudWatchLogsClient(cwClient))
			result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
				"glue_job_name":   "my-job",
				"glue_job_run_id": "jr_123",
			})
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result.State)
			assert.Equal(t, string(tt.state), result.Message)
			assert.Equal(t, tt.failureCategory, result.FailureCategory)
		})
	}
}

func TestCheckGlueStatus_MissingMetadata(t *testing.T) {
	r := NewRunner()
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{})
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}

// --- CloudWatch RCA verification tests ---

type mockCWLogsClient struct {
	filterOut *cloudwatchlogs.FilterLogEventsOutput
	filterErr error
}

func (m *mockCWLogsClient) FilterLogEvents(ctx context.Context, params *cloudwatchlogs.FilterLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	return m.filterOut, m.filterErr
}

func TestCheckGlueStatus_RCADetectsFalseSuccess(t *testing.T) {
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		filterOut: &cloudwatchlogs.FilterLogEventsOutput{
			Events: []cwltypes.FilteredLogEvent{
				{Message: aws.String(`{"Event":"GlueExceptionAnalysisJobFailed","Failure Reason":"No space left on device","Job Result":"JobFailed"}`)},
			},
		},
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(cwClient))
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckFailed, result.State)
	assert.Contains(t, result.Message, "RCA")
	assert.Contains(t, result.Message, "No space left on device")
	assert.Equal(t, types.FailureTransient, result.FailureCategory)
}

func TestCheckGlueStatus_RCANoFailure(t *testing.T) {
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		filterOut: &cloudwatchlogs.FilterLogEventsOutput{
			Events: []cwltypes.FilteredLogEvent{},
		},
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(cwClient))
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
	assert.Equal(t, "SUCCEEDED", result.Message)
}

func TestCheckGlueStatus_RCAErrorFallsThrough(t *testing.T) {
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		filterErr: fmt.Errorf("access denied"),
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(cwClient))
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State, "should fall through to SUCCEEDED when RCA check fails")
}

func TestCheckGlueStatus_NoCWLogsClientFallsThrough(t *testing.T) {
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}

	// No CW Logs client injected, and no AWS config available → graceful fallthrough
	r := NewRunner(WithGlueClient(glueClient))
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
}

func TestCheckGlueStatus_RCAUsesJobRunLogGroup(t *testing.T) {
	customLogGroup := "/aws-glue/jobs/custom-group"
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState:  gluetypes.JobRunStateSucceeded,
				LogGroupName: &customLogGroup,
			},
		},
	}

	var capturedLogGroup string
	cwClient := &mockCWLogsClient{
		filterOut: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
	}
	// Wrap to capture the log group parameter
	captureClient := &capturingCWLogsClient{
		delegate: cwClient,
		onFilter: func(input *cloudwatchlogs.FilterLogEventsInput) {
			capturedLogGroup = aws.ToString(input.LogGroupName)
		},
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(captureClient))
	_, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, customLogGroup, capturedLogGroup)
}

type capturingCWLogsClient struct {
	delegate CloudWatchLogsAPI
	onFilter func(*cloudwatchlogs.FilterLogEventsInput)
}

func (c *capturingCWLogsClient) FilterLogEvents(ctx context.Context, params *cloudwatchlogs.FilterLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	if c.onFilter != nil {
		c.onFilter(params)
	}
	return c.delegate.FilterLogEvents(ctx, params, optFns...)
}

func TestExtractGlueFailureReason(t *testing.T) {
	tests := []struct {
		name     string
		msg      string
		expected string
	}{
		{
			name:     "standard RCA message",
			msg:      `{"Event":"GlueExceptionAnalysisJobFailed","Failure Reason":"No space left on device","Job Result":"JobFailed"}`,
			expected: "No space left on device",
		},
		{
			name:     "no failure reason field",
			msg:      `{"Event":"GlueExceptionAnalysisJobFailed"}`,
			expected: "",
		},
		{
			name:     "empty message",
			msg:      "",
			expected: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, extractGlueFailureReason(tt.msg))
		})
	}
}
