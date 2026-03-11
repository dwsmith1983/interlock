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
	// streamResponses maps a log stream name to a specific response.
	// When set, FilterLogEvents returns the response matching the first
	// stream name in the request; it falls back to filterOut/filterErr
	// for unmatched streams.
	streamResponses map[string]mockCWLogsResponse
}

type mockCWLogsResponse struct {
	out *cloudwatchlogs.FilterLogEventsOutput
	err error
}

func (m *mockCWLogsClient) FilterLogEvents(ctx context.Context, params *cloudwatchlogs.FilterLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	if m.streamResponses != nil && len(params.LogStreamNames) > 0 {
		if resp, ok := m.streamResponses[params.LogStreamNames[0]]; ok {
			return resp.out, resp.err
		}
	}
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

	var capturedLogGroups []string
	cwClient := &mockCWLogsClient{
		filterOut: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
	}
	captureClient := &capturingCWLogsClient{
		delegate: cwClient,
		onFilter: func(input *cloudwatchlogs.FilterLogEventsInput) {
			capturedLogGroups = append(capturedLogGroups, aws.ToString(input.LogGroupName))
		},
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(captureClient))
	_, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	require.Len(t, capturedLogGroups, 2, "both RCA and driver output checks should use custom log group")
	assert.Equal(t, customLogGroup, capturedLogGroups[0], "RCA check should use custom log group")
	assert.Equal(t, customLogGroup, capturedLogGroups[1], "driver output check should use custom log group")
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

func TestCheckGlueStatus_RCAAndDriverOutputChecks(t *testing.T) {
	// Verify that both the RCA check and driver output check are performed,
	// but NOT the error log group check (removed in PR #60 due to 100%
	// false positive rate from benign JVM startup stderr).
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}

	var capturedInputs []*cloudwatchlogs.FilterLogEventsInput
	cwClient := &capturingCWLogsClient{
		delegate: &mockCWLogsClient{
			filterOut: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
		},
		onFilter: func(input *cloudwatchlogs.FilterLogEventsInput) {
			capturedInputs = append(capturedInputs, input)
		},
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(cwClient))
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)

	// Two FilterLogEvents calls: RCA check + driver output check.
	require.Len(t, capturedInputs, 2)

	// First call: RCA stream check.
	assert.Equal(t, "/aws-glue/jobs/logs-v2", aws.ToString(capturedInputs[0].LogGroupName))
	assert.Equal(t, []string{"jr_abc123-job-insights-rca-driver"}, capturedInputs[0].LogStreamNames)
	assert.Contains(t, aws.ToString(capturedInputs[0].FilterPattern), "GlueExceptionAnalysisJobFailed")

	// Second call: driver output stream check.
	assert.Equal(t, "/aws-glue/jobs/logs-v2", aws.ToString(capturedInputs[1].LogGroupName))
	assert.Equal(t, []string{"jr_abc123"}, capturedInputs[1].LogStreamNames)
	assert.Contains(t, aws.ToString(capturedInputs[1].FilterPattern), `" ERROR "`)
	assert.Contains(t, aws.ToString(capturedInputs[1].FilterPattern), `" FATAL "`)
}

func TestCheckGlueStatus_DriverLogDetectsError(t *testing.T) {
	// Glue reports SUCCEEDED, RCA stream is empty, but driver output stream
	// contains an ERROR log4j line → should detect as FAILED.
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		streamResponses: map[string]mockCWLogsResponse{
			"jr_abc123-job-insights-rca-driver": {
				out: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
			},
			"jr_abc123": {
				out: &cloudwatchlogs.FilterLogEventsOutput{
					Events: []cwltypes.FilteredLogEvent{
						{Message: aws.String("2026-01-15 10:00:00,000 ERROR org.apache.spark.sql.FileFormatWriter: Aborting job abc123. java.io.IOException: No space left on device")},
					},
				},
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
	assert.Contains(t, result.Message, "driver log")
	assert.Equal(t, types.FailureTransient, result.FailureCategory)
}

func TestCheckGlueStatus_DriverLogDetectsFatal(t *testing.T) {
	// Glue reports SUCCEEDED, RCA empty, driver output has a FATAL log4j
	// line → should detect as FAILED.
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		streamResponses: map[string]mockCWLogsResponse{
			"jr_abc123-job-insights-rca-driver": {
				out: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
			},
			"jr_abc123": {
				out: &cloudwatchlogs.FilterLogEventsOutput{
					Events: []cwltypes.FilteredLogEvent{
						{Message: aws.String("2026-01-15 10:00:00,000 FATAL org.apache.spark.util.SparkUncaughtExceptionHandler: Uncaught exception")},
					},
				},
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
	assert.Contains(t, result.Message, "driver log")
	assert.Contains(t, result.Message, "FATAL")
	assert.Equal(t, types.FailureTransient, result.FailureCategory)
}

func TestCheckGlueStatus_DriverLogClean(t *testing.T) {
	// Glue SUCCEEDED, RCA empty, driver log empty → SUCCEEDED (no false positive).
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		streamResponses: map[string]mockCWLogsResponse{
			"jr_abc123-job-insights-rca-driver": {
				out: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
			},
			"jr_abc123": {
				out: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
			},
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

func TestCheckGlueStatus_DriverLogErrorFallsThrough(t *testing.T) {
	// Glue SUCCEEDED, RCA empty, driver log query returns error →
	// should fall through to SUCCEEDED (graceful degradation).
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState: gluetypes.JobRunStateSucceeded,
			},
		},
	}
	cwClient := &mockCWLogsClient{
		streamResponses: map[string]mockCWLogsResponse{
			"jr_abc123-job-insights-rca-driver": {
				out: &cloudwatchlogs.FilterLogEventsOutput{Events: []cwltypes.FilteredLogEvent{}},
			},
			"jr_abc123": {
				err: fmt.Errorf("ResourceNotFoundException: log stream not found"),
			},
		},
	}

	r := NewRunner(WithGlueClient(glueClient), WithCloudWatchLogsClient(cwClient))
	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})
	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State, "should fall through to SUCCEEDED when driver log check fails")
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
