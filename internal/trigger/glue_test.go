package trigger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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
				{Message: aws.String(glueRCAMessage(`"No space left on device"`))},
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

func TestCheckGlueStatus_RCAReadsInsightsStreamInLogsV2(t *testing.T) {
	// Whatever log group GetJobRun reports, Glue writes the job insights
	// stream to /aws-glue/jobs/logs-v2.
	tests := []struct {
		name         string
		logGroupName string
	}{
		{"default jobs prefix", "/aws-glue/jobs"},
		{"security configuration prefix", "/aws-glue/jobs-etl-role-secconf/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured []*cloudwatchlogs.FilterLogEventsInput
			cw := &capturingCWLogsClient{
				delegate: &mockCWLogsClient{filterOut: &cloudwatchlogs.FilterLogEventsOutput{}},
				onFilter: func(in *cloudwatchlogs.FilterLogEventsInput) { captured = append(captured, in) },
			}
			r, buf := glueSucceededRunner(aws.String(tt.logGroupName), cw)

			result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
				"glue_job_name":   "my-job",
				"glue_job_run_id": "jr_abc123",
			})

			require.NoError(t, err)
			assert.Equal(t, RunCheckSucceeded, result.State)
			require.Len(t, captured, 1, "only the job insights stream is read")
			assert.Equal(t, "/aws-glue/jobs/logs-v2", aws.ToString(captured[0].LogGroupName))
			assert.Equal(t, []string{"jr_abc123-job-insights-rca-driver"}, captured[0].LogStreamNames)
			assert.Equal(t, "GlueExceptionAnalysisJobFailed", aws.ToString(captured[0].FilterPattern))
			assert.Empty(t, buf.String(), "a clean success logs nothing")
		})
	}
}

// glueRCAMessage returns a job insights failure event as Glue logs it: a
// log4j prefix, then one JSON object whose "Failure Reason" is the JSON value
// reasonJSON.
func glueRCAMessage(reasonJSON string) string {
	return `12:34:56 ERROR GlueExceptionAnalysisListener: [Glue Exception Analysis] ` +
		`{"Event":"GlueExceptionAnalysisJobFailed","Timestamp":1763700000000,"Failure Reason":` + reasonJSON +
		`,"Failed Stage Id":3,"Job Id":2,"Job Result":"JobFailed",` +
		`"Stack Trace":["org.apache.spark.SparkException: Job aborted","at org.apache.spark.scheduler.DAGScheduler.run"]}`
}

func TestCheckGlueStatus_RCAReason(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		want string
	}{
		{"reason", glueRCAMessage(`"No space left on device"`), "No space left on device"},
		{"escaped quote survives whole", glueRCAMessage(`"Path \"s3://bucket/key\" does not exist"`), `Path "s3://bucket/key" does not exist`},
		{"escaped control character removed", glueRCAMessage(`"No space\u001b[31m left on device"`), "No space[31m left on device"},
		{"cut to the error body limit", glueRCAMessage(`"` + strings.Repeat("a", 2*maxErrorBodyBytes) + `"`), strings.Repeat("a", maxErrorBodyBytes)},
		{"not JSON", "12:34:56 ERROR GlueExceptionAnalysisListener: [Glue Exception Analysis] Event: GlueExceptionAnalysisJobFailed Failure Reason: disk full", "JobFailed"},
		{"broken JSON", `12:34:56 ERROR GlueExceptionAnalysisListener: [Glue Exception Analysis] {"Failure Reason":"disk`, "JobFailed"},
		{"no reason", `12:34:56 ERROR GlueExceptionAnalysisListener: [Glue Exception Analysis] {"Event":"GlueExceptionAnalysisJobFailed"}`, "JobFailed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cw := &mockCWLogsClient{filterOut: &cloudwatchlogs.FilterLogEventsOutput{
				Events: []cwltypes.FilteredLogEvent{{Message: aws.String(tt.msg)}},
			}}
			r, _ := glueSucceededRunner(nil, cw)

			result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
				"glue_job_name":   "my-job",
				"glue_job_run_id": "jr_abc123",
			})

			require.NoError(t, err)
			assert.Equal(t, RunCheckFailed, result.State, "the event fails the run whatever its reason")
			assert.Equal(t, "SUCCEEDED (RCA: "+tt.want+")", result.Message)
		})
	}
}

// glueSucceededRunner returns a Runner whose Glue client reports SUCCEEDED
// with the log group name logGroupName, the CloudWatch Logs client cw and a
// JSON logger writing to the returned buffer.
func glueSucceededRunner(logGroupName *string, cw CloudWatchLogsAPI) (*Runner, *bytes.Buffer) {
	glueClient := &mockGlueClient{
		getOut: &glue.GetJobRunOutput{
			JobRun: &gluetypes.JobRun{
				JobRunState:  gluetypes.JobRunStateSucceeded,
				LogGroupName: logGroupName,
			},
		},
	}
	var buf bytes.Buffer
	opts := []RunnerOption{WithGlueClient(glueClient), WithLogger(slog.New(slog.NewJSONHandler(&buf, nil)))}
	if cw != nil {
		opts = append(opts, WithCloudWatchLogsClient(cw))
	}
	return NewRunner(opts...), &buf
}

// logLines decodes each JSON log line in buf.
func logLines(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if raw == "" {
			continue
		}
		var line map[string]any
		require.NoError(t, json.Unmarshal([]byte(raw), &line), "log line %q", raw)
		lines = append(lines, line)
	}
	return lines
}

// codedError is an AWS API error: its message can echo log content, its
// code cannot.
type codedError struct{ code, message string }

func (e *codedError) Error() string     { return e.code + ": " + e.message }
func (e *codedError) ErrorCode() string { return e.code }

func TestCheckGlueStatus_LogQueryErrorsWarnWithGroupStreamAndCode(t *testing.T) {
	cw := &mockCWLogsClient{
		filterErr: fmt.Errorf("operation error: %w",
			&codedError{code: "AccessDeniedException", message: "secret-log-content"}),
	}
	r, buf := glueSucceededRunner(aws.String("/aws-glue/jobs"), cw)

	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})

	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State, "a CloudWatch error must not fail the run")
	assert.NotContains(t, buf.String(), "secret-log-content", "the error message must not be logged")
	lines := logLines(t, buf)
	require.Len(t, lines, 1, "one warning for the failed query")
	assert.Equal(t, "WARN", lines[0]["level"])
	assert.Equal(t, "/aws-glue/jobs/logs-v2", lines[0]["logGroup"])
	assert.Equal(t, "jr_abc123-job-insights-rca-driver", lines[0]["logStream"])
	assert.Equal(t, "AccessDeniedException", lines[0]["errorCode"])
}

func TestCheckGlueStatus_MissingStreamOrGroupDoesNotWarn(t *testing.T) {
	// A clean success has no insights stream: CloudWatch answers
	// ResourceNotFoundException.
	notFound := &codedError{code: "ResourceNotFoundException", message: "The specified log stream does not exist."}
	tests := []struct {
		name string
		err  error
	}{
		{"not found", notFound},
		{"wrapped not found", fmt.Errorf("operation error: %w", notFound)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cw := &mockCWLogsClient{filterErr: tt.err}
			r, buf := glueSucceededRunner(nil, cw)

			result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
				"glue_job_name":   "my-job",
				"glue_job_run_id": "jr_abc123",
			})

			require.NoError(t, err)
			assert.Equal(t, RunCheckSucceeded, result.State)
			assert.Empty(t, buf.String())
		})
	}
}

func TestCheckGlueStatus_NoLogsClientWarns(t *testing.T) {
	// A named profile that does not exist makes the AWS config fail to load.
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	require.NoError(t, os.WriteFile(empty, nil, 0o600))
	t.Setenv("AWS_CONFIG_FILE", empty)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", empty)
	t.Setenv("AWS_PROFILE", "interlock-missing-profile")
	r, buf := glueSucceededRunner(nil, nil)

	result, err := r.checkGlueStatus(context.Background(), map[string]interface{}{
		"glue_job_name":   "my-job",
		"glue_job_run_id": "jr_abc123",
	})

	require.NoError(t, err)
	assert.Equal(t, RunCheckSucceeded, result.State)
	lines := logLines(t, buf)
	require.Len(t, lines, 1)
	assert.Equal(t, "WARN", lines[0]["level"])
	assert.Equal(t, "/aws-glue/jobs/logs-v2", lines[0]["logGroup"])
	assert.Equal(t, "jr_abc123", lines[0]["runId"])
	assert.Equal(t, "unknown", lines[0]["errorCode"], "a config load error carries no AWS error code")
	assert.NotContains(t, lines[0], "error", "the raw error is not logged")
}
