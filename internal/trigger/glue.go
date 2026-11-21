package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// GlueAPI is the subset of the AWS Glue client used by the trigger package.
type GlueAPI interface {
	StartJobRun(ctx context.Context, params *glue.StartJobRunInput, optFns ...func(*glue.Options)) (*glue.StartJobRunOutput, error)
	GetJobRun(ctx context.Context, params *glue.GetJobRunInput, optFns ...func(*glue.Options)) (*glue.GetJobRunOutput, error)
}

// CloudWatchLogsAPI is the subset of the CloudWatch Logs client used for
// verifying Glue job outcomes via the RCA (root cause analysis) log stream.
type CloudWatchLogsAPI interface {
	FilterLogEvents(ctx context.Context, params *cloudwatchlogs.FilterLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error)
}

// glueInsightsLogGroup is the group Glue writes the job insights streams to
// for a job without a security configuration or --continuous-log-logGroup.
// It is always the group read: the LogGroupName GetJobRun reports is a
// prefix (/aws-glue/jobs, or /aws-glue/jobs-<role>-<config>/ with a security
// configuration), not a group.
const glueInsightsLogGroup = "/aws-glue/jobs/logs-v2"

// ExecuteGlue starts an AWS Glue job run.
func ExecuteGlue(ctx context.Context, cfg *types.GlueTriggerConfig, client GlueAPI) (map[string]interface{}, error) {
	if cfg.JobName == "" {
		return nil, fmt.Errorf("glue trigger: jobName is required")
	}

	input := &glue.StartJobRunInput{
		JobName:   &cfg.JobName,
		Arguments: cfg.Arguments,
	}

	out, err := client.StartJobRun(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("glue trigger: StartJobRun failed: %w", err)
	}

	runID := ""
	if out.JobRunId != nil {
		runID = *out.JobRunId
	}

	meta := map[string]interface{}{
		"glue_job_name":   cfg.JobName,
		"glue_job_run_id": runID,
	}
	return meta, nil
}

// checkGlueStatus checks the status of an AWS Glue job run.
func (r *Runner) checkGlueStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	jobName, _ := metadata["glue_job_name"].(string)
	runID, _ := metadata["glue_job_run_id"].(string)
	if jobName == "" || runID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing glue metadata"}, nil
	}

	client, err := r.getGlueClient(ctx, "")
	if err != nil {
		return StatusResult{}, fmt.Errorf("glue status: getting client: %w", err)
	}

	out, err := client.GetJobRun(ctx, &glue.GetJobRunInput{
		JobName: &jobName,
		RunId:   &runID,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("glue status: GetJobRun failed: %w", err)
	}

	if out.JobRun == nil {
		return StatusResult{}, fmt.Errorf("glue status: GetJobRun returned nil JobRun")
	}
	state := out.JobRun.JobRunState
	switch state {
	case gluetypes.JobRunStateSucceeded:
		// Glue can report SUCCEEDED when the Spark job actually failed
		// (driver exits 0 despite SparkException, or disk-full errors).
		// Cross-check the job insights exception analysis stream.
		if failed, reason := r.verifyGlueRCA(ctx, runID); failed {
			return StatusResult{
				State:           RunCheckFailed,
				Message:         fmt.Sprintf("SUCCEEDED (%s)", reason),
				FailureCategory: types.FailureTransient,
			}, nil
		}
		return StatusResult{State: RunCheckSucceeded, Message: string(state)}, nil
	case gluetypes.JobRunStateTimeout:
		return StatusResult{State: RunCheckFailed, Message: string(state), FailureCategory: types.FailureTimeout}, nil
	case gluetypes.JobRunStateFailed, gluetypes.JobRunStateStopped, gluetypes.JobRunStateError:
		return StatusResult{State: RunCheckFailed, Message: string(state), FailureCategory: types.FailureTransient}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: string(state)}, nil
	}
}

// verifyGlueRCA checks the job insights exception analysis stream
// (<runId>-job-insights-rca-driver) of a Glue job run for a
// GlueExceptionAnalysisJobFailed event, to detect false successes: Glue can
// report SUCCEEDED when the Spark job actually failed (driver exits 0 despite
// SparkException). Glue writes the stream only for a failed run of a job with
// job insights on.
//
// Returns (true, reason) if the event is found, with control characters other
// than newline and tab removed from the failure reason and the reason cut to
// maxErrorBodyBytes (512). Returns (false, "") if it is not found or
// CloudWatch cannot be read; a failed read logs one warning and never fails
// the run.
func (r *Runner) verifyGlueRCA(ctx context.Context, runID string) (failed bool, reason string) {
	client, err := r.getCWLogsClient(ctx, "")
	if err != nil {
		r.logger.WarnContext(ctx, "glue log check skipped: no CloudWatch Logs client",
			"logGroup", glueInsightsLogGroup, "runId", runID, "errorCode", awsErrorCode(err))
		return false, ""
	}

	msg, ok := r.firstGlueLogEvent(ctx, client, glueInsightsLogGroup, runID+"-job-insights-rca-driver", "GlueExceptionAnalysisJobFailed")
	if !ok {
		return false, ""
	}
	if fr := sanitizeBody([]byte(extractGlueFailureReason(msg)), nil, maxErrorBodyBytes); fr != "" {
		return true, "RCA: " + fr
	}
	return true, "RCA: JobFailed"
}

// cwLogsNotFoundCode is the error code CloudWatch Logs returns for a missing
// log group or stream.
const cwLogsNotFoundCode = "ResourceNotFoundException"

// firstGlueLogEvent returns the message of the first event in stream matching
// pattern. A missing stream or group is not an error: a clean success writes
// no insights stream. Any other failed query logs one warning naming the
// group, stream and AWS error code, but not the error message, which can echo
// log content.
func (r *Runner) firstGlueLogEvent(ctx context.Context, client CloudWatchLogsAPI, logGroup, stream, pattern string) (string, bool) {
	out, err := client.FilterLogEvents(ctx, &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:   &logGroup,
		LogStreamNames: []string{stream},
		FilterPattern:  &pattern,
		Limit:          aws.Int32(1),
	})
	if err != nil {
		if code := awsErrorCode(err); code != cwLogsNotFoundCode {
			r.logger.WarnContext(ctx, "glue log check could not read CloudWatch Logs",
				"logGroup", logGroup, "logStream", stream, "errorCode", code)
		}
		return "", false
	}
	if len(out.Events) == 0 {
		return "", false
	}
	return aws.ToString(out.Events[0].Message), true
}

// awsErrorCode returns the AWS API error code in err's chain, or "unknown"
// when err carries none (a network failure, a cancelled context or an AWS
// config that does not load).
func awsErrorCode(err error) string {
	var coded interface{ ErrorCode() string }
	if errors.As(err, &coded) {
		return coded.ErrorCode()
	}
	return "unknown"
}

// extractGlueFailureReason returns the "Failure Reason" of a job insights
// failure event: a log4j prefix, then one JSON object to the end of the line.
// It returns "" when the message holds no such object.
func extractGlueFailureReason(msg string) string {
	start := strings.IndexByte(msg, '{')
	if start < 0 {
		return ""
	}
	var event struct {
		FailureReason string `json:"Failure Reason"`
	}
	if err := json.Unmarshal([]byte(msg[start:]), &event); err != nil {
		return ""
	}
	return event.FailureReason
}
