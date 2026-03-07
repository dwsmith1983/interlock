package trigger

import (
	"context"
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

// defaultGlueLogGroup is the standard CloudWatch log group for Glue v2 jobs.
const defaultGlueLogGroup = "/aws-glue/jobs/logs-v2"

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

	client, err := r.getGlueClient("")
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
		// (driver exits 0 despite SparkException). Cross-check the RCA
		// log stream for GlueExceptionAnalysisJobFailed events.
		if failed, reason := r.verifyGlueRCA(ctx, runID, out.JobRun.LogGroupName); failed {
			msg := "SUCCEEDED (RCA: JobFailed)"
			if reason != "" {
				msg = fmt.Sprintf("SUCCEEDED (RCA: %s)", reason)
			}
			return StatusResult{State: RunCheckFailed, Message: msg, FailureCategory: types.FailureTransient}, nil
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

// verifyGlueRCA checks the CloudWatch RCA log stream for a Glue job run.
// Returns (true, reason) if the RCA indicates the job actually failed despite
// the API reporting SUCCEEDED. Returns (false, "") on any error or if no
// failure is found — callers should trust the Glue API in that case.
func (r *Runner) verifyGlueRCA(ctx context.Context, runID string, logGroupName *string) (bool, string) {
	client, err := r.getCWLogsClient("")
	if err != nil {
		return false, ""
	}

	logGroup := defaultGlueLogGroup
	if logGroupName != nil && *logGroupName != "" {
		logGroup = *logGroupName
	}
	rcaStream := runID + "-job-insights-rca-driver"

	out, err := client.FilterLogEvents(ctx, &cloudwatchlogs.FilterLogEventsInput{
		LogGroupName:   &logGroup,
		LogStreamNames: []string{rcaStream},
		FilterPattern:  aws.String("GlueExceptionAnalysisJobFailed"),
		Limit:          aws.Int32(1),
	})
	if err != nil {
		// Graceful: log group may not exist, permissions may be missing, etc.
		return false, ""
	}

	if len(out.Events) > 0 {
		reason := extractGlueFailureReason(aws.ToString(out.Events[0].Message))
		return true, reason
	}
	return false, ""
}

// extractGlueFailureReason pulls the "Failure Reason" value from a Glue RCA
// log message. The message is JSON but we do a simple string extraction to
// avoid a full parse dependency for a single field.
func extractGlueFailureReason(msg string) string {
	const marker = `"Failure Reason":"`
	idx := strings.Index(msg, marker)
	if idx < 0 {
		return ""
	}
	start := idx + len(marker)
	end := strings.Index(msg[start:], `"`)
	if end < 0 {
		return ""
	}
	return msg[start : start+end]
}
