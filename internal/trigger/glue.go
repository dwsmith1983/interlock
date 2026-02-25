package trigger

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// GlueAPI is the subset of the AWS Glue client used by the trigger package.
type GlueAPI interface {
	StartJobRun(ctx context.Context, params *glue.StartJobRunInput, optFns ...func(*glue.Options)) (*glue.StartJobRunOutput, error)
	GetJobRun(ctx context.Context, params *glue.GetJobRunInput, optFns ...func(*glue.Options)) (*glue.GetJobRunOutput, error)
}

// ExecuteGlue starts an AWS Glue job run.
func ExecuteGlue(ctx context.Context, cfg *types.TriggerConfig, client GlueAPI) (map[string]interface{}, error) {
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
		return StatusResult{State: RunCheckSucceeded, Message: string(state)}, nil
	case gluetypes.JobRunStateFailed, gluetypes.JobRunStateTimeout,
		gluetypes.JobRunStateStopped, gluetypes.JobRunStateError:
		return StatusResult{State: RunCheckFailed, Message: string(state)}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: string(state)}, nil
	}
}
