package trigger

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// EMRServerlessAPI is the subset of the AWS EMR Serverless client used by the trigger package.
type EMRServerlessAPI interface {
	StartJobRun(ctx context.Context, params *emrserverless.StartJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.StartJobRunOutput, error)
	GetJobRun(ctx context.Context, params *emrserverless.GetJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.GetJobRunOutput, error)
}

// ExecuteEMRServerless starts an EMR Serverless job run.
func ExecuteEMRServerless(ctx context.Context, cfg *types.TriggerConfig, client EMRServerlessAPI) (map[string]interface{}, error) {
	if cfg.ApplicationID == "" {
		return nil, fmt.Errorf("emr-serverless trigger: applicationId is required")
	}
	if cfg.JobName == "" {
		return nil, fmt.Errorf("emr-serverless trigger: jobName is required")
	}

	input := &emrserverless.StartJobRunInput{
		ApplicationId: &cfg.ApplicationID,
		Name:          &cfg.JobName,
	}

	out, err := client.StartJobRun(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("emr-serverless trigger: StartJobRun failed: %w", err)
	}

	runID := ""
	if out.JobRunId != nil {
		runID = *out.JobRunId
	}

	meta := map[string]interface{}{
		"emr_sl_application_id": cfg.ApplicationID,
		"emr_sl_job_run_id":     runID,
	}
	return meta, nil
}

// checkEMRServerlessStatus checks the status of an EMR Serverless job run.
func (r *Runner) checkEMRServerlessStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	appID, _ := metadata["emr_sl_application_id"].(string)
	runID, _ := metadata["emr_sl_job_run_id"].(string)
	if appID == "" || runID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing emr-serverless metadata"}, nil
	}

	client, err := r.getEMRServerlessClient("")
	if err != nil {
		return StatusResult{}, fmt.Errorf("emr-serverless status: getting client: %w", err)
	}

	out, err := client.GetJobRun(ctx, &emrserverless.GetJobRunInput{
		ApplicationId: &appID,
		JobRunId:      &runID,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("emr-serverless status: GetJobRun failed: %w", err)
	}

	state := out.JobRun.State
	switch state {
	case emrtypes.JobRunStateSuccess:
		return StatusResult{State: RunCheckSucceeded, Message: string(state)}, nil
	case emrtypes.JobRunStateFailed, emrtypes.JobRunStateCancelled:
		return StatusResult{State: RunCheckFailed, Message: string(state)}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: string(state)}, nil
	}
}
