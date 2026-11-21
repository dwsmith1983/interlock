package trigger

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// EMRServerlessAPI is the subset of the AWS EMR Serverless client used by the trigger package.
type EMRServerlessAPI interface {
	StartJobRun(ctx context.Context, params *emrserverless.StartJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.StartJobRunOutput, error)
	GetJobRun(ctx context.Context, params *emrserverless.GetJobRunInput, optFns ...func(*emrserverless.Options)) (*emrserverless.GetJobRunOutput, error)
}

// ExecuteEMRServerless starts an EMR Serverless job run: a spark-submit of the
// entry point under the execution role. The client token is the hex SHA-256 of
// the run ID (64 characters, the token's limit, while a run ID can be 80): a
// retried start of the same run reuses the token, so EMR Serverless does not
// start a second job run, and a new run starts a new one.
func ExecuteEMRServerless(ctx context.Context, cfg *types.EMRServerlessTriggerConfig, client EMRServerlessAPI) (map[string]interface{}, error) {
	if cfg.ApplicationID == "" {
		return nil, fmt.Errorf("emr-serverless trigger: applicationId is required")
	}
	if cfg.JobName == "" {
		return nil, fmt.Errorf("emr-serverless trigger: jobName is required")
	}
	if cfg.ExecutionRoleARN == "" {
		return nil, fmt.Errorf("emr-serverless trigger: executionRoleArn is required")
	}
	if cfg.EntryPoint == "" {
		return nil, fmt.Errorf("emr-serverless trigger: entryPoint is required")
	}
	if cfg.Run.RunID == "" {
		return nil, fmt.Errorf("emr-serverless trigger: run ID is required for the client token")
	}

	token := sha256.Sum256([]byte(cfg.Run.RunID))
	submit := emrtypes.SparkSubmit{
		EntryPoint:          &cfg.EntryPoint,
		EntryPointArguments: cfg.EntryPointArguments,
	}
	if cfg.SparkSubmitParameters != "" {
		submit.SparkSubmitParameters = &cfg.SparkSubmitParameters
	}
	input := &emrserverless.StartJobRunInput{
		ApplicationId:    &cfg.ApplicationID,
		Name:             &cfg.JobName,
		ClientToken:      aws.String(hex.EncodeToString(token[:])),
		ExecutionRoleArn: &cfg.ExecutionRoleARN,
		JobDriver:        &emrtypes.JobDriverMemberSparkSubmit{Value: submit},
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

	client, err := r.getEMRServerlessClient(ctx, "")
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

	if out.JobRun == nil {
		return StatusResult{}, fmt.Errorf("emr-serverless status: GetJobRun returned nil JobRun")
	}
	state := out.JobRun.State
	switch state {
	case emrtypes.JobRunStateSuccess:
		return StatusResult{State: RunCheckSucceeded, Message: string(state)}, nil
	case emrtypes.JobRunStateFailed, emrtypes.JobRunStateCancelled:
		return StatusResult{State: RunCheckFailed, Message: string(state), FailureCategory: types.FailureTransient}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: string(state)}, nil
	}
}
