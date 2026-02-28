package trigger

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// EMRAPI is the subset of the AWS EMR client used by the trigger package.
type EMRAPI interface {
	AddJobFlowSteps(ctx context.Context, params *emr.AddJobFlowStepsInput, optFns ...func(*emr.Options)) (*emr.AddJobFlowStepsOutput, error)
	DescribeStep(ctx context.Context, params *emr.DescribeStepInput, optFns ...func(*emr.Options)) (*emr.DescribeStepOutput, error)
}

// ExecuteEMR adds a step to an existing EMR cluster.
func ExecuteEMR(ctx context.Context, cfg *types.EMRTriggerConfig, client EMRAPI) (map[string]interface{}, error) {
	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("emr trigger: clusterId is required")
	}
	if cfg.StepName == "" {
		return nil, fmt.Errorf("emr trigger: stepName is required")
	}
	if cfg.Command == "" {
		return nil, fmt.Errorf("emr trigger: command is required (JAR path)")
	}

	args := make([]string, 0, len(cfg.Arguments))
	for k, v := range cfg.Arguments {
		args = append(args, k+"="+v)
	}

	input := &emr.AddJobFlowStepsInput{
		JobFlowId: &cfg.ClusterID,
		Steps: []emrtypes.StepConfig{
			{
				Name: &cfg.StepName,
				HadoopJarStep: &emrtypes.HadoopJarStepConfig{
					Jar:  &cfg.Command,
					Args: args,
				},
				ActionOnFailure: emrtypes.ActionOnFailureContinue,
			},
		},
	}

	out, err := client.AddJobFlowSteps(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("emr trigger: AddJobFlowSteps failed: %w", err)
	}

	stepID := ""
	if len(out.StepIds) > 0 {
		stepID = out.StepIds[0]
	}

	meta := map[string]interface{}{
		"emr_cluster_id": cfg.ClusterID,
		"emr_step_id":    stepID,
	}
	return meta, nil
}

// checkEMRStatus checks the status of an EMR step.
func (r *Runner) checkEMRStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	clusterID, _ := metadata["emr_cluster_id"].(string)
	stepID, _ := metadata["emr_step_id"].(string)
	if clusterID == "" || stepID == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing emr metadata"}, nil
	}

	client, err := r.getEMRClient("")
	if err != nil {
		return StatusResult{}, fmt.Errorf("emr status: getting client: %w", err)
	}

	out, err := client.DescribeStep(ctx, &emr.DescribeStepInput{
		ClusterId: &clusterID,
		StepId:    &stepID,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("emr status: DescribeStep failed: %w", err)
	}

	if out.Step == nil {
		return StatusResult{}, fmt.Errorf("emr status: DescribeStep returned nil Step")
	}
	if out.Step.Status == nil {
		return StatusResult{}, fmt.Errorf("emr status: DescribeStep returned nil Step.Status")
	}
	state := out.Step.Status.State
	switch state {
	case emrtypes.StepStateCompleted:
		return StatusResult{State: RunCheckSucceeded, Message: string(state)}, nil
	case emrtypes.StepStateFailed, emrtypes.StepStateCancelled, emrtypes.StepStateInterrupted:
		return StatusResult{State: RunCheckFailed, Message: string(state), FailureCategory: types.FailureTransient}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: string(state)}, nil
	}
}
