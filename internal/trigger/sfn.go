package trigger

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// SFNAPI is the subset of the AWS Step Functions client used by the trigger package.
type SFNAPI interface {
	StartExecution(ctx context.Context, params *sfn.StartExecutionInput, optFns ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error)
	DescribeExecution(ctx context.Context, params *sfn.DescribeExecutionInput, optFns ...func(*sfn.Options)) (*sfn.DescribeExecutionOutput, error)
}

// ExecuteSFN starts an AWS Step Functions execution.
func ExecuteSFN(ctx context.Context, cfg *types.TriggerConfig, client SFNAPI) (map[string]interface{}, error) {
	if cfg.StateMachineARN == "" {
		return nil, fmt.Errorf("step-function trigger: stateMachineArn is required")
	}

	input := &sfn.StartExecutionInput{
		StateMachineArn: &cfg.StateMachineARN,
	}

	if len(cfg.Arguments) > 0 {
		b, err := json.Marshal(cfg.Arguments)
		if err != nil {
			return nil, fmt.Errorf("step-function trigger: marshaling arguments: %w", err)
		}
		s := string(b)
		input.Input = &s
	}

	out, err := client.StartExecution(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("step-function trigger: StartExecution failed: %w", err)
	}

	arn := ""
	if out.ExecutionArn != nil {
		arn = *out.ExecutionArn
	}

	meta := map[string]interface{}{
		"sfn_execution_arn": arn,
	}
	return meta, nil
}

// checkSFNStatus checks the status of a Step Functions execution.
func (r *Runner) checkSFNStatus(ctx context.Context, metadata map[string]interface{}) (StatusResult, error) {
	execArn, _ := metadata["sfn_execution_arn"].(string)
	if execArn == "" {
		return StatusResult{State: RunCheckRunning, Message: "missing sfn metadata"}, nil
	}

	client, err := r.getSFNClient("")
	if err != nil {
		return StatusResult{}, fmt.Errorf("sfn status: getting client: %w", err)
	}

	out, err := client.DescribeExecution(ctx, &sfn.DescribeExecutionInput{
		ExecutionArn: &execArn,
	})
	if err != nil {
		return StatusResult{}, fmt.Errorf("sfn status: DescribeExecution failed: %w", err)
	}

	state := out.Status
	switch state {
	case sfntypes.ExecutionStatusSucceeded:
		return StatusResult{State: RunCheckSucceeded, Message: string(state)}, nil
	case sfntypes.ExecutionStatusTimedOut:
		return StatusResult{State: RunCheckFailed, Message: string(state), FailureCategory: types.FailureTimeout}, nil
	case sfntypes.ExecutionStatusFailed, sfntypes.ExecutionStatusAborted:
		return StatusResult{State: RunCheckFailed, Message: string(state), FailureCategory: types.FailureTransient}, nil
	default:
		return StatusResult{State: RunCheckRunning, Message: string(state)}, nil
	}
}
