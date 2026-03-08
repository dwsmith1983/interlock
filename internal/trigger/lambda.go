package trigger

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// LambdaAPI is the subset of the AWS Lambda client used by the trigger package.
type LambdaAPI interface {
	Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// ExecuteLambda invokes an AWS Lambda function synchronously.
func ExecuteLambda(ctx context.Context, cfg *types.LambdaTriggerConfig, client LambdaAPI) error {
	if cfg.FunctionName == "" {
		return fmt.Errorf("lambda trigger: functionName is required")
	}

	input := &lambda.InvokeInput{
		FunctionName:   &cfg.FunctionName,
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
	}

	if cfg.Payload != "" {
		input.Payload = []byte(os.ExpandEnv(cfg.Payload))
	}

	out, err := client.Invoke(ctx, input)
	if err != nil {
		return fmt.Errorf("lambda trigger: Invoke failed: %w", err)
	}

	if out.FunctionError != nil {
		errMsg := string(out.Payload)
		if len(errMsg) > 200 {
			errMsg = errMsg[:200]
		}
		return fmt.Errorf("lambda trigger: function error: %s: %s", aws.ToString(out.FunctionError), errMsg)
	}

	if out.StatusCode < 200 || out.StatusCode >= 300 {
		return fmt.Errorf("lambda trigger: unexpected status %d", out.StatusCode)
	}

	return nil
}
