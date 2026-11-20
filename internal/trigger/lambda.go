package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// LambdaAPI is the subset of the AWS Lambda client used by the trigger package.
type LambdaAPI interface {
	Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// JobFailedError means the job ran and failed, as opposed to an error that
// kept it from starting. Only a synchronous trigger whose job has already run
// and failed may return it.
type JobFailedError struct {
	Message string
}

func (e *JobFailedError) Error() string { return e.Message }

// maxLambdaErrorBytes bounds the function error payload kept in the error.
const maxLambdaErrorBytes = 200

// ExecuteLambda invokes an AWS Lambda function synchronously. A function
// error is the job's own result and comes back as a *JobFailedError; an
// Invoke failure means the function never ran.
func ExecuteLambda(ctx context.Context, cfg *types.LambdaTriggerConfig, client LambdaAPI) error {
	if cfg.FunctionName == "" {
		return fmt.Errorf("lambda trigger: functionName is required")
	}

	input := &lambda.InvokeInput{
		FunctionName:   &cfg.FunctionName,
		InvocationType: lambdatypes.InvocationTypeRequestResponse,
	}

	if cfg.Payload != "" {
		input.Payload = []byte(expandRunBody(cfg.Payload, cfg.PayloadGenerated, cfg.Run))
		// Fixed text: the payload holds expanded values, such as a token.
		if !json.Valid(input.Payload) {
			return errors.New("lambda trigger: payload is not valid JSON after variable expansion")
		}
	}

	out, err := client.Invoke(ctx, input)
	if err != nil {
		return fmt.Errorf("lambda trigger: Invoke failed: %w", err)
	}

	if out.FunctionError != nil {
		errMsg := sanitizeBody(out.Payload, secretValues(nil, cfg.Payload), maxLambdaErrorBytes)
		return &JobFailedError{Message: fmt.Sprintf("lambda trigger: function error: %s: %s", aws.ToString(out.FunctionError), errMsg)}
	}

	if out.StatusCode < 200 || out.StatusCode >= 300 {
		return fmt.Errorf("lambda trigger: unexpected status %d", out.StatusCode)
	}

	return nil
}
