package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// LambdaAPI is the subset of the AWS Lambda client used by the trigger package.
type LambdaAPI interface {
	Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error)
}

// JobFailedError means the job ran and failed, or with category
// types.FailureOutcomeUnknown may still be running, as opposed to an error
// that kept it from starting. Only a synchronous trigger may return it.
type JobFailedError struct {
	Message string
	// Category is the joblog row's failure category; empty means
	// types.FailurePermanent.
	Category types.FailureCategory
}

func (e *JobFailedError) Error() string { return e.Message }

// maxLambdaErrorBytes bounds the function error payload kept in the error.
const maxLambdaErrorBytes = 200

// The synchronous call waits until the caller's deadline less
// lambdaWaitMargin, which leaves the orchestrator time to record the result,
// or lambdaDefaultWait when the caller has no deadline. Variables so tests can
// shorten them.
var (
	lambdaWaitMargin  = 15 * time.Second
	lambdaDefaultWait = 100 * time.Second
)

// ExecuteLambda invokes an AWS Lambda function synchronously. A function
// error is the job's own result and comes back as a *JobFailedError; an
// Invoke failure means the function never ran. A call still running when the
// wait ends is abandoned as a *JobFailedError of category
// types.FailureOutcomeUnknown.
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

	start := time.Now()
	deadline, ok := ctx.Deadline()
	if ok {
		deadline = deadline.Add(-lambdaWaitMargin)
	} else {
		deadline = start.Add(lambdaDefaultWait)
	}
	if !deadline.After(start) {
		// Nothing was sent, so the Trigger state's retry may call it.
		return errors.New("lambda trigger: no time left to wait for the function")
	}
	callCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	out, err := client.Invoke(callCtx, input)
	if err != nil {
		// Only the wait ran out: the function may still be running, so its
		// result is unknown and calling it again could run it twice.
		if errors.Is(callCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil {
			return &JobFailedError{
				Message: fmt.Sprintf("lambda trigger: function did not finish within %ds, the orchestrator's wait; use the step-function trigger for long jobs",
					int(deadline.Sub(start)/time.Second)),
				Category: types.FailureOutcomeUnknown,
			}
		}
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
