package trigger

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockLambdaClient struct {
	invokeOut *lambda.InvokeOutput
	invokeErr error
}

func (m *mockLambdaClient) Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	return m.invokeOut, m.invokeErr
}

func TestExecuteLambda_Success(t *testing.T) {
	client := &mockLambdaClient{
		invokeOut: &lambda.InvokeOutput{StatusCode: 200, Payload: []byte(`{"ok":true}`)},
	}
	cfg := &types.LambdaTriggerConfig{FunctionName: "my-audit"}
	err := ExecuteLambda(context.Background(), cfg, client)
	require.NoError(t, err)
}

func TestExecuteLambda_MissingFunctionName(t *testing.T) {
	cfg := &types.LambdaTriggerConfig{}
	err := ExecuteLambda(context.Background(), cfg, &mockLambdaClient{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "functionName is required")
}

func TestExecuteLambda_InvokeError(t *testing.T) {
	client := &mockLambdaClient{invokeErr: fmt.Errorf("access denied")}
	cfg := &types.LambdaTriggerConfig{FunctionName: "my-audit"}
	err := ExecuteLambda(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invoke failed")
}

func TestExecuteLambda_FunctionError(t *testing.T) {
	client := &mockLambdaClient{
		invokeOut: &lambda.InvokeOutput{
			StatusCode:    200,
			FunctionError: aws.String("Unhandled"),
			Payload:       []byte(`{"errorMessage":"something broke"}`),
		},
	}
	cfg := &types.LambdaTriggerConfig{FunctionName: "my-audit"}
	err := ExecuteLambda(context.Background(), cfg, client)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "function error")
	assert.Contains(t, err.Error(), "something broke")
}

func TestExecuteLambda_WithPayload(t *testing.T) {
	var capturedPayload []byte
	client := &mockLambdaClient{
		invokeOut: &lambda.InvokeOutput{StatusCode: 200},
	}
	capClient := &capturingLambdaClient{
		delegate: client,
		onInvoke: func(input *lambda.InvokeInput) {
			capturedPayload = input.Payload
		},
	}
	cfg := &types.LambdaTriggerConfig{
		FunctionName: "my-audit",
		Payload:      `{"key":"value"}`,
	}
	err := ExecuteLambda(context.Background(), cfg, capClient)
	require.NoError(t, err)
	assert.JSONEq(t, `{"key":"value"}`, string(capturedPayload))
}

type capturingLambdaClient struct {
	delegate LambdaAPI
	onInvoke func(*lambda.InvokeInput)
}

func (c *capturingLambdaClient) Invoke(ctx context.Context, params *lambda.InvokeInput, optFns ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	if c.onInvoke != nil {
		c.onInvoke(params)
	}
	return c.delegate.Invoke(ctx, params, optFns...)
}

func TestExecuteLambda_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TEST_VAR", "safe")
	t.Setenv("SECRET_VAR", "leaked")

	var capturedPayload []byte
	client := &mockLambdaClient{
		invokeOut: &lambda.InvokeOutput{StatusCode: 200},
	}
	capClient := &capturingLambdaClient{
		delegate: client,
		onInvoke: func(input *lambda.InvokeInput) {
			capturedPayload = input.Payload
		},
	}
	cfg := &types.LambdaTriggerConfig{
		FunctionName: "my-audit",
		Payload:      `{"safe":"${INTERLOCK_TEST_VAR}","secret":"${SECRET_VAR}"}`,
	}
	err := ExecuteLambda(context.Background(), cfg, capClient)
	require.NoError(t, err)

	payload := string(capturedPayload)
	assert.Contains(t, payload, `"safe":"safe"`)
	assert.Contains(t, payload, `"secret":""`)
	assert.NotContains(t, payload, "leaked")
}

func TestExecuteLambda_NonPolling(t *testing.T) {
	client := &mockLambdaClient{
		invokeOut: &lambda.InvokeOutput{StatusCode: 200},
	}
	r := NewRunner(WithLambdaClient(client))
	result, err := r.CheckStatus(context.Background(), types.TriggerLambda, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, RunCheckRunning, result.State)
}
