package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

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
	var jobErr *JobFailedError
	assert.False(t, errors.As(err, &jobErr), "a failed Invoke never started the job, so it is not a job failure")
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
	var jobErr *JobFailedError
	require.True(t, errors.As(err, &jobErr), "err = %v, want a *JobFailedError", err)
	assert.Contains(t, jobErr.Error(), "function error")
	assert.Contains(t, jobErr.Error(), "something broke")
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

// invokedPayload runs ExecuteLambda with cfg and returns the Invoke payload.
func invokedPayload(t *testing.T, cfg *types.LambdaTriggerConfig) []byte {
	t.Helper()
	var payload []byte
	client := &capturingLambdaClient{
		delegate: &mockLambdaClient{invokeOut: &lambda.InvokeOutput{StatusCode: 200}},
		onInvoke: func(input *lambda.InvokeInput) { payload = input.Payload },
	}
	require.NoError(t, ExecuteLambda(context.Background(), cfg, client))
	return payload
}

// The function learns the run it serves: the identity variables expand to
// this launch's values, JSON-escaped, ahead of any INTERLOCK_ env var.
func TestExecuteLambda_RunIdentity(t *testing.T) {
	t.Setenv("INTERLOCK_DATE", "from-env")
	t.Setenv("INTERLOCK_TOKEN", "tok")
	run := types.RunIdentity{PipelineID: "orders", ScheduleID: "stream", Date: "2026-03-01T05", RunID: `exec"4\2`}
	payload := invokedPayload(t, &types.LambdaTriggerConfig{
		FunctionName: "f",
		Payload: `{"p":"${INTERLOCK_PIPELINE_ID}","s":"${INTERLOCK_SCHEDULE_ID}","d":"${INTERLOCK_DATE}",` +
			`"r":"${INTERLOCK_RUN_ID}","t":"${INTERLOCK_TOKEN}"}`,
		Run: run,
	})
	var got map[string]string
	require.NoError(t, json.Unmarshal(payload, &got), "payload %s", payload)
	assert.Equal(t, map[string]string{"p": "orders", "s": "stream", "d": "2026-03-01T05", "r": `exec"4\2`, "t": "tok"}, got)
}

// A generated payload is already marshalled, so it is sent as is, and a
// direct caller with neither a payload nor a generated one sends none.
func TestExecuteLambda_GeneratedAndEmptyPayload(t *testing.T) {
	t.Setenv("INTERLOCK_X", "expanded")
	body := `{"pipelineId":"a${INTERLOCK_X}$INTERLOCK_X"}`
	assert.Equal(t, body, string(invokedPayload(t, &types.LambdaTriggerConfig{
		FunctionName: "f", Payload: body, PayloadGenerated: true,
	})))
	assert.Nil(t, invokedPayload(t, &types.LambdaTriggerConfig{FunctionName: "f"}))
}

// blockingLambdaClient blocks Invoke until its context ends, or a safety timer
// fires so a missing deadline fails the test instead of hanging it.
type blockingLambdaClient struct{ calls atomic.Int32 }

func (c *blockingLambdaClient) Invoke(ctx context.Context, _ *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	c.calls.Add(1)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, errors.New("test safety timer: Invoke was never cancelled")
	}
}

// The wait ends lambdaWaitMargin before the caller's deadline, or after
// lambdaDefaultWait without one, as one failure whose outcome is unknown. The
// caller's own deadline, or no time left to wait, is a plain error. Calls go
// through the Runner, so redaction must keep the category.
func TestExecuteLambda_Wait(t *testing.T) {
	const unknown = "lambda trigger: function did not finish within %ds, the orchestrator's wait; use the step-function trigger for long jobs"
	tests := []struct {
		name                         string
		callerDeadline, margin, dflt time.Duration // callerDeadline 0: none
		wantErr                      string
		wantUnknown                  bool
		wantCalls                    int32
	}{
		{name: "stops before the caller deadline", callerDeadline: 2 * time.Second, margin: 1500 * time.Millisecond,
			wantErr: fmt.Sprintf(unknown, 0), wantUnknown: true, wantCalls: 1},
		{name: "default wait without a caller deadline", dflt: 1100 * time.Millisecond,
			wantErr: fmt.Sprintf(unknown, 1), wantUnknown: true, wantCalls: 1},
		// A margin past the caller's deadline leaves that deadline in charge.
		{name: "caller deadline is a plain error", callerDeadline: 20 * time.Millisecond, margin: -time.Second,
			wantErr: "lambda trigger: Invoke failed: context deadline exceeded", wantCalls: 1},
		{name: "no time left is a plain error", callerDeadline: 2 * time.Second, margin: 2 * time.Second,
			wantErr: "lambda trigger: no time left to wait for the function"},
	}
	defer func(m, w time.Duration) { lambdaWaitMargin, lambdaDefaultWait = m, w }(lambdaWaitMargin, lambdaDefaultWait)
	cfg := &types.TriggerConfig{Type: types.TriggerLambda, Lambda: &types.LambdaTriggerConfig{FunctionName: "f"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lambdaWaitMargin, lambdaDefaultWait = tt.margin, tt.dflt
			ctx, cancel := context.Background(), context.CancelFunc(func() {})
			if tt.callerDeadline > 0 {
				ctx, cancel = context.WithTimeout(ctx, tt.callerDeadline)
			}
			defer cancel()
			client := &blockingLambdaClient{}

			_, err := NewRunner(WithLambdaClient(client)).Execute(ctx, cfg)

			require.EqualError(t, err, tt.wantErr)
			var jobErr *JobFailedError
			require.Equal(t, tt.wantUnknown, errors.As(err, &jobErr), "err = %v, a *JobFailedError?", err)
			if tt.wantUnknown {
				assert.Equal(t, types.FailureOutcomeUnknown, jobErr.Category)
				assert.NoError(t, ctx.Err(), "returned after the caller's deadline")
			}
			assert.Equal(t, tt.wantCalls, client.calls.Load())
		})
	}
}
