package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedact(t *testing.T) {
	tests := []struct {
		name     string
		env      map[string]string
		headers  map[string]string
		bodies   []string
		in, want string
	}{
		{"one value", map[string]string{"INTERLOCK_A": "s3cret"}, map[string]string{"Authorization": "Bearer ${INTERLOCK_A}"}, nil,
			"echo Bearer s3cret", "echo Bearer [REDACTED]"},
		{"value used twice", map[string]string{"INTERLOCK_A": "s3cret"}, nil, []string{"${INTERLOCK_A}"},
			"s3cret and s3cret", "[REDACTED] and [REDACTED]"},
		{"longest first", map[string]string{"INTERLOCK_A": "abcd", "INTERLOCK_B": "abcdefgh"}, nil,
			[]string{"${INTERLOCK_A}", "$INTERLOCK_B"}, "x abcdefgh y abcd", "x [REDACTED] y [REDACTED]"},
		{"three characters left alone", map[string]string{"INTERLOCK_A": "abc"}, nil, []string{"${INTERLOCK_A}"},
			"abc cabbage", "abc cabbage"},
		// airflow and databricks headers expand identity names from the env.
		{"identity variable name", map[string]string{"INTERLOCK_RUN_ID": "run-value"}, nil, []string{"${INTERLOCK_RUN_ID}"},
			"run-value", "[REDACTED]"},
		{"escaped forms", map[string]string{"INTERLOCK_A": `a"b<c`}, nil, []string{"${INTERLOCK_A}"},
			`a"b<c a\"b<c a\"b\u003cc`, "[REDACTED] [REDACTED] [REDACTED]"},
		{"slash escaped", map[string]string{"INTERLOCK_A": "ab/cd<zq9"}, nil, []string{"${INTERLOCK_A}"},
			`ab\/cd<zq9 ab\/cd\u003czq9`, "[REDACTED] [REDACTED]"},
		{"percent-encoded", map[string]string{"INTERLOCK_A": "a+b/c=zq9"}, nil, []string{"${INTERLOCK_A}"},
			"a%2Bb%2Fc%3Dzq9 a+b%2Fc=zq9", "[REDACTED] [REDACTED]"},
		{"empty value list", nil, nil, nil, "nothing to hide", "nothing to hide"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			assert.Equal(t, tt.want, redact(tt.in, secretValues(tt.headers, tt.bodies...)))
		})
	}
}

// A token that crosses the error body limit is redacted before the cut, so
// no prefix of it survives; control characters inside it do not stop the
// match, and the cut never splits a rune.
func TestSanitizeBodyRedactsBeforeCut(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-zq9-secret")
	values := secretValues(map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"})
	pad := strings.Repeat("x", maxErrorBodyBytes-7)

	got := sanitizeBody([]byte(pad+"tok-zq9-secret tail"), values, maxErrorBodyBytes)
	assert.NotContains(t, got, "tok-")
	assert.Equal(t, pad+"[REDACT", got)

	assert.Equal(t, "a [REDACTED] b", sanitizeBody([]byte("a tok-zq9\x00-secret b"), values, maxErrorBodyBytes))

	got = sanitizeBody([]byte(strings.Repeat("x", maxErrorBodyBytes-1)+"\u00e9"), nil, maxErrorBodyBytes)
	assert.Equal(t, strings.Repeat("x", maxErrorBodyBytes-1), got)
}

func TestRedactErrorKeepsTypes(t *testing.T) {
	values := []string{"tok-zq9-secret"}

	var jobErr *JobFailedError
	err := redactError(&JobFailedError{Message: "failed: tok-zq9-secret"}, values)
	require.True(t, errors.As(err, &jobErr))
	assert.Equal(t, "failed: [REDACTED]", jobErr.Message)

	var trigErr *TriggerError
	err = redactError(fmt.Errorf("outer: %w", NewTriggerError(types.FailurePermanent, "status 400: tok-zq9-secret", nil)), values)
	require.True(t, errors.As(err, &trigErr))
	assert.Equal(t, types.FailurePermanent, trigErr.Category)
	assert.Equal(t, "outer: status 400: [REDACTED]", err.Error())

	err = redactError(fmt.Errorf("wrapped: %w", errors.New("tok-zq9-secret")), values)
	assert.Nil(t, errors.Unwrap(err), "a wrapped cause would still print the value")
	assert.Equal(t, "wrapped: [REDACTED]", err.Error())
}

// serve starts h and points defaultHTTPClient at it for the test.
func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	t.Cleanup(func() { defaultHTTPClient = origClient })
	return srv
}

// echoServer answers status with pad bytes of filler, then the request's
// Authorization header and body, as an endpoint that quotes the request in its
// error does: raw, and the header JSON-encoded with and without HTML escaping.
func echoServer(t *testing.T, status, pad int) *httptest.Server {
	t.Helper()
	return serve(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		auth := r.Header.Get("Authorization")
		escaped, _ := json.Marshal(auth)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(strings.Repeat("x", pad) + "auth=" + auth + " body=" + string(body) +
			" plain=" + jsonStringContent(auth) + " escaped=" + string(escaped)))
	})
}

func TestExecuteRedactsEchoedToken(t *testing.T) {
	auth := map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"}
	configs := map[string]func(url string) *types.TriggerConfig{
		"http": func(url string) *types.TriggerConfig {
			return &types.TriggerConfig{Type: types.TriggerHTTP, HTTP: &types.HTTPTriggerConfig{
				Method: http.MethodPost, URL: url, Headers: auth, Body: `{"t":"${INTERLOCK_BODY_TOKEN}"}`,
			}}
		},
		"airflow": func(url string) *types.TriggerConfig {
			return &types.TriggerConfig{Type: types.TriggerAirflow, Airflow: &types.AirflowTriggerConfig{
				URL: url, DagID: "d", Headers: auth, Body: `{"t":"${INTERLOCK_BODY_TOKEN}"}`,
			}}
		},
		"databricks": func(url string) *types.TriggerConfig {
			return &types.TriggerConfig{Type: types.TriggerDatabricks, Databricks: &types.DatabricksTriggerConfig{
				WorkspaceURL: url, JobID: "1", Headers: auth,
			}}
		},
	}
	// Each token contains zq9, so its absence shows no form of it is left.
	// The body token stays valid inside a JSON string; airflow re-encodes it
	// with HTML escaping.
	for _, token := range []string{"tok-zq9-secret", `tok"zq9&x`} {
		for name, build := range configs {
			t.Run(name+"/"+token, func(t *testing.T) {
				t.Setenv("INTERLOCK_TOKEN", token)
				t.Setenv("INTERLOCK_BODY_TOKEN", "body<zq9&x")
				srv := echoServer(t, http.StatusBadRequest, 0)

				_, err := NewRunner(WithHTTPClient(srv.Client())).Execute(context.Background(), build(srv.URL))

				require.Error(t, err)
				assert.Contains(t, err.Error(), "[REDACTED]")
				assert.NotContains(t, err.Error(), "zq9")
			})
		}
	}
}

// echoLambdaClient fails the function with the event it was given.
type echoLambdaClient struct{ invoked bool }

func (c *echoLambdaClient) Invoke(_ context.Context, in *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	c.invoked = true
	return &lambda.InvokeOutput{StatusCode: 200, FunctionError: aws.String("Unhandled"), Payload: in.Payload}, nil
}

// The function echoes its event; the token starts 4 bytes before the cut, so
// it is redacted before the cut and the error keeps its type.
func TestExecuteLambdaRedactsFunctionError(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-zq9-secret")
	pad := strings.Repeat("x", maxLambdaErrorBytes-4-len(`{"p":"","t":"`))
	cfg := &types.TriggerConfig{Type: types.TriggerLambda, Lambda: &types.LambdaTriggerConfig{
		FunctionName: "f", Payload: `{"p":"` + pad + `","t":"${INTERLOCK_TOKEN}"}`,
	}}

	_, err := NewRunner(WithLambdaClient(&echoLambdaClient{})).Execute(context.Background(), cfg)

	var jobErr *JobFailedError
	require.True(t, errors.As(err, &jobErr), "err = %v, want a *JobFailedError", err)
	assert.Equal(t, `lambda trigger: function error: Unhandled: {"p":"`+pad+`","t":"[RED`, jobErr.Message)
}

// rejectingLambdaClient fails Invoke with the event in its error, which no
// cut site handles, so only the Runner redacts it.
type rejectingLambdaClient struct{}

func (rejectingLambdaClient) Invoke(_ context.Context, in *lambda.InvokeInput, _ ...func(*lambda.Options)) (*lambda.InvokeOutput, error) {
	return nil, errors.New("rejected event " + string(in.Payload))
}

func TestExecuteRedactsUncutError(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-zq9-secret")
	cfg := &types.TriggerConfig{Type: types.TriggerLambda, Lambda: &types.LambdaTriggerConfig{
		FunctionName: "f", Payload: `{"t":"${INTERLOCK_TOKEN}"}`,
	}}

	_, err := NewRunner(WithLambdaClient(rejectingLambdaClient{})).Execute(context.Background(), cfg)

	require.EqualError(t, err, `lambda trigger: Invoke failed: rejected event {"t":"[REDACTED]"}`)
}

func TestExecuteLambdaStripsControlCharacters(t *testing.T) {
	client := &mockLambdaClient{invokeOut: &lambda.InvokeOutput{
		StatusCode: 200, FunctionError: aws.String("Unhandled"), Payload: []byte("bad\x00input\x1b[31m"),
	}}

	err := ExecuteLambda(context.Background(), &types.LambdaTriggerConfig{FunctionName: "f"}, client)

	require.EqualError(t, err, "lambda trigger: function error: Unhandled: badinput[31m")
}

// The echoed token starts 4 bytes before the error body cut, so a site that
// cut before redacting would leave its prefix, which the Runner cannot match.
func TestRedactsTokenAtTheCut(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-zq9-secret")
	auth := map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"}
	execute := func(r *Runner, cfg *types.TriggerConfig) error {
		_, err := r.Execute(context.Background(), cfg)
		return err
	}
	status := func(r *Runner, tt types.TriggerType, meta map[string]interface{}) error {
		_, err := r.CheckStatus(context.Background(), tt, meta, auth)
		return err
	}
	httpCfg := func(url string) *types.TriggerConfig {
		return &types.TriggerConfig{Type: types.TriggerHTTP, HTTP: &types.HTTPTriggerConfig{URL: url, Headers: auth}}
	}
	rows := []struct {
		name string
		code int
		call func(r *Runner, url string) error
	}{
		{"http 4xx", http.StatusBadRequest, func(r *Runner, url string) error { return execute(r, httpCfg(url)) }},
		{"http 5xx", http.StatusBadGateway, func(r *Runner, url string) error { return execute(r, httpCfg(url)) }},
		{"airflow", http.StatusBadRequest, func(r *Runner, url string) error {
			return execute(r, &types.TriggerConfig{Type: types.TriggerAirflow, Airflow: &types.AirflowTriggerConfig{URL: url, DagID: "d", Headers: auth}})
		}},
		{"databricks", http.StatusBadRequest, func(r *Runner, url string) error {
			return execute(r, &types.TriggerConfig{Type: types.TriggerDatabricks, Databricks: &types.DatabricksTriggerConfig{WorkspaceURL: url, JobID: "1", Headers: auth}})
		}},
		{"airflow status", http.StatusUnauthorized, func(r *Runner, url string) error {
			return status(r, types.TriggerAirflow, map[string]interface{}{"airflow_url": url, "airflow_dag_id": "d", "airflow_dag_run_id": "r"})
		}},
		{"databricks status", http.StatusUnauthorized, func(r *Runner, url string) error {
			return status(r, types.TriggerDatabricks, map[string]interface{}{"databricks_workspace_url": url, "databricks_run_id": "1"})
		}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			srv := echoServer(t, row.code, maxErrorBodyBytes-4-len("auth=Bearer "))

			err := row.call(NewRunner(WithHTTPClient(srv.Client())), srv.URL)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "auth=Bearer [RED")
			assert.NotContains(t, err.Error(), "tok-")
		})
	}
}

func TestExecuteLambdaRejectsInvalidExpandedPayload(t *testing.T) {
	t.Setenv("INTERLOCK_X", `"zq9`)
	client := &echoLambdaClient{}
	cfg := &types.TriggerConfig{Type: types.TriggerLambda, Lambda: &types.LambdaTriggerConfig{
		FunctionName: "f", Payload: `{"t":"${INTERLOCK_X}"}`,
	}}

	_, err := NewRunner(WithLambdaClient(client)).Execute(context.Background(), cfg)

	require.EqualError(t, err, "lambda trigger: payload is not valid JSON after variable expansion")
	assert.False(t, client.invoked, "an invalid payload must not reach the function")
}

func TestCheckStatusRedactsEchoedHeader(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "tok-zq9-secret")
	headers := map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"}
	meta := func(url string) map[string]interface{} {
		return map[string]interface{}{"airflow_url": url, "airflow_dag_id": "d", "airflow_dag_run_id": "r"}
	}

	t.Run("error", func(t *testing.T) {
		srv := echoServer(t, http.StatusUnauthorized, 0)
		_, err := NewRunner().CheckStatus(context.Background(), types.TriggerAirflow, meta(srv.URL), headers)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "[REDACTED]")
		assert.NotContains(t, err.Error(), "zq9")
	})

	t.Run("message", func(t *testing.T) {
		srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"state":"` + r.Header.Get("Authorization") + `"}`))
		})

		res, err := NewRunner().CheckStatus(context.Background(), types.TriggerAirflow, meta(srv.URL), headers)
		require.NoError(t, err)
		assert.Equal(t, "Bearer [REDACTED]", res.Message)
	})
}
