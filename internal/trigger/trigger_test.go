package trigger

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteCommand_Success(t *testing.T) {
	cfg := &types.TriggerConfig{
		Type:    types.TriggerCommand,
		Command: &types.CommandTriggerConfig{Command: "echo ok"},
	}
	_, err := Execute(context.Background(), cfg)
	require.NoError(t, err)
}

func TestExecuteCommand_Failure(t *testing.T) {
	cfg := &types.TriggerConfig{
		Type:    types.TriggerCommand,
		Command: &types.CommandTriggerConfig{Command: "false"},
	}
	_, err := Execute(context.Background(), cfg)
	assert.Error(t, err)
}

func TestExecuteHTTP_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "POST", URL: srv.URL},
	}
	_, err := Execute(context.Background(), cfg)
	require.NoError(t, err)
}

func TestClassifyFailure_Timeout(t *testing.T) {
	err := fmt.Errorf("context deadline exceeded")
	assert.Equal(t, types.FailureTimeout, ClassifyFailure(err))
}

func TestClassifyFailure_HTTP4xx(t *testing.T) {
	err := fmt.Errorf("trigger returned status 400: bad request")
	assert.Equal(t, types.FailurePermanent, ClassifyFailure(err))

	err = fmt.Errorf("trigger returned status 404: not found")
	assert.Equal(t, types.FailurePermanent, ClassifyFailure(err))
}

func TestClassifyFailure_HTTP5xx(t *testing.T) {
	err := fmt.Errorf("trigger returned status 500: internal error")
	assert.Equal(t, types.FailureTransient, ClassifyFailure(err))
}

func TestClassifyFailure_NetworkError(t *testing.T) {
	err := fmt.Errorf("connection refused")
	assert.Equal(t, types.FailureTransient, ClassifyFailure(err))
}

func TestClassifyFailure_Nil(t *testing.T) {
	assert.Equal(t, types.FailureCategory(""), ClassifyFailure(nil))
}

func TestExecuteHTTP_ErrorBodyTruncated(t *testing.T) {
	// Return a body larger than 512 bytes.
	longBody := strings.Repeat("x", 1024)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(longBody))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := Execute(context.Background(), cfg)
	require.Error(t, err)

	// Error message must contain the truncated body (512 bytes), not the full 1024.
	msg := err.Error()
	// The error format is "trigger returned status 500: <body>".
	const prefix = "trigger returned status 500: "
	if !strings.HasPrefix(msg, prefix) {
		t.Fatalf("unexpected error format: %s", msg)
	}
	bodyPart := msg[len(prefix):]
	if len(bodyPart) != maxErrorBodyBytes {
		t.Errorf("body length in error = %d, want %d", len(bodyPart), maxErrorBodyBytes)
	}
}

func TestExecuteHTTP_ErrorBodySanitized(t *testing.T) {
	// Return a body with control characters.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad\x00input\x1b[31mred"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := Execute(context.Background(), cfg)
	require.Error(t, err)

	msg := err.Error()
	assert.NotContains(t, msg, "\x00")
	assert.NotContains(t, msg, "\x1b")
	assert.Contains(t, msg, "badinput")
}

func TestSafeEnvLookup_AllowsInterlockPrefix(t *testing.T) {
	t.Setenv("INTERLOCK_API_KEY", "safe-value")

	result := safeEnvLookup("INTERLOCK_API_KEY")
	assert.Equal(t, "safe-value", result)
}

func TestSafeEnvLookup_BlocksNonInterlockVars(t *testing.T) {
	t.Setenv("AWS_SECRET_ACCESS_KEY", "should-not-leak")
	t.Setenv("HOME", "/home/test")

	assert.Equal(t, "", safeEnvLookup("AWS_SECRET_ACCESS_KEY"))
	assert.Equal(t, "", safeEnvLookup("HOME"))
	assert.Equal(t, "", safeEnvLookup("PATH"))
}

func TestExecuteHTTP_EnvExpansionRestricted(t *testing.T) {
	t.Setenv("INTERLOCK_TOKEN", "good-token")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "leaked")

	var receivedAuth string
	var receivedBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		body := make([]byte, 1024)
		n, _ := r.Body.Read(body)
		receivedBody = string(body[:n])
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{
			Method:  "POST",
			URL:     srv.URL,
			Headers: map[string]string{"Authorization": "Bearer ${INTERLOCK_TOKEN}"},
			Body:    `{"secret":"${AWS_SECRET_ACCESS_KEY}","token":"${INTERLOCK_TOKEN}"}`,
		},
	}
	_, err := Execute(context.Background(), cfg)
	require.NoError(t, err)

	// INTERLOCK_ prefixed vars should resolve.
	assert.Equal(t, "Bearer good-token", receivedAuth)
	assert.Contains(t, receivedBody, `"token":"good-token"`)

	// Non-INTERLOCK_ vars must NOT resolve (replaced with empty string).
	assert.NotContains(t, receivedBody, "leaked")
	assert.Contains(t, receivedBody, `"secret":""`)
}

func TestTriggerError_ErrorMessage(t *testing.T) {
	t.Run("with underlying error", func(t *testing.T) {
		underlying := fmt.Errorf("connection refused")
		te := NewTriggerError(types.FailureTransient, "trigger request failed", underlying)
		assert.Equal(t, "trigger request failed: connection refused", te.Error())
	})

	t.Run("without underlying error", func(t *testing.T) {
		te := NewTriggerError(types.FailurePermanent, "trigger returned status 400: bad request", nil)
		assert.Equal(t, "trigger returned status 400: bad request", te.Error())
	})
}

func TestTriggerError_Unwrap(t *testing.T) {
	underlying := fmt.Errorf("timeout")
	te := NewTriggerError(types.FailureTimeout, "request timed out", underlying)
	assert.Equal(t, underlying, errors.Unwrap(te))
}

func TestTriggerError_ErrorsAs(t *testing.T) {
	te := NewTriggerError(types.FailurePermanent, "status 404", nil)
	wrapped := fmt.Errorf("execute trigger: %w", te)

	var target *TriggerError
	require.True(t, errors.As(wrapped, &target))
	assert.Equal(t, types.FailurePermanent, target.Category)
	assert.Equal(t, "status 404", target.Message)
}

func TestClassifyFailure_TriggerError(t *testing.T) {
	t.Run("direct TriggerError", func(t *testing.T) {
		te := NewTriggerError(types.FailurePermanent, "status 400", nil)
		assert.Equal(t, types.FailurePermanent, ClassifyFailure(te))
	})

	t.Run("wrapped TriggerError", func(t *testing.T) {
		te := NewTriggerError(types.FailureTimeout, "timed out", nil)
		wrapped := fmt.Errorf("execute: %w", te)
		assert.Equal(t, types.FailureTimeout, ClassifyFailure(wrapped))
	})

	t.Run("transient TriggerError", func(t *testing.T) {
		te := NewTriggerError(types.FailureTransient, "status 500", nil)
		assert.Equal(t, types.FailureTransient, ClassifyFailure(te))
	})
}

func TestExecuteHTTP_Returns_TriggerError_On4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := Execute(context.Background(), cfg)
	require.Error(t, err)

	var te *TriggerError
	require.True(t, errors.As(err, &te), "expected TriggerError, got %T", err)
	assert.Equal(t, types.FailurePermanent, te.Category)
	assert.Contains(t, te.Message, "status 404")
}

func TestExecuteHTTP_Returns_TriggerError_On5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("service unavailable"))
	}))
	defer srv.Close()

	origClient := defaultHTTPClient
	defaultHTTPClient = srv.Client()
	defer func() { defaultHTTPClient = origClient }()

	cfg := &types.TriggerConfig{
		Type: types.TriggerHTTP,
		HTTP: &types.HTTPTriggerConfig{Method: "GET", URL: srv.URL},
	}
	_, err := Execute(context.Background(), cfg)
	require.Error(t, err)

	var te *TriggerError
	require.True(t, errors.As(err, &te), "expected TriggerError, got %T", err)
	assert.Equal(t, types.FailureTransient, te.Category)
	assert.Contains(t, te.Message, "status 503")
}

func TestExecuteCommand_EmptyCommand(t *testing.T) {
	err := ExecuteCommand(context.Background(), "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "command is empty")
}

func TestExecuteCommand_DirectExec(t *testing.T) {
	err := ExecuteCommand(context.Background(), "echo hello")
	require.NoError(t, err)
}

func TestExecuteCommand_NoShellMetacharacters(t *testing.T) {
	// The semicolon should be passed as a literal argument to echo, not
	// interpreted as a shell command separator. With direct exec there is
	// no shell to split on ";", so echo receives [";", "ls"] as arguments
	// and prints them literally. If a shell were involved, "ls" would
	// execute as a separate command.
	err := ExecuteCommand(context.Background(), "echo ; ls")
	require.NoError(t, err, "echo should succeed even with ; in args")
}
