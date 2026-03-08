package trigger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"
	"unicode"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// TriggerError is a typed error returned by trigger execution. It carries
// a failure category so callers can use errors.Is/As to inspect the cause.
type TriggerError struct {
	Category types.FailureCategory
	Message  string
	Err      error // underlying error, may be nil
}

func (e *TriggerError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *TriggerError) Unwrap() error {
	return e.Err
}

// NewTriggerError creates a TriggerError with the given category and message.
func NewTriggerError(category types.FailureCategory, message string, err error) *TriggerError {
	return &TriggerError{Category: category, Message: message, Err: err}
}

const maxErrorBodyBytes = 512

const defaultTriggerTimeout = 30 * time.Second

// defaultHTTPClient is shared across HTTP and Airflow triggers to reuse connections.
var defaultHTTPClient = &http.Client{Timeout: defaultTriggerTimeout}

// defaultRunner provides backward-compatible package-level functions.
var defaultRunner = NewRunner()

// Execute runs the appropriate trigger via the default Runner.
func Execute(ctx context.Context, cfg *types.TriggerConfig) (map[string]interface{}, error) {
	return defaultRunner.Execute(ctx, cfg)
}

// CheckStatus checks the status of an active run via the default Runner.
func CheckStatus(ctx context.Context, triggerType types.TriggerType, metadata map[string]interface{}, headers map[string]string) (StatusResult, error) {
	return defaultRunner.CheckStatus(ctx, triggerType, metadata, headers)
}

// ExecuteCommand runs a shell command trigger.
func ExecuteCommand(ctx context.Context, command string) error {
	if command == "" {
		return fmt.Errorf("trigger command is empty")
	}

	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// ExecuteHTTP fires an HTTP trigger.
func ExecuteHTTP(ctx context.Context, cfg *types.HTTPTriggerConfig) error {
	method := cfg.Method
	if method == "" {
		method = "POST"
	}

	var body io.Reader
	if cfg.Body != "" {
		body = bytes.NewBufferString(os.Expand(cfg.Body, safeEnvLookup))
	}

	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, body)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		req.Header.Set(k, os.Expand(v, safeEnvLookup))
	}

	client := defaultHTTPClient
	if cfg.Timeout > 0 {
		timeout := time.Duration(cfg.Timeout) * time.Second
		if timeout != defaultTriggerTimeout {
			client = &http.Client{Timeout: timeout}
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("trigger request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		msg := fmt.Sprintf("trigger returned status %d: %s", resp.StatusCode, sanitizeBody(respBody))
		category := types.FailureTransient
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			category = types.FailurePermanent
		}
		return NewTriggerError(category, msg, nil)
	}

	return nil
}

// ClassifyFailure categorizes a trigger execution error. If the error is a
// TriggerError (inspectable via errors.As), the embedded Category is returned
// directly. Otherwise, string-based heuristics are applied.
func ClassifyFailure(err error) types.FailureCategory {
	if err == nil {
		return ""
	}

	// Prefer typed TriggerError when available.
	var te *TriggerError
	if errors.As(err, &te) {
		return te.Category
	}

	if os.IsTimeout(err) || strings.Contains(err.Error(), "deadline exceeded") || strings.Contains(err.Error(), "context deadline") {
		return types.FailureTimeout
	}

	// HTTP 4xx errors are permanent (client errors)
	if strings.Contains(err.Error(), "status 4") {
		return types.FailurePermanent
	}

	// HTTP 5xx and network errors are transient
	return types.FailureTransient
}

// safeEnvLookup only resolves environment variables with the INTERLOCK_ prefix.
// This prevents accidental leakage of arbitrary env vars (e.g. AWS credentials)
// from the Lambda runtime.
func safeEnvLookup(key string) string {
	if strings.HasPrefix(key, "INTERLOCK_") {
		return os.Getenv(key)
	}
	return ""
}

// sanitizeBody truncates a response body to maxErrorBodyBytes and strips
// control characters to prevent log injection.
func sanitizeBody(body []byte) string {
	if len(body) > maxErrorBodyBytes {
		body = body[:maxErrorBodyBytes]
	}
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, string(body))
}
