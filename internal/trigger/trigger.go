package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/dwsmith1983/interlock/internal/resilience"
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
// It uses an SSRF-safe transport that rejects private, loopback, and link-local addresses.
var defaultHTTPClient = &http.Client{
	Timeout:   defaultTriggerTimeout,
	Transport: newSSRFSafeTransport(),
}

// resolveHTTPClient returns a client with the given timeout in seconds. If
// timeoutSec is zero or matches the default, the shared defaultHTTPClient is
// returned to reuse connections. When a custom timeout is required, the returned
// client inherits the transport from defaultHTTPClient so that transport-level
// settings (including SSRF protection and test overrides) are preserved.
func resolveHTTPClient(timeoutSec int) *http.Client {
	if timeoutSec > 0 {
		timeout := time.Duration(timeoutSec) * time.Second
		if timeout != defaultTriggerTimeout {
			return &http.Client{
				Timeout:   timeout,
				Transport: defaultHTTPClient.Transport,
			}
		}
	}
	return defaultHTTPClient
}

// runField is one part of the run identity: the header that carries it on
// every http trigger request and the variable it expands in the body and
// header values.
type runField struct{ header, variable, value string }

func runFields(run types.RunIdentity) []runField {
	return []runField{
		{"X-Interlock-Pipeline-Id", "INTERLOCK_PIPELINE_ID", run.PipelineID},
		{"X-Interlock-Schedule-Id", "INTERLOCK_SCHEDULE_ID", run.ScheduleID},
		{"X-Interlock-Date", "INTERLOCK_DATE", run.Date},
		{"X-Interlock-Run-Id", "INTERLOCK_RUN_ID", run.RunID},
	}
}

// identityLookup resolves the run identity variables through format and any
// other variable through the INTERLOCK_ env allowlist.
func identityLookup(fields []runField, format func(string) string) func(string) string {
	return func(key string) string {
		for _, f := range fields {
			if key == f.variable {
				return format(f.value)
			}
		}
		return safeEnvLookup(key)
	}
}

// jsonStringContent is s encoded as a JSON string without the surrounding
// quotes, and without HTML escaping so "/", "<" and "&" stay literal.
func jsonStringContent(s string) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s) // encoding a string cannot fail
	out := strings.TrimSuffix(b.String(), "\n")
	return out[1 : len(out)-1]
}

// expandRunBody expands a configured JSON body: the run identity variables
// to this launch's values, JSON-escaped since identity values are free-form,
// and every other variable through the INTERLOCK_ env allowlist. A generated
// body is already marshalled and is returned as is.
func expandRunBody(body string, generated bool, run types.RunIdentity) string {
	if generated {
		return body
	}
	return os.Expand(body, identityLookup(runFields(run), jsonStringContent))
}

// ExecuteHTTP fires an HTTP trigger. A 2xx only means the job was accepted.
func ExecuteHTTP(ctx context.Context, cfg *types.HTTPTriggerConfig) error {
	method := cfg.Method
	if method == "" {
		method = "POST"
	}

	// The run identity variables expand to this launch's values, JSON-escaped
	// in the body since identity values are free-form; every other variable
	// follows the INTERLOCK_ env allowlist. A generated body is sent as is.
	fields := runFields(cfg.Run)
	headerLookup := identityLookup(fields, func(v string) string { return v })
	bodyStr := expandRunBody(cfg.Body, cfg.BodyGenerated, cfg.Run)

	var bodyReader io.Reader
	if bodyStr != "" {
		bodyReader = strings.NewReader(bodyStr)
	}

	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, bodyReader)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		req.Header.Set(k, os.Expand(v, headerLookup))
	}
	// Set after the configured headers so config cannot override them.
	for _, f := range fields {
		req.Header.Set(f.header, f.value)
	}

	httpClient := resolveHTTPClient(cfg.Timeout)

	var resp *http.Response
	var lastStatusCode int
	var lastBody []byte
	retryCfg := resilience.RetryConfig{
		MaxRetries:   2,
		BaseDelay:    200 * time.Millisecond,
		MaxDelay:     2 * time.Second,
		JitterFactor: 0.3,
	}

	err = resilience.Retry(ctx, retryCfg, func() error {
		// Reset request body for retry so retries don't send an empty body.
		if bodyStr != "" {
			req.Body = io.NopCloser(strings.NewReader(bodyStr))
		}

		var doErr error
		resp, doErr = httpClient.Do(req)
		if doErr != nil {
			return doErr // network errors are retried
		}

		if resp.StatusCode >= 500 {
			lastStatusCode = resp.StatusCode
			lastBody, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("server error %d", resp.StatusCode) // transient, retry
		}
		return nil // success or 4xx — stop retrying
	})

	if err != nil {
		if lastStatusCode >= 500 {
			msg := fmt.Sprintf("trigger returned status %d: %s", lastStatusCode, sanitizeBody(lastBody, secretValues(cfg.Headers, cfg.Body), maxErrorBodyBytes))
			return NewTriggerError(types.FailureTransient, msg, nil)
		}
		return fmt.Errorf("trigger request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		respBody, _ := io.ReadAll(resp.Body)
		msg := fmt.Sprintf("trigger returned status %d: %s", resp.StatusCode, sanitizeBody(respBody, secretValues(cfg.Headers, cfg.Body), maxErrorBodyBytes))
		return NewTriggerError(types.FailurePermanent, msg, nil)
	}

	return nil
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

// sanitizeBody strips control characters from a response body to prevent log
// injection, redacts values, and truncates the result to limit bytes.
// Redacting before the cut leaves no prefix of a value that crosses it.
func sanitizeBody(body []byte, values []string, limit int) string {
	s := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, string(body))
	return truncateUTF8(redact(s, values), limit)
}

// truncateUTF8 cuts s to at most n bytes without splitting a rune.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
