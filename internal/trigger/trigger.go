package trigger

import (
	"context"
	"encoding/json"
	"errors"
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

// maxResponseBytes bounds every response body read: a Databricks runs/get for
// a multi-task job can run to hundreds of KiB.
const maxResponseBytes = 1 << 20

// errResponseTooLarge is fixed text, since the response may quote the request.
var errResponseTooLarge = errors.New("response exceeds 1 MiB")

// readBounded reads a response body of up to maxResponseBytes. A larger one is
// an error, not a truncated body that would fail to parse.
func readBounded(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxResponseBytes {
		return nil, errResponseTooLarge
	}
	return b, nil
}

const defaultTriggerTimeout = 30 * time.Second

// maxRedirects matches the http.Client default policy that sameOriginRedirect
// replaces.
const maxRedirects = 10

// defaultHTTPClient is shared across HTTP and Airflow triggers to reuse connections.
// It uses an SSRF-safe transport that rejects private, loopback, and link-local addresses.
var defaultHTTPClient = &http.Client{
	Timeout:       defaultTriggerTimeout,
	Transport:     newSSRFSafeTransport(),
	CheckRedirect: sameOriginRedirect,
}

// sameOriginRedirect follows a redirect only on the original request's Host
// (with port), keeping the previous hop's scheme or going from http to https,
// and keeping the method, so configured headers such as a token never go to
// another host or over plain http, and a POST is never turned into a GET
// without its body. A refused redirect returns the 3xx as the response.
func sameOriginRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	orig, prev := via[0], via[len(via)-1]
	upgrade := prev.URL.Scheme == "http" && req.URL.Scheme == "https"
	if req.URL.Host != orig.URL.Host || (req.URL.Scheme != prev.URL.Scheme && !upgrade) || req.Method != orig.Method {
		return http.ErrUseLastResponse
	}
	return nil
}

// failedStatus describes a response with status 300 or more, sanitized and
// redacted with values: a redirect sameOriginRedirect refused names its target
// (without userinfo or query) and how to fix it, any other status its code and
// body.
func failedStatus(resp *http.Response, body []byte, values []string) string {
	loc, err := resp.Location() // resolved against the request URL
	if resp.StatusCode >= 400 || err != nil {
		return fmt.Sprintf("returned status %d: %s", resp.StatusCode, sanitizeBody(body, values, maxErrorBodyBytes))
	}
	text := fmt.Sprintf("redirect to %s://%s%s refused (a redirect must keep the host, port and method and must not go "+
		"from https to http); point the trigger URL at the final endpoint", loc.Scheme, loc.Host, loc.Path)
	return sanitizeBody([]byte(text), values, maxErrorBodyBytes)
}

// resolveHTTPClient returns a client with the given timeout in seconds. If
// timeoutSec is zero or matches the default, the shared defaultHTTPClient is
// returned to reuse connections. When a custom timeout is required, the returned
// client inherits the transport and redirect policy from defaultHTTPClient so
// that SSRF protection, same-origin redirects and test overrides are preserved.
func resolveHTTPClient(timeoutSec int) *http.Client {
	if timeoutSec > 0 {
		timeout := time.Duration(timeoutSec) * time.Second
		if timeout != defaultTriggerTimeout {
			return &http.Client{
				Timeout:       timeout,
				Transport:     defaultHTTPClient.Transport,
				CheckRedirect: defaultHTTPClient.CheckRedirect,
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
			var readErr error
			if lastBody, readErr = readBounded(resp.Body); readErr != nil {
				lastBody = []byte(readErr.Error())
			}
			_ = resp.Body.Close()
			return fmt.Errorf("server error %d", resp.StatusCode) // transient, retry
		}
		return nil // success, a refused redirect or 4xx: stop retrying
	})

	if err != nil {
		if lastStatusCode >= 500 {
			msg := fmt.Sprintf("trigger returned status %d: %s", lastStatusCode, sanitizeBody(lastBody, secretValues(cfg.Headers, cfg.Body), maxErrorBodyBytes))
			return NewTriggerError(types.FailureTransient, msg, nil)
		}
		return fmt.Errorf("trigger request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// A 303 See Other to a POST (or other non-GET request) answers a processed
	// request: the job reports its own result, as after a 2xx, and the redirect
	// is not followed. A GET or HEAD follows a 303 like any other redirect, so
	// one that reaches here was refused.
	if resp.StatusCode == http.StatusSeeOther && method != http.MethodGet && method != http.MethodHead {
		return nil
	}
	// Any other 3xx here is a redirect sameOriginRedirect refused, so the
	// request was not handled.
	if resp.StatusCode >= 300 && resp.StatusCode < 500 {
		respBody, readErr := readBounded(resp.Body)
		if readErr != nil {
			respBody = []byte(readErr.Error())
		}
		msg := "trigger " + failedStatus(resp, respBody, secretValues(cfg.Headers, cfg.Body))
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
