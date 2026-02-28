package evaluator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	maxRetries     = 3
	retryBaseDelay = 500 * time.Millisecond
)

// HTTPRunner executes evaluators over HTTP instead of subprocesses.
// It satisfies the engine.TraitRunner interface for use in Lambda environments.
type HTTPRunner struct {
	client  *http.Client
	baseURL string
}

// NewHTTPRunner creates a new HTTP-based evaluator runner.
// If baseURL is provided, relative evaluator paths are resolved against it.
func NewHTTPRunner(baseURL string) *HTTPRunner {
	return &HTTPRunner{
		client:  &http.Client{Timeout: 60 * time.Second},
		baseURL: strings.TrimRight(baseURL, "/"),
	}
}

// Run calls the evaluator endpoint over HTTP with the given input.
// If evaluatorPath is a full URL (starts with http:// or https://), it is used directly.
// Otherwise, baseURL is prepended.
// Retryable HTTP errors (429, 502, 503, 504) are retried with exponential backoff.
func (r *HTTPRunner) Run(ctx context.Context, evaluatorPath string, input types.EvaluatorInput, timeout time.Duration) (*types.EvaluatorOutput, error) {
	url := evaluatorPath
	if !strings.HasPrefix(evaluatorPath, "http://") && !strings.HasPrefix(evaluatorPath, "https://") {
		if r.baseURL == "" {
			return nil, fmt.Errorf("evaluator path %q is not a URL and no base URL configured", evaluatorPath)
		}
		url = r.baseURL + "/" + strings.TrimLeft(evaluatorPath, "/")
	}

	body, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshaling evaluator input: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var lastStatus int
	var lastBody string

	for attempt := range maxRetries {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("creating request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := r.client.Do(req)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return &types.EvaluatorOutput{
					Status:          types.TraitError,
					Reason:          "EVALUATOR_TIMEOUT",
					FailureCategory: types.FailureTimeout,
				}, nil
			}
			return nil, fmt.Errorf("evaluator request failed: %w", err)
		}

		respBody, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading evaluator response: %w", err)
		}

		if resp.StatusCode < 400 {
			var output types.EvaluatorOutput
			if err := json.Unmarshal(respBody, &output); err != nil {
				return &types.EvaluatorOutput{
					Status:          types.TraitError,
					Reason:          fmt.Sprintf("EVALUATOR_OUTPUT_INVALID: %v (body: %s)", err, string(respBody)),
					FailureCategory: types.FailurePermanent,
				}, nil
			}
			return &output, nil
		}

		lastStatus = resp.StatusCode
		lastBody = string(respBody)

		if !isRetryableStatus(resp.StatusCode) || attempt == maxRetries-1 {
			break
		}

		delay := retryBaseDelay << attempt // 500ms, 1s, 2s
		slog.WarnContext(ctx, "retrying evaluator HTTP call",
			"url", url,
			"status", resp.StatusCode,
			"attempt", attempt+1,
			"delay", delay,
		)

		select {
		case <-ctx.Done():
			return &types.EvaluatorOutput{
				Status:          types.TraitError,
				Reason:          "EVALUATOR_TIMEOUT",
				FailureCategory: types.FailureTimeout,
			}, nil
		case <-time.After(delay):
		}
	}

	return &types.EvaluatorOutput{
		Status:          types.TraitError,
		Reason:          fmt.Sprintf("EVALUATOR_HTTP_ERROR: status %d: %s", lastStatus, lastBody),
		FailureCategory: classifyHTTPStatus(lastStatus),
	}, nil
}

func isRetryableStatus(code int) bool {
	return code == 429 || code == 502 || code == 503 || code == 504
}

func classifyHTTPStatus(code int) types.FailureCategory {
	if code == 429 || code >= 500 {
		return types.FailureTransient
	}
	return types.FailurePermanent
}
