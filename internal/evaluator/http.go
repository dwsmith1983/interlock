package evaluator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
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
		client:  &http.Client{},
		baseURL: strings.TrimRight(baseURL, "/"),
	}
}

// Run calls the evaluator endpoint over HTTP with the given input.
// If evaluatorPath is a full URL (starts with http:// or https://), it is used directly.
// Otherwise, baseURL is prepended.
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

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return &types.EvaluatorOutput{
				Status:          types.TraitFail,
				Reason:          "EVALUATOR_TIMEOUT",
				FailureCategory: types.FailureTimeout,
			}, nil
		}
		return nil, fmt.Errorf("evaluator request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading evaluator response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return &types.EvaluatorOutput{
			Status:          types.TraitFail,
			Reason:          fmt.Sprintf("EVALUATOR_HTTP_ERROR: status %d: %s", resp.StatusCode, string(respBody)),
			FailureCategory: classifyHTTPStatus(resp.StatusCode),
		}, nil
	}

	var output types.EvaluatorOutput
	if err := json.Unmarshal(respBody, &output); err != nil {
		return &types.EvaluatorOutput{
			Status:          types.TraitFail,
			Reason:          fmt.Sprintf("EVALUATOR_OUTPUT_INVALID: %v (body: %s)", err, string(respBody)),
			FailureCategory: types.FailurePermanent,
		}, nil
	}

	return &output, nil
}

func classifyHTTPStatus(code int) types.FailureCategory {
	if code >= 400 && code < 500 {
		return types.FailurePermanent
	}
	return types.FailureTransient
}
