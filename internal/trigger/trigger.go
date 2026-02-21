// Package trigger implements pipeline trigger execution.
package trigger

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/interlock-systems/interlock/pkg/types"
)

// Execute runs the appropriate trigger based on configuration.
func Execute(ctx context.Context, cfg *types.TriggerConfig) error {
	if cfg == nil {
		return fmt.Errorf("no trigger configured")
	}

	switch cfg.Type {
	case types.TriggerCommand:
		return ExecuteCommand(ctx, cfg.Command)
	case types.TriggerHTTP:
		return ExecuteHTTP(ctx, cfg)
	default:
		return fmt.Errorf("unknown trigger type: %s", cfg.Type)
	}
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
func ExecuteHTTP(ctx context.Context, cfg *types.TriggerConfig) error {
	method := cfg.Method
	if method == "" {
		method = "POST"
	}

	var body io.Reader
	if cfg.Body != "" {
		body = bytes.NewBufferString(os.ExpandEnv(cfg.Body))
	}

	req, err := http.NewRequestWithContext(ctx, method, cfg.URL, body)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range cfg.Headers {
		req.Header.Set(k, os.ExpandEnv(v))
	}

	timeout := 30 * time.Second
	if cfg.Timeout > 0 {
		timeout = time.Duration(cfg.Timeout) * time.Second
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("trigger request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("trigger returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// ClassifyFailure categorizes a trigger execution error.
func ClassifyFailure(err error) types.FailureCategory {
	if err == nil {
		return ""
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
