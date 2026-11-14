//go:build e2e

// Package e2ehooks holds stand-ins that let the e2e harness drive the Lambdas
// on a simulated clock. It is compiled only with -tags e2e; production
// binaries never link it.
package e2ehooks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// clockTimeout bounds each request to the clock endpoint.
const clockTimeout = 2 * time.Second

// maxClockBody caps how much of the clock response is read.
const maxClockBody = 256

// HTTPClock returns a clock that reads the current time from url on every
// call. The endpoint answers GET with a plain-text RFC3339 or RFC3339Nano
// timestamp; surrounding whitespace is allowed.
//
// HTTPClock calls the endpoint once up front and panics if url is empty or
// the endpoint does not answer with a valid time. A later failure logs at
// error and panics. There is no fallback to the wall clock or to the last
// value: either would silently corrupt a simulated run, whereas a panic fails
// the invocation and the caller retries it.
func HTTPClock(url string) func() time.Time {
	if url == "" {
		panic("e2ehooks: E2E_CLOCK_URL is empty")
	}
	if _, err := fetchTime(url); err != nil {
		panic(fmt.Sprintf("e2ehooks: clock at E2E_CLOCK_URL=%s is not usable: %v", url, err))
	}
	return func() time.Time {
		t, err := fetchTime(url)
		if err != nil {
			slog.Default().Error("e2e clock read failed", "url", url, "error", err)
			panic(fmt.Sprintf("e2ehooks: read clock at E2E_CLOCK_URL=%s: %v", url, err))
		}
		return t
	}
}

// fetchTime does one GET against url, bounded by clockTimeout, and parses
// the body as a timestamp.
func fetchTime(url string) (time.Time, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clockTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return time.Time{}, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("get: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxClockBody))
	if err != nil {
		return time.Time{}, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	// time.RFC3339Nano parsing also accepts RFC3339 without fractional seconds.
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(body)))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse body: %w", err)
	}
	return t, nil
}
