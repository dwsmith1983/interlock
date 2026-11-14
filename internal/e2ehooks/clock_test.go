//go:build e2e

package e2ehooks

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// servePlain returns a handler that answers every request with status and body.
func servePlain(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = fmt.Fprint(w, body)
	}
}

// recoverPanic runs fn and returns the recovered panic value as a string, or
// "" when fn returned normally.
func recoverPanic(fn func()) (msg string) {
	defer func() {
		if r := recover(); r != nil {
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return ""
}

func TestHTTPClock_ParsesRFC3339(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		want      time.Time
		wantPanic bool
	}{
		{
			name:   "rfc3339 with trailing newline",
			status: http.StatusOK,
			body:   "2026-03-10T12:00:00Z\n",
			want:   time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC),
		},
		{
			name:   "rfc3339nano with surrounding whitespace",
			status: http.StatusOK,
			body:   "  2026-03-10T12:00:00.123456789Z \n",
			want:   time.Date(2026, 3, 10, 12, 0, 0, 123456789, time.UTC),
		},
		{
			name:      "unparsable body at construction",
			status:    http.StatusOK,
			body:      "not a time",
			wantPanic: true,
		},
		{
			name:      "non-200 at construction",
			status:    http.StatusInternalServerError,
			body:      "2026-03-10T12:00:00Z",
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(servePlain(tt.status, tt.body))
			defer srv.Close()

			var clock func() time.Time
			msg := recoverPanic(func() { clock = HTTPClock(srv.URL) })

			if tt.wantPanic {
				if msg == "" {
					t.Fatal("HTTPClock did not panic")
				}
				if !strings.Contains(msg, "E2E_CLOCK_URL") {
					t.Errorf("panic message %q does not name E2E_CLOCK_URL", msg)
				}
				return
			}
			if msg != "" {
				t.Fatalf("HTTPClock panicked: %s", msg)
			}
			if got := clock(); !got.Equal(tt.want) {
				t.Errorf("clock() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHTTPClock_PanicsOnEmptyURL(t *testing.T) {
	msg := recoverPanic(func() { HTTPClock("") })
	if !strings.Contains(msg, "E2E_CLOCK_URL") {
		t.Errorf("panic message %q does not name E2E_CLOCK_URL", msg)
	}
}

func TestHTTPClock_ReflectsChangingTime(t *testing.T) {
	bodies := []string{
		"2026-03-10T12:00:00Z",
		"2026-03-10T12:00:00Z",
		"2026-03-10T12:05:00Z",
		"2026-03-11T00:00:00Z",
	}
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(calls.Add(1)) - 1
		if i >= len(bodies) {
			i = len(bodies) - 1
		}
		_, _ = fmt.Fprint(w, bodies[i])
	}))
	defer srv.Close()

	clock := HTTPClock(srv.URL) // consumes bodies[0]

	for i, body := range bodies[1:] {
		want, err := time.Parse(time.RFC3339, body)
		if err != nil {
			t.Fatal(err)
		}
		if got := clock(); !got.Equal(want) {
			t.Errorf("call %d: clock() = %v, want %v", i+1, got, want)
		}
	}
}

func TestHTTPClock_PanicsWhenUnreachableLater(t *testing.T) {
	srv := httptest.NewServer(servePlain(http.StatusOK, "2026-03-10T12:00:00Z"))
	defer srv.Close()

	// Swaps the global slog.Default, so this test must not use t.Parallel.
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(prev)

	clock := HTTPClock(srv.URL)
	srv.Close()

	msg := recoverPanic(func() { clock() })
	if msg == "" {
		t.Fatal("clock() did not panic after the endpoint went away")
	}
	if !strings.Contains(msg, "E2E_CLOCK_URL") {
		t.Errorf("panic message %q does not name E2E_CLOCK_URL", msg)
	}
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("no error-level log before the panic; logs: %q", logs.String())
	}
}
