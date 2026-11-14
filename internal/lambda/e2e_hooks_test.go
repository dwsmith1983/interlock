//go:build e2e

package lambda_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
)

// servedTime is what the test clock server always answers.
var servedTime = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

// startTestClock serves servedTime and points E2E_CLOCK_URL at it.
func startTestClock(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, servedTime.Format(time.RFC3339))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("E2E_CLOCK_URL", srv.URL)
}

func TestApplyE2EHooks_SetsClock(t *testing.T) {
	startTestClock(t)

	tests := []struct {
		name          string
		deps          func() *lambda.Deps
		wantStartedAt time.Time
	}{
		{
			name: "store and started-at rewired",
			deps: func() *lambda.Deps {
				return &lambda.Deps{
					Store:     &store.Store{},
					StartedAt: time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
				}
			},
			wantStartedAt: servedTime,
		},
		{
			name:          "nil store and zero started-at stay unset",
			deps:          func() *lambda.Deps { return &lambda.Deps{} },
			wantStartedAt: time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.deps()

			lambda.ApplyE2EHooks(d, aws.Config{})

			if got := d.Now(); !got.Equal(servedTime) {
				t.Errorf("d.Now() = %v, want %v", got, servedTime)
			}
			if d.Store != nil {
				if d.Store.Now == nil {
					t.Fatal("d.Store.Now not set")
				}
				if got := d.Store.Now(); !got.Equal(servedTime) {
					t.Errorf("d.Store.Now() = %v, want %v", got, servedTime)
				}
			}
			if !d.StartedAt.Equal(tt.wantStartedAt) {
				t.Errorf("d.StartedAt = %v, want %v", d.StartedAt, tt.wantStartedAt)
			}
		})
	}
}
