//go:build e2e

package lambda_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/scheduler"

	"github.com/dwsmith1983/interlock/internal/e2ehooks"
	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
)

func e2eAWSConfig() aws.Config {
	return aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}
}

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

func TestApplyE2EHooks_SetsClockAndScheduler(t *testing.T) {
	startTestClock(t)
	t.Setenv("E2E_SCHEDULE_TABLE", "e2e-schedules")

	tests := []struct {
		name      string
		deps      func() *lambda.Deps
		wantTable bool
	}{
		{
			name: "store and scheduler rewired",
			deps: func() *lambda.Deps {
				return &lambda.Deps{
					Store:     &store.Store{},
					Scheduler: scheduler.New(scheduler.Options{Region: "us-east-1"}),
				}
			},
			wantTable: true,
		},
		{
			name:      "nil store and nil scheduler stay unset",
			deps:      func() *lambda.Deps { return &lambda.Deps{} },
			wantTable: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := tt.deps()

			lambda.ApplyE2EHooks(d, e2eAWSConfig())

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
			_, isTable := d.Scheduler.(*e2ehooks.TableScheduler)
			if isTable != tt.wantTable {
				t.Errorf("Scheduler is *TableScheduler = %v, want %v (got %T)", isTable, tt.wantTable, d.Scheduler)
			}
			if !tt.wantTable && d.Scheduler != nil {
				t.Errorf("Scheduler = %T, want nil", d.Scheduler)
			}
		})
	}
}

func TestApplyE2EHooks_PanicsWithoutScheduleTable(t *testing.T) {
	startTestClock(t)
	t.Setenv("E2E_SCHEDULE_TABLE", "")

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("ApplyE2EHooks did not panic with an empty E2E_SCHEDULE_TABLE")
		}
		if msg := fmt.Sprint(r); !strings.Contains(msg, "E2E_SCHEDULE_TABLE") {
			t.Errorf("panic message %q does not name E2E_SCHEDULE_TABLE", msg)
		}
	}()
	lambda.ApplyE2EHooks(&lambda.Deps{Scheduler: scheduler.New(scheduler.Options{Region: "us-east-1"})}, e2eAWSConfig())
}
