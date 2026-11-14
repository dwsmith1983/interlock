//go:build e2e

package lambda

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/e2ehooks"
)

// ApplyE2EHooks rewires d for an e2e run. It reads time from the clock served
// at E2E_CLOCK_URL (for d and d.Store), moves a non-zero d.StartedAt onto
// that clock so the watchdog compares like with like, and swaps a configured
// d.Scheduler for one that records schedules in the E2E_SCHEDULE_TABLE
// DynamoDB table. E2E_CLOCK_URL is always required; E2E_SCHEDULE_TABLE is
// required only by Lambdas that have a Scheduler (sla-monitor, watchdog). It
// panics when a required variable is missing or the clock is unreachable: an
// e2e binary must never run on the wall clock.
func ApplyE2EHooks(d *Deps, awsCfg aws.Config) {
	clock := e2ehooks.HTTPClock(os.Getenv("E2E_CLOCK_URL"))
	d.NowFunc = clock
	if d.Store != nil {
		d.Store.Now = clock
	}
	if !d.StartedAt.IsZero() {
		d.StartedAt = d.Now()
	}
	if d.Scheduler != nil {
		table := os.Getenv("E2E_SCHEDULE_TABLE")
		if table == "" {
			panic("lambda: E2E_SCHEDULE_TABLE is empty; an e2e build needs it to record schedules")
		}
		d.Scheduler = e2ehooks.NewTableScheduler(dynamodb.NewFromConfig(awsCfg), table)
	}
}
