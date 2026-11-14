//go:build e2e

package lambda

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/dwsmith1983/interlock/internal/e2ehooks"
)

// ApplyE2EHooks rewires d for an e2e run. It reads time from the clock served
// at E2E_CLOCK_URL (for d and d.Store) and moves a non-zero d.StartedAt onto
// that clock so the watchdog compares like with like. It panics when
// E2E_CLOCK_URL is missing or the clock is unreachable: an e2e binary must
// never run on the wall clock.
func ApplyE2EHooks(d *Deps, _ aws.Config) {
	clock := e2ehooks.HTTPClock(os.Getenv("E2E_CLOCK_URL"))
	d.NowFunc = clock
	if d.Store != nil {
		d.Store.Now = clock
	}
	if !d.StartedAt.IsZero() {
		d.StartedAt = d.Now()
	}
}
