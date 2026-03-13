// Package lambda - export_test.go exposes unexported symbols for unit testing.
// This file is compiled ONLY during test builds (the _test.go suffix applies
// even to files in the non-_test package when placed here).
package lambda

import (
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// IsExcludedDate re-exports isExcludedDate for white-box unit testing from
// the external test package (package lambda_test).
var IsExcludedDate func(cfg *types.PipelineConfig, dateStr string) bool = isExcludedDate

// ResolveTriggerDeadlineTime re-exports resolveTriggerDeadlineTime for
// white-box unit testing from the external test package (package lambda_test).
var ResolveTriggerDeadlineTime func(deadline, date, timezone string) time.Time = resolveTriggerDeadlineTime
