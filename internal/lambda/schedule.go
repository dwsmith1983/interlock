package lambda

import (
	"os"
	"strconv"
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// ResolveScheduleID returns "cron" if the pipeline uses a cron schedule,
// otherwise returns "stream".
func ResolveScheduleID(cfg *types.PipelineConfig) string {
	if cfg.Schedule.Cron != "" {
		return "cron"
	}
	return "stream"
}

// ResolveTriggerLockTTL returns the trigger lock TTL based on the
// SFN_TIMEOUT_SECONDS env var plus a 30-minute buffer. Defaults to
// 4h30m if the env var is not set or invalid.
func ResolveTriggerLockTTL() time.Duration {
	s := os.Getenv("SFN_TIMEOUT_SECONDS")
	if s == "" {
		return DefaultTriggerLockTTL
	}
	sec, err := strconv.Atoi(s)
	if err != nil || sec <= 0 {
		return DefaultTriggerLockTTL
	}
	return time.Duration(sec)*time.Second + TriggerLockBuffer
}
