package lambda

import (
	"os"
	"strconv"
	"strings"
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

// ResolveSLADate returns the calendar date an absolute "HH:MM" SLA deadline
// applies to. Sensor-triggered daily pipelines (no cron) run T+1: data for
// date D arrives on D+1, so the deadline is on D+1. Cron pipelines and hourly
// ":MM" deadlines keep their own date. Composite hourly dates
// ("2006-01-02T15") and unparseable dates are returned unchanged.
func ResolveSLADate(cfg *types.PipelineConfig, date string) string {
	if cfg == nil || cfg.SLA == nil {
		return date
	}
	if cfg.Schedule.Cron != "" {
		return date
	}
	if strings.HasPrefix(cfg.SLA.Deadline, ":") {
		return date
	}
	t, err := time.Parse("2006-01-02", date)
	if err != nil {
		return date
	}
	return t.AddDate(0, 0, 1).Format("2006-01-02")
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
