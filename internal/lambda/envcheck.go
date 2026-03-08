package lambda

import (
	"fmt"
	"os"
	"strings"
)

// requiredEnvVars maps each Lambda handler name to the environment variables
// it requires at startup. Missing vars cause a fail-fast with a clear message.
var requiredEnvVars = map[string][]string{
	"stream-router":    {"CONTROL_TABLE", "JOBLOG_TABLE", "RERUN_TABLE", "STATE_MACHINE_ARN", "EVENT_BUS_NAME"},
	"orchestrator":     {"CONTROL_TABLE", "JOBLOG_TABLE", "RERUN_TABLE", "STATE_MACHINE_ARN", "EVENT_BUS_NAME"},
	"watchdog":         {"CONTROL_TABLE", "JOBLOG_TABLE", "RERUN_TABLE", "EVENT_BUS_NAME"},
	"sla-monitor":      {"CONTROL_TABLE", "JOBLOG_TABLE", "RERUN_TABLE", "EVENT_BUS_NAME", "SLA_MONITOR_ARN", "SCHEDULER_ROLE_ARN", "SCHEDULER_GROUP_NAME"},
	"event-sink":       {"EVENTS_TABLE"},
	"alert-dispatcher": {"SLACK_CHANNEL_ID"},
}

// ValidateEnv checks that all required environment variables for the named
// handler are set and non-empty. Returns an error listing all missing vars,
// or nil if all are present.
func ValidateEnv(handler string) error {
	vars, ok := requiredEnvVars[handler]
	if !ok {
		return nil // unknown handler, skip validation
	}
	var missing []string
	for _, v := range vars {
		if os.Getenv(v) == "" {
			missing = append(missing, v)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s: missing required env vars: %s", handler, strings.Join(missing, ", "))
	}
	return nil
}
