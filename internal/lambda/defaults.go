package lambda

import "time"

// Default timing constants for Step Function job polling.
const (
	// DefaultJobCheckIntervalSec is the default job status check interval (1 minute).
	DefaultJobCheckIntervalSec = 60

	// DefaultTriggerLockTTL is the default time from a run's latest attempt
	// to its trigger row's stale deadline (staleAt) when SFN_TIMEOUT_SECONDS
	// is not set.
	DefaultTriggerLockTTL = 4*time.Hour + 30*time.Minute

	// TriggerLockBuffer is the padding added to the SFN timeout to
	// derive the trigger row's stale deadline (staleAt).
	TriggerLockBuffer = 30 * time.Minute

	// SFNExecNameMaxLen is the AWS limit for Step Function execution names.
	SFNExecNameMaxLen = 80
)
