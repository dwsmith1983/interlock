package lambda

import "time"

// Default timing constants for Step Function evaluation and job polling.
const (
	// DefaultEvalIntervalSec is the default evaluation loop interval (5 minutes).
	DefaultEvalIntervalSec = 300

	// DefaultEvalWindowSec is the default evaluation window (1 hour).
	DefaultEvalWindowSec = 3600

	// DefaultJobCheckIntervalSec is the default job status check interval (1 minute).
	DefaultJobCheckIntervalSec = 60

	// DefaultJobPollWindowSec is the default job poll window (1 hour).
	DefaultJobPollWindowSec = 3600

	// DefaultTriggerLockTTL is the default trigger lock duration when
	// SFN_TIMEOUT_SECONDS is not set.
	DefaultTriggerLockTTL = 4*time.Hour + 30*time.Minute

	// TriggerLockBuffer is the padding added to the SFN timeout to
	// derive the trigger lock TTL.
	TriggerLockBuffer = 30 * time.Minute

	// SFNExecNameMaxLen is the AWS limit for Step Function execution names.
	SFNExecNameMaxLen = 80
)
