package alert

import (
	"strconv"
	"strings"
)

const (
	baseDelaySeconds = 60
	maxDelaySeconds  = 900

	// maxRetryAfterSeconds is the longest Retry-After the dispatcher honours.
	maxRetryAfterSeconds = 3600
)

// retryDelaySeconds is how long a message that was received receiveCount times
// is hidden before its next try: baseDelaySeconds doubling per try up to
// maxDelaySeconds, or retryAfter up to maxRetryAfterSeconds if that is longer.
// A jitter in [0, 1) then adds up to half of the doubling wait, so that messages
// that failed together do not come back together.
func retryDelaySeconds(receiveCount, retryAfter int, jitter float64) int32 {
	ladder := baseDelaySeconds
	for n := 1; n < receiveCount && ladder < maxDelaySeconds; n++ {
		ladder *= 2
	}
	ladder = min(ladder, maxDelaySeconds)
	wait := max(ladder, min(retryAfter, maxRetryAfterSeconds))
	return int32(wait + int(float64(ladder)*jitter/2))
}

// parseRetryAfter reads a Retry-After header given in whole seconds. Any other
// value, an HTTP date included, gives 0.
func parseRetryAfter(header string) int {
	seconds, err := strconv.Atoi(strings.TrimSpace(header))
	if err != nil {
		return 0
	}
	return max(seconds, 0)
}
