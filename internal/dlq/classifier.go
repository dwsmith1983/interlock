package dlq

import (
	"context"
	"errors"
	"net"
)

// ErrorClass indicates whether a failure is retryable.
type ErrorClass int

const (
	// Transient errors are temporary and the operation may succeed on retry.
	Transient ErrorClass = iota
	// Permanent errors indicate the operation will never succeed without changes.
	Permanent
)

// Classify determines whether an error is transient (retryable) or permanent.
// A nil error is classified as Permanent since there is nothing to retry.
func Classify(err error) ErrorClass {
	if err == nil {
		return Permanent
	}

	// Context deadline / timeout signals are transient.
	if errors.Is(err, context.DeadlineExceeded) {
		return Transient
	}
	// Network errors that self-report as temporary are transient.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return Transient
	}

	// DNS errors flagged as temporary are transient.
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsTemporary {
		return Transient
	}

	return Permanent
}
