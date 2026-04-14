package dlq

import "context"

// Router sends a failed Record to a dead-letter destination.
type Router interface {
	Route(ctx context.Context, record Record) error
}

// Counter is an optional metric sink incremented on every Route call.
type Counter interface {
	Inc()
}
