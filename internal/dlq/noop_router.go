package dlq

import "context"

// NoopRouter is a Router that silently discards records.
// Useful for tests and dry-run configurations.
type NoopRouter struct{}

// Route always succeeds without side effects.
func (NoopRouter) Route(context.Context, Record) error { return nil }
