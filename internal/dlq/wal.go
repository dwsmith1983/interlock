package dlq

import (
	"context"

	"github.com/oklog/ulid/v2"
)

// WALManager provides write-ahead log semantics for DLQ records.
// This is a stub to allow the package to compile; full implementation is a separate task.
type WALManager struct {
	path string
}

// NewWALManager creates a WALManager backed by the given file path.
func NewWALManager(path string) *WALManager {
	return &WALManager{path: path}
}

// Route appends a record to the write-ahead log.
func (w *WALManager) Route(_ context.Context, _ Record) error {
	return nil
}

// Ack marks a record as successfully processed.
func (w *WALManager) Ack(_ context.Context, _ ulid.ULID) error {
	return nil
}

// Reject marks a record as permanently failed.
func (w *WALManager) Reject(_ context.Context, _ ulid.ULID) error {
	return nil
}

// Pending returns the number of records that have not been acked or rejected.
func (w *WALManager) Pending() int64 {
	return 0
}

// Close flushes and closes the WAL.
func (w *WALManager) Close() {}
