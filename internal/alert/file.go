package alert

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// FileSink appends alerts as JSON lines to a file.
type FileSink struct {
	path string
	mu   sync.Mutex
}

// NewFileSink creates a new file alert sink.
func NewFileSink(path string) (*FileSink, error) {
	// Ensure the file is writable
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening alert file: %w", err)
	}
	_ = f.Close()

	return &FileSink{path: path}, nil
}

// Name returns the sink identifier.
func (s *FileSink) Name() string { return "file" }

// Send appends the alert as a JSON line to the configured file.
func (s *FileSink) Send(_ context.Context, alert types.Alert) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}

	_, err = f.Write(append(data, '\n'))
	return err
}
