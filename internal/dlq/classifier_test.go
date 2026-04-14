package dlq_test

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/dwsmith1983/interlock/internal/dlq"
)

func TestClassify_ErrorScenarios(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected dlq.ErrorClass
	}{
		{"Nil error should default to permanent", nil, dlq.Permanent},
		{"Context deadline exceeded", context.DeadlineExceeded, dlq.Transient},
		{"Transient network error", &net.DNSError{IsTemporary: true}, dlq.Transient},
		{"Unknown standard error", errors.New("database corrupted"), dlq.Permanent},
		{"Wrapped transient error", errors.Join(errors.New("wrap"), context.DeadlineExceeded), dlq.Transient},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dlq.Classify(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}
