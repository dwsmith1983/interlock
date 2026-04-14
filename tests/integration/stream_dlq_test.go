package integration_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/internal/dlq"
	"github.com/dwsmith1983/interlock/internal/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test doubles ---

// transientErr satisfies net.Error with Timeout() == true so dlq.Classify returns Transient.
type transientErr struct{}

func (transientErr) Error() string   { return "transient timeout" }
func (transientErr) Timeout() bool   { return true }
func (transientErr) Temporary() bool { return true }

var _ net.Error = transientErr{}

// scenarioProcessor returns preconfigured errors keyed by EventID.
type scenarioProcessor struct {
	errors map[string]error
}

func (p *scenarioProcessor) Process(_ context.Context, rec events.DynamoDBEventRecord) error {
	if err, ok := p.errors[rec.EventID]; ok {
		return err
	}
	return nil
}

// recordingRouter tracks every record routed to the DLQ, optionally returning an error.
type recordingRouter struct {
	mu     sync.Mutex
	routed []dlq.Record
	err    error // if non-nil, Route returns this error for every call
}

func (r *recordingRouter) Route(_ context.Context, rec dlq.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routed = append(r.routed, rec)
	return r.err
}

func (r *recordingRouter) records() []dlq.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	dst := make([]dlq.Record, len(r.routed))
	copy(dst, r.routed)
	return dst
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// makeDynamoDBRecords creates DynamoDBEventRecord stubs with sequential event IDs.
func makeDynamoDBRecords(n int) []events.DynamoDBEventRecord {
	recs := make([]events.DynamoDBEventRecord, n)
	for i := range n {
		id := fmt.Sprintf("evt-%d", i)
		recs[i] = events.DynamoDBEventRecord{
			EventID: id,
			Change: events.DynamoDBStreamRecord{
				SequenceNumber: fmt.Sprintf("seq-%d", i),
			},
		}
	}
	return recs
}

func TestStreamDLQ_MixedBatch(t *testing.T) {
	// Arrange: 10 records — 5 valid, 3 permanent errors, 2 transient errors.
	records := makeDynamoDBRecords(10)

	procErrors := map[string]error{
		// Permanent errors (plain errors → Classify returns Permanent)
		"evt-1": errors.New("schema validation failed"),
		"evt-4": errors.New("missing required field"),
		"evt-7": errors.New("unsupported event type"),
		// Transient errors (net.Error with Timeout → Classify returns Transient)
		"evt-3": transientErr{},
		"evt-8": transientErr{},
	}
	processor := &scenarioProcessor{errors: procErrors}
	router := &recordingRouter{}

	// Act
	resp, stats := handler.ProcessBatch(
		context.Background(),
		records,
		processor,
		router,
		discardLogger,
	)

	// Assert: counts
	assert.Equal(t, 5, stats.Processed, "5 records should succeed")
	assert.Equal(t, 3, stats.DLQRouted, "3 permanent errors should be DLQ-routed")
	assert.Equal(t, 2, stats.BatchFailures, "2 transient errors should be batch failures")
	assert.Equal(t, 10, stats.Total, "total should be 10")

	// Assert: accounting invariant
	invariant := stats.Processed + stats.DLQRouted + stats.BatchFailures
	assert.Equal(t, stats.Total, invariant,
		"accounting invariant: processed + dlq + failures == total")

	// Assert: batch item failures contain exactly the transient records
	require.Len(t, resp.BatchItemFailures, 2)
	failedSeqs := make(map[string]bool)
	for _, f := range resp.BatchItemFailures {
		failedSeqs[f.ItemIdentifier] = true
	}
	assert.True(t, failedSeqs["seq-3"], "evt-3 (transient) should be in batch failures")
	assert.True(t, failedSeqs["seq-8"], "evt-8 (transient) should be in batch failures")

	// Assert: DLQ records have correlation IDs matching event IDs
	routed := router.records()
	require.Len(t, routed, 3)
	correlationIDs := make(map[string]bool)
	for _, rec := range routed {
		correlationIDs[rec.CorrelationID] = true
		assert.NotEmpty(t, rec.ID, "DLQ record should have a generated ID")
		assert.NotEmpty(t, rec.ErrorMessage, "DLQ record should carry the error message")
	}
	assert.True(t, correlationIDs["evt-1"], "DLQ record for evt-1 should have matching correlation ID")
	assert.True(t, correlationIDs["evt-4"], "DLQ record for evt-4 should have matching correlation ID")
	assert.True(t, correlationIDs["evt-7"], "DLQ record for evt-7 should have matching correlation ID")
}
