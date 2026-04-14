package handler_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/internal/dlq"
	"github.com/dwsmith1983/interlock/internal/handler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockProcessor returns preconfigured errors keyed by EventID.
type mockProcessor struct {
	errors map[string]error
}

func (m *mockProcessor) Process(_ context.Context, rec events.DynamoDBEventRecord) error {
	if err, ok := m.errors[rec.EventID]; ok {
		return err
	}
	return nil
}

// mockRouter tracks routed records and optionally returns an error.
type mockRouter struct {
	routed []dlq.Record
	err    error
}

func (m *mockRouter) Route(_ context.Context, rec dlq.Record) error {
	m.routed = append(m.routed, rec)
	return m.err
}

func makeRecords(ids ...string) []events.DynamoDBEventRecord {
	recs := make([]events.DynamoDBEventRecord, len(ids))
	for i, id := range ids {
		recs[i] = events.DynamoDBEventRecord{
			EventID: id,
			Change: events.DynamoDBStreamRecord{
				SequenceNumber: "seq-" + id,
			},
		}
	}
	return recs
}

var discardLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// transientErr satisfies net.Error with Timeout() == true so dlq.Classify returns Transient.
type transientErr struct{}

func (transientErr) Error() string   { return "timeout" }
func (transientErr) Timeout() bool   { return true }
func (transientErr) Temporary() bool { return true }

var _ net.Error = transientErr{}

func TestProcessBatch_AllSucceed(t *testing.T) {
	records := makeRecords("e1", "e2", "e3")
	proc := &mockProcessor{errors: map[string]error{}}
	router := &mockRouter{}

	resp, stats := handler.ProcessBatch(context.Background(), records, proc, router, discardLogger)

	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, 3, stats.Processed)
	assert.Equal(t, 0, stats.DLQRouted)
	assert.Equal(t, 0, stats.BatchFailures)
	assert.Equal(t, 3, stats.Total)
	assert.Equal(t, stats.Total, stats.Processed+stats.DLQRouted+stats.BatchFailures)
}

func TestProcessBatch_PermanentError_RoutedToDLQ(t *testing.T) {
	records := makeRecords("e1", "e2")
	proc := &mockProcessor{errors: map[string]error{
		"e2": errors.New("validation failed"),
	}}
	router := &mockRouter{}

	resp, stats := handler.ProcessBatch(context.Background(), records, proc, router, discardLogger)

	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, 1, stats.Processed)
	assert.Equal(t, 1, stats.DLQRouted)
	assert.Equal(t, 0, stats.BatchFailures)
	require.Len(t, router.routed, 1)
	assert.Equal(t, "e2", router.routed[0].CorrelationID)
	assert.Equal(t, "validation failed", router.routed[0].ErrorMessage)
}

func TestProcessBatch_TransientError_InBatchFailures(t *testing.T) {
	records := makeRecords("e1", "e2")
	proc := &mockProcessor{errors: map[string]error{
		"e1": transientErr{},
	}}
	router := &mockRouter{}

	resp, stats := handler.ProcessBatch(context.Background(), records, proc, router, discardLogger)

	require.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "seq-e1", resp.BatchItemFailures[0].ItemIdentifier)
	assert.Equal(t, 1, stats.Processed)
	assert.Equal(t, 0, stats.DLQRouted)
	assert.Equal(t, 1, stats.BatchFailures)
}

func TestProcessBatch_MixedBatch(t *testing.T) {
	records := makeRecords("ok1", "perm1", "trans1", "ok2")
	proc := &mockProcessor{errors: map[string]error{
		"perm1":  errors.New("schema mismatch"),
		"trans1": transientErr{},
	}}
	router := &mockRouter{}

	resp, stats := handler.ProcessBatch(context.Background(), records, proc, router, discardLogger)

	require.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "seq-trans1", resp.BatchItemFailures[0].ItemIdentifier)
	assert.Equal(t, 2, stats.Processed)
	assert.Equal(t, 1, stats.DLQRouted)
	assert.Equal(t, 1, stats.BatchFailures)
	assert.Equal(t, 4, stats.Total)
	assert.Equal(t, stats.Total, stats.Processed+stats.DLQRouted+stats.BatchFailures)
}

func TestProcessBatch_DLQRoutingFailure_FallsBackToBatchFailures(t *testing.T) {
	records := makeRecords("e1")
	proc := &mockProcessor{errors: map[string]error{
		"e1": errors.New("permanent error"),
	}}
	router := &mockRouter{err: errors.New("sqs unavailable")}

	resp, stats := handler.ProcessBatch(context.Background(), records, proc, router, discardLogger)

	require.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "seq-e1", resp.BatchItemFailures[0].ItemIdentifier)
	assert.Equal(t, 0, stats.DLQRouted)
	assert.Equal(t, 1, stats.BatchFailures)
	assert.Equal(t, stats.Total, stats.Processed+stats.DLQRouted+stats.BatchFailures)
}
