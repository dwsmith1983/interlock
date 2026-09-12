package stream

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

// discardLogger keeps test output readable; every routing branch logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestHandleStreamEvent_BatchFailureUsesSequenceNumber pins the AWS
// ReportBatchItemFailures contract: Lambda matches each returned
// ItemIdentifier against the stream record's SequenceNumber. An EventID is not
// a recognised identifier, so Lambda treats the whole batch as failed and
// re-drives records whose side effects (WriteJobEvent, PublishEvent) are not
// idempotent.
func TestHandleStreamEvent_BatchFailureUsesSequenceNumber(t *testing.T) {
	s := storetest.NewStore(&storetest.FakeDynamo{})
	d := &lambda.Deps{
		Store:       s,
		ConfigCache: store.NewConfigCache(s, 5*time.Minute),
		Logger:      discardLogger(),
	}

	// No Keys at all -> handleRecord returns "record missing PK or SK".
	bad := events.DynamoDBEventRecord{
		EventID:   "evt-1",
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "111",
			Keys:           map[string]events.DynamoDBAttributeValue{},
		},
	}
	// An unrouted SK prefix hits the default branch and returns nil without
	// touching the store.
	good := events.DynamoDBEventRecord{
		EventID:   "evt-2",
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: "222",
			Keys: map[string]events.DynamoDBAttributeValue{
				"PK": events.NewStringAttribute(types.PipelinePK("p")),
				"SK": events.NewStringAttribute("UNROUTED#1"),
			},
		},
	}

	resp, err := HandleStreamEvent(context.Background(), d, lambda.StreamEvent{
		Records: []events.DynamoDBEventRecord{bad, good},
	})
	if err != nil {
		t.Fatalf("HandleStreamEvent returned error: %v", err)
	}
	if len(resp.BatchItemFailures) != 1 {
		t.Fatalf("BatchItemFailures = %+v, want exactly 1 (only the keyless record)", resp.BatchItemFailures)
	}
	if got := resp.BatchItemFailures[0].ItemIdentifier; got != "111" {
		t.Errorf("ItemIdentifier = %q, want %q (the record SequenceNumber, not the EventID)", got, "111")
	}
}
