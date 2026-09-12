// Package stream implements the DynamoDB stream-router Lambda handler.
// It processes stream events and routes each record to the appropriate
// handler based on the SK prefix.
package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-lambda-go/events"

	lambda "github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/telemetry"
	"github.com/dwsmith1983/interlock/pkg/types"
)

const (
	// removeEventName is the DynamoDB stream EventName for a deleted item.
	removeEventName = "REMOVE"
	// ttlServiceIdentity is the UserIdentity.Type DynamoDB sets on records it
	// deletes itself via the TTL process (PrincipalID "dynamodb.amazonaws.com").
	ttlServiceIdentity = "Service"
)

// isTTLExpiry reports whether a stream record was produced by DynamoDB's TTL
// deleter rather than by an application or an operator. UserIdentity is a
// pointer and is nil on every non-TTL record.
func isTTLExpiry(record events.DynamoDBEventRecord) bool {
	return record.UserIdentity != nil && record.UserIdentity.Type == ttlServiceIdentity
}

// HandleStreamEvent processes a DynamoDB stream event, routing each record
// to the appropriate handler based on the SK prefix. Per-record errors are
// collected as BatchItemFailures so the Lambda runtime can use DynamoDB's
// ReportBatchItemFailures to retry only the failed records.
func HandleStreamEvent(ctx context.Context, d *lambda.Deps, event lambda.StreamEvent) (events.DynamoDBEventResponse, error) {
	var resp events.DynamoDBEventResponse
	for i := range event.Records {
		recCtx := telemetry.WithCorrelationID(ctx, event.Records[i].EventID)
		if err := handleRecord(recCtx, d, event.Records[i]); err != nil {
			d.Logger.Error("stream record error",
				"error", err,
				"eventID", event.Records[i].EventID,
				"sequenceNumber", event.Records[i].Change.SequenceNumber,
			)
			// AWS matches each BatchItemFailures identifier against the record
			// SequenceNumber and checkpoints at the lowest one returned,
			// re-driving that record and every later record in the batch. An
			// unrecognised identifier (such as the EventID) instead falls back
			// to re-driving the entire batch.
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: event.Records[i].Change.SequenceNumber,
			})
		}
	}
	return resp, nil
}

// handleRecord extracts PK/SK and routes to the appropriate handler.
func handleRecord(ctx context.Context, d *lambda.Deps, record events.DynamoDBEventRecord) error {
	pk, sk := lambda.ExtractKeys(record)
	if pk == "" || sk == "" {
		return fmt.Errorf("record missing PK or SK")
	}

	// A deleted row carries no NewImage, so routing it makes the downstream
	// handlers act on absent data: a deleted RERUN_REQUEST# row would start a
	// rerun with reason "manual", and a deleted SENSOR# row would publish a
	// POST_RUN_* verdict. The one actionable delete is CONFIG: a pipeline
	// whose config disappeared must drop out of the cache. An empty EventName
	// (synthetic records) is treated as a write, as before.
	if record.EventName == removeEventName {
		if sk == types.ConfigSK {
			d.Logger.Info("config deleted, invalidating cache", "pk", pk)
			d.ConfigCache.Invalidate()
			return nil
		}
		d.Logger.Info("skipping REMOVE stream record",
			"pk", pk,
			"sk", sk,
			"eventID", record.EventID,
			"ttlExpiry", isTTLExpiry(record),
		)
		return nil
	}

	switch {
	case strings.HasPrefix(sk, "SENSOR#"):
		return handleSensorEvent(ctx, d, pk, sk, record)
	case sk == types.ConfigSK:
		d.Logger.Info("config changed, invalidating cache", "pk", pk)
		d.ConfigCache.Invalidate()
		return nil
	case strings.HasPrefix(sk, "JOB#"):
		return handleJobLogEvent(ctx, d, pk, sk, record)
	case strings.HasPrefix(sk, "RERUN_REQUEST#"):
		return handleRerunRequest(ctx, d, pk, sk, record)
	default:
		return nil
	}
}
