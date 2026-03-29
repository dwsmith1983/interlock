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
	"github.com/dwsmith1983/interlock/pkg/types"
)

// HandleStreamEvent processes a DynamoDB stream event, routing each record
// to the appropriate handler based on the SK prefix. Per-record errors are
// collected as BatchItemFailures so the Lambda runtime can use DynamoDB's
// ReportBatchItemFailures to retry only the failed records.
func HandleStreamEvent(ctx context.Context, d *lambda.Deps, event lambda.StreamEvent) (events.DynamoDBEventResponse, error) {
	var resp events.DynamoDBEventResponse
	for i := range event.Records {
		if err := handleRecord(ctx, d, event.Records[i]); err != nil {
			d.Logger.Error("stream record error",
				"error", err,
				"eventID", event.Records[i].EventID,
			)
			resp.BatchItemFailures = append(resp.BatchItemFailures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: event.Records[i].EventID,
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
