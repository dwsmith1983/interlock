package handler

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"github.com/dwsmith1983/interlock/internal/dlq"
)

// RecordProcessor handles a single DynamoDB stream record.
type RecordProcessor interface {
	Process(ctx context.Context, record events.DynamoDBEventRecord) error
}

// BatchStats tracks the outcome of batch processing for accounting invariants.
type BatchStats struct {
	Processed     int
	DLQRouted     int
	BatchFailures int
	Total         int
}

// ProcessBatch processes a slice of DynamoDB stream records, routing permanent
// errors to the DLQ and returning transient failures via ReportBatchItemFailures.
//
// Accounting invariant: Processed + DLQRouted + BatchFailures == Total
func ProcessBatch(
	ctx context.Context,
	records []events.DynamoDBEventRecord,
	processor RecordProcessor,
	router dlq.Router,
	logger *slog.Logger,
) (events.DynamoDBEventResponse, BatchStats) {
	stats := BatchStats{Total: len(records)}
	var failures []events.DynamoDBBatchItemFailure

	for _, rec := range records {
		err := processor.Process(ctx, rec)
		if err == nil {
			stats.Processed++
			continue
		}

		if dlq.Classify(err) == dlq.Permanent {
			original, marshalErr := json.Marshal(rec)
			if marshalErr != nil {
				logger.WarnContext(ctx, "failed to marshal original record", "event_id", rec.EventID, "error", marshalErr.Error())
			}
			dlqRecord := dlq.Record{
				ID:            dlq.GenerateID(),
				OriginalRecord: original,
				ErrorMessage:  err.Error(),
				ErrorType:     dlq.ErrorTypeUnknown,
				CorrelationID: rec.EventID,
			}

			routeErr := router.Route(ctx, dlqRecord)
			if routeErr != nil {
				logger.ErrorContext(ctx, "dlq routing failed, adding to batch failures",
					"event_id", rec.EventID,
					"sequence_number", rec.Change.SequenceNumber,
					"route_error", routeErr.Error(),
					"original_error", err.Error(),
				)
				failures = append(failures, events.DynamoDBBatchItemFailure{
					ItemIdentifier: rec.Change.SequenceNumber,
				})
				stats.BatchFailures++
			} else {
				stats.DLQRouted++
			}
		} else {
			failures = append(failures, events.DynamoDBBatchItemFailure{
				ItemIdentifier: rec.Change.SequenceNumber,
			})
			stats.BatchFailures++
		}
	}

	logger.InfoContext(ctx, "batch processing complete",
		"processed", stats.Processed,
		"dlq_routed", stats.DLQRouted,
		"batch_failures", stats.BatchFailures,
		"total", stats.Total,
		"invariant_holds", stats.Processed+stats.DLQRouted+stats.BatchFailures == stats.Total,
	)

	return events.DynamoDBEventResponse{
		BatchItemFailures: failures,
	}, stats
}
