package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSSender is the subset of the SQS client that SQSRouter needs.
type SQSSender interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// SQSRouter routes dead-letter records to an SQS queue.
type SQSRouter struct {
	client   SQSSender
	queueURL string
	counter  Counter // optional; may be nil
}

// NewSQSRouter creates a Router backed by SQS.
// counter may be nil if metrics are not needed.
func NewSQSRouter(client SQSSender, queueURL string, counter Counter) (*SQSRouter, error) {
	if queueURL == "" {
		return nil, fmt.Errorf("dlq: queueURL must not be empty")
	}
	return &SQSRouter{
		client:   client,
		queueURL: queueURL,
		counter:  counter,
	}, nil
}

// Route serialises the record as JSON and sends it to the configured SQS queue
// with PipelineID and ErrorType as message attributes for server-side filtering.
//
// If SendMessage fails the full record payload is logged at Error level so that
// data is never silently lost.
func (r *SQSRouter) Route(ctx context.Context, record Record) error {
	if r.counter != nil {
		r.counter.Inc()
	}

	body, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("dlq: marshal record %s: %w", record.ID, err)
	}

	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(r.queueURL),
		MessageBody: aws.String(string(body)),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"PipelineID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(record.PipelineID),
			},
			"ErrorType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(record.ErrorType)),
			},
		},
	}

	if _, err := r.client.SendMessage(ctx, input); err != nil {
		slog.ErrorContext(ctx, "dlq: SQS SendMessage failed, logging full payload to prevent data loss",
			slog.String("record_id", record.ID.String()),
			slog.String("pipeline_id", record.PipelineID),
			slog.String("error", err.Error()),
			slog.String("payload", string(body)),
		)
		return fmt.Errorf("dlq: send record %s to SQS: %w", record.ID, err)
	}

	return nil
}
