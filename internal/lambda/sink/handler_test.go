package sink_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/sink"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
	"github.com/dwsmith1983/interlock/pkg/types"
)

var sinkNow = time.Date(2026, 3, 4, 10, 16, 0, 0, time.UTC)

func newSinkDeps(ttlDays int) (*lambda.Deps, *storetest.FakeDynamo, *storetest.Mem) {
	fake, mem := storetest.NewMem()
	return &lambda.Deps{
		Store:         storetest.NewStore(fake),
		EventsTTLDays: ttlDays,
		NowFunc:       func() time.Time { return sinkNow },
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, fake, mem
}

func sinkInput(t *testing.T, detailType string, detail types.InterlockEvent) lambda.EventBridgeInput {
	t.Helper()
	detailJSON, err := json.Marshal(detail)
	require.NoError(t, err)
	return lambda.EventBridgeInput{Source: "interlock", DetailType: detailType, Detail: detailJSON}
}

func breach(ts time.Time) types.InterlockEvent {
	return types.InterlockEvent{
		PipelineID: "silver-cdr-hour", ScheduleID: "hourly", Date: "2026-03-04T10",
		Message: "SLA breach for silver-cdr-hour", Timestamp: ts,
	}
}

// soleEvent returns the one item in the events table.
func soleEvent(t *testing.T, mem *storetest.Mem) map[string]ddbtypes.AttributeValue {
	t.Helper()
	items := mem.Items("events")
	require.Len(t, items, 1)
	return items[0]
}

func TestHandleEventSink(t *testing.T) {
	t.Run("writes the event", func(t *testing.T) {
		d, _, mem := newSinkDeps(90)
		in := sinkInput(t, "SLA_BREACH", breach(time.Date(2026, 3, 4, 10, 15, 0, 0, time.UTC)))

		require.NoError(t, sink.HandleEventSink(context.Background(), d, in))
		assert.Equal(t, map[string]ddbtypes.AttributeValue{
			"PK":         &ddbtypes.AttributeValueMemberS{Value: "PIPELINE#silver-cdr-hour"},
			"SK":         &ddbtypes.AttributeValueMemberS{Value: "1772619300000#SLA_BREACH"},
			"eventType":  &ddbtypes.AttributeValueMemberS{Value: "SLA_BREACH"},
			"pipelineId": &ddbtypes.AttributeValueMemberS{Value: "silver-cdr-hour"},
			"scheduleId": &ddbtypes.AttributeValueMemberS{Value: "hourly"},
			"date":       &ddbtypes.AttributeValueMemberS{Value: "2026-03-04T10"},
			"message":    &ddbtypes.AttributeValueMemberS{Value: "SLA breach for silver-cdr-hour"},
			"timestamp":  &ddbtypes.AttributeValueMemberN{Value: "1772619300000"},
			"ttl":        &ddbtypes.AttributeValueMemberN{Value: "1780395360"},
		}, soleEvent(t, mem))
	})

	t.Run("zero timestamp uses now", func(t *testing.T) {
		d, _, mem := newSinkDeps(90)

		require.NoError(t, sink.HandleEventSink(context.Background(), d, sinkInput(t, "SLA_BREACH", breach(time.Time{}))))
		item := soleEvent(t, mem)
		assert.Equal(t, &ddbtypes.AttributeValueMemberS{Value: "1772619360000#SLA_BREACH"}, item["SK"])
		assert.Equal(t, &ddbtypes.AttributeValueMemberN{Value: "1772619360000"}, item["timestamp"])
	})

	for _, tt := range []struct {
		name    string
		ttlDays int
		wantTTL string
	}{
		{"unset TTL days default to 90", 0, "1780395360"},
		{"TTL days 7", 7, "1773224160"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, _, mem := newSinkDeps(tt.ttlDays)

			require.NoError(t, sink.HandleEventSink(context.Background(), d, sinkInput(t, "SLA_BREACH", breach(sinkNow))))
			assert.Equal(t, &ddbtypes.AttributeValueMemberN{Value: tt.wantTTL}, soleEvent(t, mem)["ttl"])
		})
	}

	t.Run("invalid detail", func(t *testing.T) {
		d, _, mem := newSinkDeps(90)
		in := lambda.EventBridgeInput{Source: "interlock", DetailType: "SLA_BREACH", Detail: json.RawMessage("not valid json{{{")}

		err := sink.HandleEventSink(context.Background(), d, in)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unmarshal event detail")
		assert.Empty(t, mem.Items("events"))
	})

	t.Run("write error", func(t *testing.T) {
		d, fake, _ := newSinkDeps(90)
		fake.PutItemFn = func(context.Context, *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return nil, errors.New("dynamodb: provisioned throughput exceeded")
		}
		in := sinkInput(t, "JOB_COMPLETED", types.InterlockEvent{PipelineID: "test-pipe", ScheduleID: "stream", Date: "2026-03-04", Message: "test", Timestamp: sinkNow})

		err := sink.HandleEventSink(context.Background(), d, in)
		require.EqualError(t, err, "write event to events table (pipeline=test-pipe type=JOB_COMPLETED): dynamodb: provisioned throughput exceeded")
	})
}
