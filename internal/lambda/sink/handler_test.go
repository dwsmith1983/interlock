package sink_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

// eventSK is the sort key an event is stored under.
func eventSK(t *testing.T, millisAndType string, event types.InterlockEvent) *ddbtypes.AttributeValueMemberS {
	t.Helper()
	eventJSON, err := json.Marshal(event)
	require.NoError(t, err)
	sum := sha256.Sum256(eventJSON)
	return &ddbtypes.AttributeValueMemberS{Value: millisAndType + "#" + hex.EncodeToString(sum[:])[:16]}
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
			"SK":         &ddbtypes.AttributeValueMemberS{Value: "1772619300000#SLA_BREACH#e984205bf84ea8f6"},
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
		assert.Equal(t, eventSK(t, "1772619360000#SLA_BREACH", breach(time.Time{})), item["SK"])
		assert.Equal(t, &ddbtypes.AttributeValueMemberN{Value: "1772619360000"}, item["timestamp"])
	})

	t.Run("stores the detail map", func(t *testing.T) {
		d, _, mem := newSinkDeps(90)
		in := lambda.EventBridgeInput{Source: "interlock", DetailType: "JOB_FAILED", Detail: json.RawMessage(
			`{"pipelineId":"silver-cdr-hour","timestamp":"2026-03-04T10:15:00Z","detail":{"error":"boom","retries":0,
			"delta":-1.5,"ok":true,"note":"","nothing":null,"nested":{"a":"b"},"list":["x",2]}}`)}

		require.NoError(t, sink.HandleEventSink(context.Background(), d, in))
		assert.Equal(t, &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
			"error":   &ddbtypes.AttributeValueMemberS{Value: "boom"},
			"retries": &ddbtypes.AttributeValueMemberN{Value: "0"},
			"delta":   &ddbtypes.AttributeValueMemberN{Value: "-1.5"},
			"ok":      &ddbtypes.AttributeValueMemberBOOL{Value: true},
			"note":    &ddbtypes.AttributeValueMemberS{Value: ""},
			"nothing": &ddbtypes.AttributeValueMemberNULL{Value: true},
			"nested": &ddbtypes.AttributeValueMemberM{Value: map[string]ddbtypes.AttributeValue{
				"a": &ddbtypes.AttributeValueMemberS{Value: "b"},
			}},
			"list": &ddbtypes.AttributeValueMemberL{Value: []ddbtypes.AttributeValue{
				&ddbtypes.AttributeValueMemberS{Value: "x"},
				&ddbtypes.AttributeValueMemberN{Value: "2"},
			}},
		}}, soleEvent(t, mem)["detail"])
	})

	t.Run("a detail that cannot be encoded is an error", func(t *testing.T) {
		d, _, mem := newSinkDeps(90)
		event := breach(sinkNow)
		event.Detail = map[string]interface{}{"": "no key"}

		err := sink.HandleEventSink(context.Background(), d, sinkInput(t, "SLA_BREACH", event))
		require.ErrorContains(t, err, "marshal event detail (pipeline=silver-cdr-hour type=SLA_BREACH)")
		assert.Empty(t, mem.Items("events"))
	})

	for name, change := range map[string]func(*types.InterlockEvent){
		"message":     func(e *types.InterlockEvent) { e.Message = "SLA breach again" },
		"date":        func(e *types.InterlockEvent) { e.Date = "2026-03-04T11" },
		"detail":      func(e *types.InterlockEvent) { e.Detail = map[string]interface{}{"status": "RUNNING"} },
		"microsecond": func(e *types.InterlockEvent) { e.Timestamp = e.Timestamp.Add(time.Microsecond) },
	} {
		t.Run("two events of one type in one millisecond both stay: "+name, func(t *testing.T) {
			d, _, mem := newSinkDeps(90)
			first, second := breach(sinkNow), breach(sinkNow)
			change(&second)

			for _, e := range []types.InterlockEvent{first, second} {
				require.NoError(t, sink.HandleEventSink(context.Background(), d, sinkInput(t, "SLA_BREACH", e)))
			}
			assert.Len(t, mem.Items("events"), 2)
		})
	}

	// The second delivery comes later and with the JSON laid out differently.
	t.Run("an event delivered twice writes one row", func(t *testing.T) {
		d, _, mem := newSinkDeps(90)
		in := lambda.EventBridgeInput{Source: "interlock", DetailType: "INFRA_ALARM", Detail: json.RawMessage(
			`{ "timestamp": "2026-03-04T10:15:00Z", "pipelineId": "INFRASTRUCTURE", "message": "sink-errors: ALARM", "detail": { "b": 1, "a": "x" } }`)}
		require.NoError(t, sink.HandleEventSink(context.Background(), d, in))

		var compact bytes.Buffer
		require.NoError(t, json.Compact(&compact, in.Detail))
		in.Detail = compact.Bytes()
		d.NowFunc = func() time.Time { return sinkNow.Add(time.Minute) }
		require.NoError(t, sink.HandleEventSink(context.Background(), d, in))
		assert.Equal(t, &ddbtypes.AttributeValueMemberN{Value: "1772619300000"}, soleEvent(t, mem)["timestamp"])
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
