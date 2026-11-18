package lambda

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebTypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// testEventBridge is a local EventBridgeAPI implementation for white-box tests.
type testEventBridge struct {
	failedEntryCount int32
	calls            int
	err              error
}

func (t *testEventBridge) PutEvents(_ context.Context, _ *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	t.calls++
	if t.err != nil {
		return nil, t.err
	}
	if t.failedEntryCount > 0 {
		errCode := "InternalError"
		errMsg := "simulated partial failure"
		return &eventbridge.PutEventsOutput{
			FailedEntryCount: t.failedEntryCount,
			Entries: []ebTypes.PutEventsResultEntry{
				{ErrorCode: &errCode, ErrorMessage: &errMsg},
			},
		}, nil
	}
	return &eventbridge.PutEventsOutput{}, nil
}

func TestPublishEvent_PartialFailure(t *testing.T) {
	d := &Deps{
		EventBridge:  &testEventBridge{failedEntryCount: 1},
		EventBusName: "test-bus",
		NowFunc:      func() time.Time { return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) },
	}

	err := PublishEvent(context.Background(), d, "test.event", "pipeline-1", "cron", "2025-01-01", "test message")
	if err == nil {
		t.Fatal("expected error for partial failure, got nil")
	}
	if !strings.Contains(err.Error(), "partial failure") {
		t.Errorf("expected error to contain 'partial failure', got: %s", err.Error())
	}
	if !strings.Contains(err.Error(), "InternalError") {
		t.Errorf("expected error to contain 'InternalError', got: %s", err.Error())
	}
}

// TestPublishEvent_NoOpAndError: without a client or a bus name nothing is
// sent; a client error comes back wrapped with the event type.
func TestPublishEvent_NoOpAndError(t *testing.T) {
	now := func() time.Time { return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) }
	publish := func(d *Deps) error {
		return PublishEvent(context.Background(), d, "test.event", "pipeline-1", "cron", "2025-01-01", "test message")
	}

	if err := publish(&Deps{EventBusName: "test-bus", NowFunc: now}); err != nil {
		t.Errorf("nil EventBridge: err = %v, want nil", err)
	}

	eb := &testEventBridge{}
	if err := publish(&Deps{EventBridge: eb, NowFunc: now}); err != nil || eb.calls != 0 {
		t.Errorf("empty bus: err = %v, calls = %d, want nil and 0", err, eb.calls)
	}

	throttled := errors.New("throttled")
	err := publish(&Deps{EventBridge: &testEventBridge{err: throttled}, EventBusName: "test-bus", NowFunc: now})
	if err == nil || !strings.Contains(err.Error(), "publish test.event event:") || !errors.Is(err, throttled) {
		t.Errorf("client error: err = %v, want it to wrap %v as publish test.event event", err, throttled)
	}
}
