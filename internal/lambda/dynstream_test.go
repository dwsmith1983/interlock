package lambda

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebTypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// testEventBridge is a local EventBridgeAPI implementation for white-box tests.
type testEventBridge struct {
	failedEntryCount int32
}

func (t *testEventBridge) PutEvents(_ context.Context, _ *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
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

	err := publishEvent(context.Background(), d, "test.event", "pipeline-1", "cron", "2025-01-01", "test message")
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
