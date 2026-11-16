package lambda_test

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/stream"
	"github.com/dwsmith1983/interlock/pkg/types"
	"github.com/dwsmith1983/interlock/pkg/validation"
)

// sfnNameRe is a subset of the Step Functions execution name alphabet.
var sfnNameRe = regexp.MustCompile(`^[0-9A-Za-z_.-]+$`)

// maxRerunAttempt has three digits, far beyond any date's rerun budgets.
const maxRerunAttempt = 999

// TestNamesFitAtMaxInputs builds every live execution and schedule name from
// the longest inputs a config and the stream accept: a MaxPipelineIDLen ID,
// schedule "stream", an hourly date, a 10-digit Unix time, rerun attempt 999
// and a -r99 retry. Each must fit its AWS limit as built, and two IDs that
// differ only in their first character must never share a name.
func TestNamesFitAtMaxInputs(t *testing.T) {
	const date = "2026-03-03T10" // the longest date lambda.IsValidExecutionDate accepts
	const unix = 9999999999      // the last 10-digit Unix second
	seen := map[string]bool{}
	for _, first := range []string{"a", "b"} {
		id := first + strings.Repeat("x", validation.MaxPipelineIDLen-1)
		require.Empty(t, validation.ValidatePipelineConfig(&types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id}}))

		d, mock, sfnM := nameDeps(t, id, date, time.Unix(unix, 0))
		for attempt := 0; attempt < maxRerunAttempt; attempt++ { // earlier reruns, so this one is attempt 999
			mock.putRaw("rerun", map[string]ddbtypes.AttributeValue{"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
				"SK": &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", date, attempt)}})
		}
		require.NoError(t, lambda.StartSFN(context.Background(), d, &types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id}}, id, "stream", date))
		requestRerun(t, d, id, date, "operator asked: reload/fix #42")

		names := executionNames(sfnM)
		require.Equal(t, []string{
			id + "-stream-" + date + "-9999999999",
			id + "-stream-" + date + "-rerun999-9999999999", // no rerun reason in the name
		}, names)
		for _, name := range names {
			retry := lambda.NextExecName(name + "-r98")
			assert.Equal(t, name+"-r99", retry)
			for _, n := range []string{name, retry} {
				assert.LessOrEqual(t, len(n), lambda.SFNExecNameMaxLen, "execution name %q", n)
				assert.Regexp(t, sfnNameRe, n)
				assert.False(t, seen[n], "execution name %q built twice", n)
				seen[n] = true
			}
		}

		for _, suffix := range []string{"warning", "breach"} {
			name := lambda.SLAScheduleName(id, "stream", date, suffix)
			assert.Equal(t, id+"-stream-"+date+"-sla-"+suffix, name)
			assertValidSchedulerName(t, name)
		}
	}
}

// TestRerunNamesDifferInOneSecond: two accepted reruns of one pipeline and
// date in the same second must not share an execution name, or the second
// start fails as AlreadyExists and releases the lock the first run holds.
func TestRerunNamesDifferInOneSecond(t *testing.T) {
	const id, date = "orders", "2026-03-07"
	d, _, sfnM := nameDeps(t, id, date, time.Unix(1772870400, 0))
	requestRerun(t, d, id, date, "manual")
	requestRerun(t, d, id, date, "data-drift") // its own budget, so it is accepted too

	assert.Equal(t, []string{"orders-stream-2026-03-07-rerun0-1772870400", "orders-stream-2026-03-07-rerun1-1772870400"}, executionNames(sfnM))
}

// nameDeps returns deps fixed at now, with a config and trigger lock for id and date.
func nameDeps(t *testing.T, id, date string, now time.Time) (*lambda.Deps, *mockDDB, *mockSFN) {
	t.Helper()
	mock := newMockDDB()
	d, sfnM, _ := buildE2EDeps(mock, &mockTriggerRunner{}, &mockStatusPoller{})
	d.NowFunc = func() time.Time { return now }
	d.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	seedConfig(mock, types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id}})
	seedTriggerLock(mock, id, date)
	return d, mock, sfnM
}

// requestRerun sends a RERUN_REQUEST# record with reason through the stream router.
func requestRerun(t *testing.T, d *lambda.Deps, id, date, reason string) {
	t.Helper()
	keys := map[string]events.DynamoDBAttributeValue{"PK": events.NewStringAttribute(types.PipelinePK(id)),
		"SK": events.NewStringAttribute(types.RerunRequestSK("stream", date))}
	resp, err := stream.HandleStreamEvent(context.Background(), d, lambda.StreamEvent{Records: []events.DynamoDBEventRecord{{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{SequenceNumber: "1", Keys: keys, NewImage: map[string]events.DynamoDBAttributeValue{
			"PK": keys["PK"], "SK": keys["SK"], "reason": events.NewStringAttribute(reason),
		}},
	}}})
	require.NoError(t, err)
	require.Empty(t, resp.BatchItemFailures, "rerun %q", reason)
}

func executionNames(sfnM *mockSFN) (names []string) {
	sfnM.mu.Lock()
	defer sfnM.mu.Unlock()
	for _, in := range sfnM.executions {
		names = append(names, *in.Name)
	}
	return names
}
