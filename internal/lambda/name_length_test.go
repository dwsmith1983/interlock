package lambda_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dwsmith1983/interlock/internal/lambda"
	"github.com/dwsmith1983/interlock/internal/lambda/stream"
	"github.com/dwsmith1983/interlock/internal/store"
	"github.com/dwsmith1983/interlock/internal/store/storetest"
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
		require.Empty(t, validation.ValidatePipelineConfig(nameConfig(id)))

		d, mem, sfnM := newNameDeps(t, id, date, time.Unix(unix, 0))
		for attempt := 0; attempt < maxRerunAttempt; attempt++ { // earlier reruns, so this one is attempt 999
			mem.Put("rerun", map[string]ddbtypes.AttributeValue{"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)},
				"SK": &ddbtypes.AttributeValueMemberS{Value: types.RerunSK("stream", date, attempt)}})
		}
		require.NoError(t, lambda.StartSFN(context.Background(), d, nameConfig(id), id, "stream", date))
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
// start fails as AlreadyExists and that rerun never runs.
func TestRerunNamesDifferInOneSecond(t *testing.T) {
	const id, date = "orders", "2026-03-07"
	d, _, sfnM := newNameDeps(t, id, date, time.Unix(1772870400, 0))
	requestRerun(t, d, id, date, "manual")
	// The first rerun ends within the second, so the date is no longer
	// RUNNING. The stubbed per-source budget queries answer zero, so this
	// rerun is accepted too; CountReruns reads the first rerun's row from
	// Mem, which makes this one attempt 1.
	require.NoError(t, d.Store.SetTriggerStatus(context.Background(), id, "stream", date, types.TriggerStatusFailedFinal))
	requestRerun(t, d, id, date, "data-drift")

	assert.Equal(t, []string{"orders-stream-2026-03-07-rerun0-1772870400", "orders-stream-2026-03-07-rerun1-1772870400"}, executionNames(sfnM))
}

// nameSFN records the name of every execution it is asked to start.
type nameSFN struct {
	mu    sync.Mutex
	names []string
}

func (n *nameSFN) StartExecution(_ context.Context, in *sfn.StartExecutionInput, _ ...func(*sfn.Options)) (*sfn.StartExecutionOutput, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.names = append(n.names, *in.Name)
	return &sfn.StartExecutionOutput{}, nil
}

// nameConfig is a valid config for pipeline id.
func nameConfig(id string) *types.PipelineConfig {
	return &types.PipelineConfig{Pipeline: types.PipelineIdentity{ID: id},
		Schedule: types.ScheduleConfig{Trigger: &types.TriggerCondition{Key: "upstream", Check: types.CheckExists, Cutoff: "23:59", CutoffDayOffset: 7}}}
}

// newNameDeps returns deps fixed at now, with a config and a FAILED_FINAL
// trigger row for id and date, so a rerun request is accepted. Mem has no
// FilterExpression, so the per-source rerun budget queries answer zero;
// CountReruns has no filter and still counts Mem's rows.
func newNameDeps(t *testing.T, id, date string, now time.Time) (*lambda.Deps, *storetest.Mem, *nameSFN) {
	t.Helper()
	fake, mem := storetest.NewMem()
	query := fake.QueryFn
	fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
		if *in.TableName == "rerun" && in.FilterExpression != nil {
			return &dynamodb.QueryOutput{}, nil
		}
		return query(ctx, in)
	}
	cfg, err := json.Marshal(nameConfig(id))
	require.NoError(t, err)
	pk := &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(id)}
	mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": pk,
		"SK": &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK}, "config": &ddbtypes.AttributeValueMemberS{Value: string(cfg)}})
	mem.Put("control", map[string]ddbtypes.AttributeValue{"PK": pk,
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.TriggerSK("stream", date)},
		"status": &ddbtypes.AttributeValueMemberS{Value: types.TriggerStatusFailedFinal}})
	s, sfnN := storetest.NewStore(fake), &nameSFN{}
	return &lambda.Deps{Store: s, ConfigCache: store.NewConfigCache(s, 5*time.Minute), SFNClient: sfnN,
		NowFunc: func() time.Time { return now }, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}, mem, sfnN
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

func executionNames(sfnN *nameSFN) []string {
	sfnN.mu.Lock()
	defer sfnN.mu.Unlock()
	return append([]string(nil), sfnN.names...)
}
