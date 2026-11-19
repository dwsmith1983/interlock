//go:build e2e

// Package e2e drives the deployed Lambdas on LocalStack through the compose
// stack in docker-compose.yml (run it with `make e2e`). The helpers below
// write the inputs a producer would write, move the mock clock, stand in for
// the EventBridge rule and Scheduler the e2e deploy leaves out, and read back
// what the Lambdas did.
//
// Every scenario shares one stack and one forward-only clock. So each
// scenario uses its own pipeline ID and its own simulated day, scenarios run
// serially (never t.Parallel), and the order of the scenarios sets the clock.
package e2e

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// Resource names as deploy/localstack/deploy.py creates them (INTERLOCK_ENV=local).
const (
	prefix          = "local-interlock"
	controlTable    = prefix + "-control"
	eventsTable     = prefix + "-events"
	joblogTable     = prefix + "-joblog"
	schedulesTable  = prefix + "-schedules"
	alertQueue      = prefix + "-alerts"
	watchdogFn      = prefix + "-watchdog"
	orchestratorFn  = prefix + "-orchestrator"
	stateMachineARN = "arn:aws:states:us-east-1:000000000000:stateMachine:" + prefix + "-pipeline"

	// mockURLForLambdas is the mock as the Lambda containers reach it.
	mockURLForLambdas = "http://mock:8080"

	pollInterval     = 500 * time.Millisecond
	readyTimeout     = 2 * time.Minute
	callTimeout      = 30 * time.Second
	idleTimeout      = 2 * time.Minute // for running executions to park in a Wait state
	alertWaitSeconds = 1               // SQS long-poll wait per receive

	lambdaLogPrefix = "/aws/lambda/"
	logTailLines    = 200 // per Lambda log group, in the dump after a failure
	logDumpTimeout  = time.Minute

	jobRowTTL = 30 * 24 * time.Hour // as store.WriteJobEvent sets it
)

var (
	mockURL   string
	ddb       *dynamodb.Client
	lambdaAPI *lambda.Client
	sfnAPI    *sfn.Client
	sqsAPI    *sqs.Client
	logsAPI   *cloudwatchlogs.Client
)

// TestMain refuses to run outside the compose stack, then waits for
// LocalStack, the mock and the stream mappings deploy.py created. When a test
// fails it prints the tail of every Lambda's log: the Lambdas run in
// containers LocalStack starts, so their output is not in the compose logs.
func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
	code := m.Run()
	if code != 0 {
		dumpLambdaLogs(os.Stdout)
	}
	os.Exit(code)
}

// dumpLambdaLogs writes the last logTailLines events of each Lambda log group
// to w. It reports errors instead of returning them: the run already failed.
func dumpLambdaLogs(w io.Writer) {
	ctx, cancel := context.WithTimeout(context.Background(), logDumpTimeout)
	defer cancel()
	groups := cloudwatchlogs.NewDescribeLogGroupsPaginator(logsAPI, &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(lambdaLogPrefix),
	})
	for groups.HasMorePages() {
		page, err := groups.NextPage(ctx)
		if err != nil {
			_, _ = fmt.Fprintf(w, "e2e: list Lambda log groups: %v\n", err)
			return
		}
		for _, g := range page.LogGroups {
			dumpLogGroup(ctx, w, aws.ToString(g.LogGroupName))
		}
	}
}

// dumpLogGroup writes the group's last logTailLines events, oldest first.
func dumpLogGroup(ctx context.Context, w io.Writer, group string) {
	type line struct {
		at  int64
		msg string
	}
	var lines []line
	p := cloudwatchlogs.NewFilterLogEventsPaginator(logsAPI, &cloudwatchlogs.FilterLogEventsInput{LogGroupName: aws.String(group)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			_, _ = fmt.Fprintf(w, "e2e: read %s: %v\n", group, err)
			break
		}
		for _, e := range page.Events {
			lines = append(lines, line{aws.ToInt64(e.Timestamp), strings.TrimRight(aws.ToString(e.Message), "\n")})
		}
	}
	slices.SortStableFunc(lines, func(a, b line) int { return cmp.Compare(a.at, b.at) })
	tail := lines[max(0, len(lines)-logTailLines):]
	_, _ = fmt.Fprintf(w, "=== %s: last %d of %d log events\n", group, len(tail), len(lines))
	for _, l := range tail {
		_, _ = fmt.Fprintln(w, l.msg)
	}
}

func setup() error {
	mockURL = os.Getenv("E2E_MOCK_URL")
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if mockURL == "" || endpoint == "" {
		return fmt.Errorf("E2E_MOCK_URL and AWS_ENDPOINT_URL must be set; run these tests with `make e2e`, not go test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), readyTimeout)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	ddb, lambdaAPI = dynamodb.NewFromConfig(cfg), lambda.NewFromConfig(cfg)
	sfnAPI, sqsAPI = sfn.NewFromConfig(cfg), sqs.NewFromConfig(cfg)
	logsAPI = cloudwatchlogs.NewFromConfig(cfg)

	checks := []struct {
		what  string
		ready func() error
	}{
		{"LocalStack", func() error { return httpOK(ctx, endpoint+"/_localstack/health") }},
		{"the mock", func() error { return httpOK(ctx, mockURL+"/healthz") }},
		{"enabled stream mappings", func() error { return mappingsEnabled(ctx) }},
	}
	for _, c := range checks {
		for err := c.ready(); err != nil; err = c.ready() {
			select {
			case <-ctx.Done():
				return fmt.Errorf("waiting for %s: %w", c.what, err)
			case <-time.After(pollInterval):
			}
		}
	}
	return nil
}

func httpOK(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// mappingsEnabled checks the stream mappings: with StartingPosition LATEST a
// write made before a mapping is enabled is never delivered.
func mappingsEnabled(ctx context.Context) error {
	out, err := lambdaAPI.ListEventSourceMappings(ctx, &lambda.ListEventSourceMappingsInput{})
	if err != nil {
		return err
	}
	if len(out.EventSourceMappings) == 0 {
		return fmt.Errorf("no event source mappings")
	}
	for _, m := range out.EventSourceMappings {
		if aws.ToString(m.State) != "Enabled" {
			return fmt.Errorf("mapping %s is %s", aws.ToString(m.UUID), aws.ToString(m.State))
		}
	}
	return nil
}

// callCtx bounds one helper's AWS or mock calls.
func callCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), callTimeout)
}

// mockDo sends a request to the mock and returns the body, failing the test
// on a transport error or a status other than want.
func mockDo(t *testing.T, method, path string, body any, want int) []byte {
	t.Helper()
	var r io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal %s %s body: %v", method, path, err)
		}
		r = bytes.NewReader(b)
	}
	ctx, cancel := callCtx()
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, mockURL+path, r)
	if err != nil {
		t.Fatalf("build %s %s: %v", method, path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: read body: %v", method, path, err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, want, out)
	}
	return out
}

// mockNow is the simulated time every Lambda reads.
func mockNow(t *testing.T) time.Time {
	t.Helper()
	body := mockDo(t, http.MethodGet, "/clock", nil, http.StatusOK)
	now, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(body)))
	if err != nil {
		t.Fatalf("parse mock clock %q: %v", body, err)
	}
	return now
}

// launches is the X-Interlock-Run-Id of each launch of the pipeline's http
// job, in launch order: its length is how many times the trigger hit the mock.
func launches(t *testing.T, pipeline string) []string {
	t.Helper()
	var job struct {
		RunIDs []string `json:"runIds"`
	}
	body := mockDo(t, http.MethodGet, "/jobs/"+pipeline, nil, http.StatusOK)
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatalf("decode mock job %s: %v: %s", pipeline, err, body)
	}
	return job.RunIDs
}

// reportJob writes the JOB# row an http job writes to report its result:
// SK JOB#<schedule>#<date>#<millis> with the simulated clock's millis, and
// the run ID the trigger sent. The row is on the stream schedule, the one of
// every pipeline without a cron; reportJobOn takes the schedule.
func reportJob(t *testing.T, pipeline, date, runID, event string) {
	t.Helper()
	reportJobOn(t, streamSchedule, pipeline, date, runID, event)
}

func reportJobOn(t *testing.T, schedule, pipeline, date, runID, event string) {
	t.Helper()
	now := mockNow(t)
	item := map[string]ddbtypes.AttributeValue{
		"PK":    &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
		"SK":    &ddbtypes.AttributeValueMemberS{Value: types.JobSK(schedule, date, strconv.FormatInt(now.UnixMilli(), 10))},
		"event": &ddbtypes.AttributeValueMemberS{Value: event},
		"runId": &ddbtypes.AttributeValueMemberS{Value: runID},
		"ttl":   &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(now.Add(jobRowTTL).Unix(), 10)},
	}
	ctx, cancel := callCtx()
	defer cancel()
	if _, err := ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(joblogTable), Item: item}); err != nil {
		t.Fatalf("report %s for %s run %s: %v", event, pipeline, runID, err)
	}
}

func executionARN(name string) string {
	return strings.Replace(stateMachineARN, ":stateMachine:", ":execution:", 1) + ":" + name
}

// executionStatus is the status of the state machine's execution name.
func executionStatus(t *testing.T, name string) sfntypes.ExecutionStatus {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := sfnAPI.DescribeExecution(ctx, &sfn.DescribeExecutionInput{ExecutionArn: aws.String(executionARN(name))})
	if err != nil {
		t.Fatalf("describe execution %s: %v", name, err)
	}
	return out.Status
}

// executionHistory is every history event of the execution name, oldest first.
func executionHistory(t *testing.T, name string) []sfntypes.HistoryEvent {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	var evs []sfntypes.HistoryEvent
	p := sfn.NewGetExecutionHistoryPaginator(sfnAPI, &sfn.GetExecutionHistoryInput{ExecutionArn: aws.String(executionARN(name))})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("history of execution %s: %v", name, err)
		}
		evs = append(evs, page.Events...)
	}
	return evs
}

// waitFor polls cond until it holds, failing the test after timeout. A
// t.Fatalf inside cond (from any helper it calls) ends the test at once, so
// one transient error aborts the wait instead of being retried.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(pollInterval)
	}
}

func putItem(t *testing.T, item map[string]ddbtypes.AttributeValue) {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	if _, err := ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(controlTable), Item: item}); err != nil {
		t.Fatalf("put %v into %s: %v", item["SK"], controlTable, err)
	}
}

// putConfig writes the CONFIG row the way Terraform's config loader does:
// the config as a JSON string attribute.
func putConfig(t *testing.T, cfg types.PipelineConfig) {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	putItem(t, map[string]ddbtypes.AttributeValue{
		"PK":     &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(cfg.Pipeline.ID)},
		"SK":     &ddbtypes.AttributeValueMemberS{Value: types.ConfigSK},
		"config": &ddbtypes.AttributeValueMemberS{Value: string(data)},
	})
}

// writeSensor writes a SENSOR# row the way store.WriteSensor does: the
// fields nested in a "data" map.
func writeSensor(t *testing.T, pipeline, key string, data map[string]interface{}) {
	t.Helper()
	item, err := attributevalue.MarshalMap(types.ControlRecord{
		PK: types.PipelinePK(pipeline), SK: types.SensorSK(key), Data: data,
	})
	if err != nil {
		t.Fatalf("marshal sensor %s: %v", key, err)
	}
	putItem(t, item)
}

// triggerStatus is the status of the pipeline's TRIGGER# row for date, or ""
// when there is none. Every e2e pipeline is sensor-triggered, so the row is
// on the stream schedule. A row whose status is missing or not a string fails
// the test.
func triggerStatus(t *testing.T, pipeline, date string) string {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	sk := types.TriggerSK(streamSchedule, date)
	out, err := ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(controlTable),
		ConsistentRead: aws.Bool(true),
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sk},
		},
	})
	if err != nil {
		t.Fatalf("get %s for %s: %v", sk, pipeline, err)
	}
	if out.Item == nil {
		return ""
	}
	s, ok := out.Item["status"].(*ddbtypes.AttributeValueMemberS)
	if !ok {
		t.Fatalf("%s for %s has status %#v, want a string", sk, pipeline, out.Item["status"])
	}
	return s.Value
}

// jobEvents returns the event of every JOB# row in the job log for pipeline,
// oldest first: the sort key ends in the writer's (simulated) unix millis.
func jobEvents(t *testing.T, pipeline string) []string {
	t.Helper()
	rows := jobRows(t, pipeline)
	evs := make([]string, len(rows))
	for i, r := range rows {
		evs[i] = r.Event
	}
	return evs
}

// jobRow is the part of a JOB# row the scenarios check.
type jobRow struct {
	Event    string `dynamodbav:"event"`
	RunID    string `dynamodbav:"runId"`
	Error    string `dynamodbav:"error"`
	Category string `dynamodbav:"category"`
}

// jobRows returns every JOB# row in the job log for pipeline, oldest first.
func jobRows(t *testing.T, pipeline string) []jobRow {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := ddb.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(joblogTable),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
		},
	})
	if err != nil {
		t.Fatalf("query %s for %s: %v", joblogTable, pipeline, err)
	}
	var rows []jobRow
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &rows); err != nil {
		t.Fatalf("decode job log for %s: %v", pipeline, err)
	}
	return rows
}

// advanceTo moves the simulated clock forward to at. It first waits until
// the state machine has no Task in progress: every RUNNING execution is
// parked in a Wait state (its latest history event is WaitStateEntered). That
// still lets a scenario move the clock mid-run (an SLA warning while a job is
// polling). It does not cover the stream- and EventBridge-fed Lambdas that
// run outside the state machine (stream-router, event-sink):
// tests wait for the effects of that work themselves before advancing. A
// Wait can end on real time right after the check; with waits of seconds
// that window is small but not zero. A direct invoke the test makes is
// synchronous, so none is in flight here.
func advanceTo(t *testing.T, at time.Time) {
	t.Helper()
	deadline := time.Now().Add(idleTimeout)
	for busy := busyExecutions(t); len(busy) > 0; busy = busyExecutions(t) {
		if time.Now().After(deadline) {
			t.Fatalf("advanceTo %s: executions not parked in a Wait state after %s: %v",
				at.Format(time.RFC3339), idleTimeout, busy)
		}
		time.Sleep(pollInterval)
	}
	mockDo(t, http.MethodPost, "/clock/advance", map[string]string{"to": at.UTC().Format(time.RFC3339)}, http.StatusOK)
}

// busyExecutions lists the RUNNING executions whose latest history event is
// not WaitStateEntered, as "name (event type)".
func busyExecutions(t *testing.T) []string {
	t.Helper()
	var busy []string
	for _, e := range runningExecutions(t) {
		ctx, cancel := callCtx()
		out, err := sfnAPI.GetExecutionHistory(ctx, &sfn.GetExecutionHistoryInput{
			ExecutionArn: e.ExecutionArn, ReverseOrder: true, MaxResults: 1,
		})
		cancel()
		if err != nil {
			t.Fatalf("history of execution %s: %v", aws.ToString(e.Name), err)
		}
		latest := sfntypes.HistoryEventType("none")
		if len(out.Events) > 0 {
			latest = out.Events[0].Type
		}
		if latest != sfntypes.HistoryEventTypeWaitStateEntered {
			busy = append(busy, fmt.Sprintf("%s (%s)", aws.ToString(e.Name), latest))
		}
	}
	return busy
}

func runningExecutions(t *testing.T) []sfntypes.ExecutionListItem {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	var running []sfntypes.ExecutionListItem
	p := sfn.NewListExecutionsPaginator(sfnAPI, &sfn.ListExecutionsInput{
		StateMachineArn: aws.String(stateMachineARN), StatusFilter: sfntypes.ExecutionStatusRunning,
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("list running executions: %v", err)
		}
		running = append(running, page.Executions...)
	}
	return running
}

// executionNames returns the names of the state machine's executions, of any
// status, that start with prefix.
func executionNames(t *testing.T, prefix string) []string {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	var names []string
	p := sfn.NewListExecutionsPaginator(sfnAPI, &sfn.ListExecutionsInput{StateMachineArn: aws.String(stateMachineARN)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("list executions: %v", err)
		}
		for _, e := range page.Executions {
			if name := aws.ToString(e.Name); strings.HasPrefix(name, prefix) {
				names = append(names, name)
			}
		}
	}
	return names
}

// invoke calls a Lambda synchronously and fails on an invoke or function error.
func invoke(t *testing.T, fn string, payload []byte) {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := lambdaAPI.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String(fn), Payload: payload})
	if err != nil {
		t.Fatalf("invoke %s: %v", fn, err)
	}
	if out.FunctionError != nil {
		t.Fatalf("invoke %s: %s: %s", fn, aws.ToString(out.FunctionError), out.Payload)
	}
}

// invokeWatchdog runs one watchdog pass, standing in for its EventBridge rule.
func invokeWatchdog(t *testing.T) {
	t.Helper()
	invoke(t, watchdogFn, []byte("{}"))
}

// event is one row event-sink wrote to the events table.
type event struct {
	Type string
	Date string
	At   time.Time // the publisher's (simulated) clock
}

// events returns the pipeline's events oldest first. event-sink keys them
// PK=PIPELINE#<id>, SK=<unix millis>#<detail type>, so events stamped in the
// same simulated millisecond sort by type, and two of the same type in the
// same millisecond overwrite each other. Assert order only between events
// that are causally ordered.
func events(t *testing.T, pipeline string) []event {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	type row struct {
		EventType string `dynamodbav:"eventType"`
		Date      string `dynamodbav:"date"`
		Timestamp int64  `dynamodbav:"timestamp"`
	}
	var out []event
	p := dynamodb.NewQueryPaginator(ddb, &dynamodb.QueryInput{
		TableName:              aws.String(eventsTable),
		ConsistentRead:         aws.Bool(true),
		KeyConditionExpression: aws.String("PK = :pk"),
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":pk": &ddbtypes.AttributeValueMemberS{Value: types.PipelinePK(pipeline)},
		},
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("query events for %s: %v", pipeline, err)
		}
		var rows []row
		if err := attributevalue.UnmarshalListOfMaps(page.Items, &rows); err != nil {
			t.Fatalf("decode events for %s: %v", pipeline, err)
		}
		for _, r := range rows {
			out = append(out, event{Type: r.EventType, Date: r.Date, At: time.UnixMilli(r.Timestamp).UTC()})
		}
	}
	return out
}

func eventTypes(evs []event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

// alert is one message from the alerts queue, from its EventBridge envelope.
type alert struct {
	Pipeline string
	Date     string
	Type     string
}

// alertLog keeps every alert drained from the shared queue, so a scenario
// never loses one that another scenario's drain received.
var alertLog struct {
	mu      sync.Mutex
	entries []alert
}

// alerts drains the alerts queue into alertLog, deleting each message, and
// returns every alert seen so far for pipeline, oldest first. A drain stops
// only after two consecutive empty long-poll receives, so a test may treat
// an alert missing from the result as absent (once its publish has happened).
func alerts(t *testing.T, pipeline string) []alert {
	t.Helper()
	alertLog.mu.Lock()
	defer alertLog.mu.Unlock()
	ctx, cancel := callCtx()
	url, err := sqsAPI.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(alertQueue)})
	cancel()
	if err != nil {
		t.Fatalf("get %s url: %v", alertQueue, err)
	}
	for empty := 0; empty < 2; {
		if receiveAlerts(t, url.QueueUrl) == 0 {
			empty++
		} else {
			empty = 0
		}
	}
	var mine []alert
	for _, a := range alertLog.entries {
		if a.Pipeline == pipeline {
			mine = append(mine, a)
		}
	}
	return mine
}

// receiveAlerts does one long-poll receive, moves what it got into alertLog
// (alertLog.mu held) and deletes it, all under a fresh call context, and
// returns how many messages it received.
func receiveAlerts(t *testing.T, queueURL *string) int {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	out, err := sqsAPI.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: queueURL, MaxNumberOfMessages: 10, WaitTimeSeconds: alertWaitSeconds,
	})
	if err != nil {
		t.Fatalf("receive from %s: %v", alertQueue, err)
	}
	for _, msg := range out.Messages {
		alertLog.entries = append(alertLog.entries, parseAlert(t, aws.ToString(msg.Body)))
		if _, err := sqsAPI.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queueURL, ReceiptHandle: msg.ReceiptHandle}); err != nil {
			t.Fatalf("delete alert: %v", err)
		}
	}
	return len(out.Messages)
}

func parseAlert(t *testing.T, body string) alert {
	t.Helper()
	var env struct {
		DetailType string               `json:"detail-type"`
		Detail     types.InterlockEvent `json:"detail"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil || env.DetailType == "" {
		t.Fatalf("alert body is not an interlock EventBridge event (%v): %s", err, body)
	}
	if env.Detail.PipelineID == "" {
		t.Fatalf("alert %s has no pipelineId, so it would match no pipeline: %s", env.DetailType, body)
	}
	return alert{Pipeline: env.Detail.PipelineID, Date: env.Detail.Date, Type: env.DetailType}
}

// schedule is one item the e2ehooks TableScheduler recorded.
type schedule struct {
	Name       string `dynamodbav:"name"`
	Expression string `dynamodbav:"expression"`
	Timezone   string `dynamodbav:"timezone"`
	State      string `dynamodbav:"state"`
	TargetArn  string `dynamodbav:"targetArn"`
	Input      string `dynamodbav:"input"`
	at         time.Time
}

// fireDueSchedules stands in for EventBridge Scheduler for one pipeline's
// schedules: those whose target input names pipeline. Every one that is
// enabled and whose at(...) time is at or before the mock's now fires,
// earliest first (ties by name): the item is deleted (the stand-in does not
// record ActionAfterCompletion, and a failed invoke must not leave it for the
// next scenario), then its target is invoked with its input. An empty state
// counts as enabled, the Scheduler default; the Lambdas never set one. It
// returns how many of the pipeline's schedules it scanned and how many it
// fired. Other pipelines' schedules are left alone, so a scenario never
// fires, or counts, what another one left behind.
func fireDueSchedules(t *testing.T, pipeline string) (scanned, fired int) {
	t.Helper()
	due, scanned := dueSchedules(t, pipeline, mockNow(t))
	for _, s := range due {
		if s.TargetArn == "" {
			t.Fatalf("schedule %s has no targetArn", s.Name)
		}
		ctx, cancel := callCtx()
		_, err := ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(schedulesTable),
			Key:       map[string]ddbtypes.AttributeValue{"name": &ddbtypes.AttributeValueMemberS{Value: s.Name}},
		})
		cancel()
		if err != nil {
			t.Fatalf("delete schedule %s: %v", s.Name, err)
		}
		invoke(t, s.TargetArn, []byte(s.Input)) // its own call context
	}
	return scanned, len(due)
}

// dueSchedules scans the schedules table for pipeline's schedules and returns
// the enabled ones due at now, earliest first (ties by name), and how many of
// the pipeline's schedules it scanned. A schedule belongs to the pipeline
// named by the pipelineId in its target input (the SLA monitor payload); the
// name is no key, since long names are hashed.
func dueSchedules(t *testing.T, pipeline string, now time.Time) (due []schedule, scanned int) {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	p := dynamodb.NewScanPaginator(ddb, &dynamodb.ScanInput{TableName: aws.String(schedulesTable), ConsistentRead: aws.Bool(true)})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			t.Fatalf("scan %s: %v", schedulesTable, err)
		}
		var batch []schedule
		if err := attributevalue.UnmarshalListOfMaps(page.Items, &batch); err != nil {
			t.Fatalf("decode schedules: %v", err)
		}
		for _, s := range batch {
			if schedulePipeline(t, s) != pipeline {
				continue
			}
			scanned++
			if s.State != "" && s.State != "ENABLED" {
				continue
			}
			at, err := parseScheduleTime(s.Expression, s.Timezone)
			if err != nil {
				t.Fatalf("schedule %s: %v", s.Name, err)
			}
			if !at.After(now) {
				s.at = at
				due = append(due, s)
			}
		}
	}
	slices.SortFunc(due, func(a, b schedule) int {
		return cmp.Or(a.at.Compare(b.at), cmp.Compare(a.Name, b.Name))
	})
	return due, scanned
}

// schedulePipeline is the pipelineId in a schedule's target input. An input
// that is not JSON or names no pipeline fails the test.
func schedulePipeline(t *testing.T, s schedule) string {
	t.Helper()
	var in struct {
		PipelineID string `json:"pipelineId"`
	}
	if err := json.Unmarshal([]byte(s.Input), &in); err != nil || in.PipelineID == "" {
		t.Fatalf("schedule %s input names no pipeline (%v): %s", s.Name, err, s.Input)
	}
	return in.PipelineID
}

// parseScheduleTime reads a one-time "at(2006-01-02T15:04:05)" expression in
// the IANA zone tz (UTC when empty).
func parseScheduleTime(expr, tz string) (time.Time, error) {
	inner, ok := strings.CutPrefix(expr, "at(")
	inner, closed := strings.CutSuffix(inner, ")")
	if !ok || !closed {
		return time.Time{}, fmt.Errorf("%q is not an at(...) expression", expr)
	}
	loc := time.UTC
	if tz != "" {
		var err error
		if loc, err = time.LoadLocation(tz); err != nil {
			return time.Time{}, fmt.Errorf("timezone %q: %w", tz, err)
		}
	}
	at, err := time.ParseInLocation("2006-01-02T15:04:05", inner, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q: %w", expr, err)
	}
	return at, nil
}
