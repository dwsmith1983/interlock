//go:build e2e

// Package e2e drives the deployed Lambdas on LocalStack through the compose
// stack in docker-compose.yml (run it with `make e2e`). The helpers below
// write the inputs a producer would write, read the mock clock and job, and
// read back what the Lambdas did.
//
// Every scenario shares one stack and one forward-only clock. So each
// scenario uses its own pipeline ID and its own simulated day, scenarios run
// serially (never t.Parallel), and the order of the scenarios sets the clock.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// Resource names as deploy/localstack/deploy.py creates them (INTERLOCK_ENV=local).
const (
	prefix       = "local-interlock"
	controlTable = prefix + "-control"

	// mockURLForLambdas is the mock as the Lambda containers reach it.
	mockURLForLambdas = "http://mock:8080"

	pollInterval = 500 * time.Millisecond
	readyTimeout = 2 * time.Minute
	callTimeout  = 30 * time.Second
)

var (
	mockURL   string
	ddb       *dynamodb.Client
	lambdaAPI *lambda.Client
)

// TestMain refuses to run outside the compose stack, then waits for
// LocalStack, the mock and the stream mappings deploy.py created.
func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
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

// launches is how many times the pipeline's http trigger hit the mock job.
func launches(t *testing.T, pipeline string) int {
	t.Helper()
	var job struct {
		Launches int `json:"launches"`
	}
	body := mockDo(t, http.MethodGet, "/jobs/"+pipeline, nil, http.StatusOK)
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatalf("decode mock job %s: %v: %s", pipeline, err, body)
	}
	return job.Launches
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

// putConfig writes the CONFIG row the way store.PutConfig does: the config
// as a JSON string attribute.
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

// triggerStatus is the status of the TRIGGER# row, or "" when there is none.
// A row whose status is missing or not a string fails the test.
func triggerStatus(t *testing.T, pipeline, schedule, date string) string {
	t.Helper()
	ctx, cancel := callCtx()
	defer cancel()
	sk := types.TriggerSK(schedule, date)
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
