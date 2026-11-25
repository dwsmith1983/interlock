package store_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
)

// TestGetSensorsBetween pins the SENSOR# range query: both bounds inclusive,
// hourly keys on the last day included, other keys and pipelines left out,
// and every page read.
func TestGetSensorsBetween(t *testing.T) {
	const prefix = "first-sensor-arrival#"
	fake, _ := storetest.NewMem()
	s := storetest.NewStore(fake)
	data := func(v string) map[string]interface{} { return map[string]interface{}{"arrivedAt": v} }
	write := func(pipeline, key string) {
		t.Helper()
		if err := s.WriteSensor(context.Background(), pipeline, key, data(key)); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
	}
	for _, key := range []string{
		prefix + "2026-03-01T23", prefix + "2026-03-02", prefix + "2026-03-10", prefix + "2026-03-10T07",
		prefix + "2026-03-12T23", prefix + "2026-03-13", "first-sensor-arrival-x#2026-03-10", "upstream",
	} {
		write("p", key)
	}
	write("q", prefix+"2026-03-11")
	want := map[string]map[string]interface{}{}
	for _, date := range []string{"2026-03-02", "2026-03-10", "2026-03-10T07", "2026-03-12T23"} {
		want[prefix+date] = data(prefix + date)
	}

	query := fake.QueryFn
	for _, limit := range []int32{100, 2} { // rows per page
		fake.QueryFn = func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			in.Limit = aws.Int32(limit)
			return query(ctx, in)
		}
		got, err := s.GetSensorsBetween(context.Background(), "p", prefix+"2026-03-02", prefix+"2026-03-12")
		if err != nil {
			t.Fatalf("GetSensorsBetween with page limit %d: %v", limit, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("GetSensorsBetween with page limit %d = %v, want %v", limit, got, want)
		}
	}

	injected := errors.New("service unavailable")
	fake.QueryFn = func(context.Context, *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) { return nil, injected }
	_, err := s.GetSensorsBetween(context.Background(), "p", prefix+"2026-03-02", prefix+"2026-03-12")
	if !errors.Is(err, injected) || !strings.Contains(err.Error(), `"p"`) {
		t.Errorf("GetSensorsBetween error = %v, want %v wrapped with the pipeline", err, injected)
	}
}
