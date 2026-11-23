package store_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/dwsmith1983/interlock/internal/store/storetest"
)

// TestWriteSensorFieldIfAbsent: the field is added to an existing sensor
// row's data and the other fields are kept. A missing row, or a field that is
// already there, is refused with false and nothing changes.
func TestWriteSensorFieldIfAbsent(t *testing.T) {
	entry := map[string]interface{}{"count": 5000.0}
	other := map[string]interface{}{"count": 1.0}
	tests := []struct {
		name     string
		row      map[string]interface{} // the row's data; nil means no row
		want     bool
		wantData map[string]interface{} // nil means no row
	}{
		{name: "absent row", want: false},
		{name: "row without the field", row: map[string]interface{}{"a": other}, want: true,
			wantData: map[string]interface{}{"a": other, "b": entry}},
		{name: "row with empty data", row: map[string]interface{}{}, want: true,
			wantData: map[string]interface{}{"b": entry}},
		{name: "field already there", row: map[string]interface{}{"b": other}, want: false,
			wantData: map[string]interface{}{"b": other}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fake, _ := storetest.NewMem()
			s := storetest.NewStore(fake)
			if tt.row != nil {
				if err := s.WriteSensor(ctx, "p", "baseline", tt.row); err != nil {
					t.Fatal(err)
				}
			}

			got, err := s.WriteSensorFieldIfAbsent(ctx, "p", "baseline", "b", entry)
			if err != nil || got != tt.want {
				t.Fatalf("WriteSensorFieldIfAbsent = %v, %v; want %v, nil", got, err, tt.want)
			}
			data, err := s.GetSensorData(ctx, "p", "baseline")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(data, tt.wantData) {
				t.Errorf("data = %v, want %v", data, tt.wantData)
			}
		})
	}
}

// TestWriteSensorFieldIfAbsent_NoDataMap: WriteSensor with a nil map writes a
// row without data, and setting a field under it is an error, not a false
// return. The row is left as it was.
func TestWriteSensorFieldIfAbsent_NoDataMap(t *testing.T) {
	ctx := context.Background()
	fake, _ := storetest.NewMem()
	s := storetest.NewStore(fake)
	if err := s.WriteSensor(ctx, "p", "baseline", nil); err != nil {
		t.Fatal(err)
	}

	got, err := s.WriteSensorFieldIfAbsent(ctx, "p", "baseline", "b", map[string]interface{}{"count": 1.0})
	if got || err == nil {
		t.Fatalf("WriteSensorFieldIfAbsent = %v, %v; want false and an error", got, err)
	}
	sensors, err := s.GetAllSensors(ctx, "p")
	if err != nil {
		t.Fatal(err)
	}
	if data, ok := sensors["baseline"]; !ok || data != nil {
		t.Errorf("sensors = %v, want the baseline row still there without data", sensors)
	}
}

// TestWriteSensorFieldIfAbsent_ClientError: an error other than the condition
// failing is returned wrapped, naming the sensor.
func TestWriteSensorFieldIfAbsent_ClientError(t *testing.T) {
	boom := errors.New("boom")
	s := storetest.NewStore(&storetest.FakeDynamo{
		UpdateItemFn: func(context.Context, *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			return nil, boom
		},
	})

	got, err := s.WriteSensorFieldIfAbsent(context.Background(), "p", "baseline", "b", map[string]interface{}{"count": 1})
	if got || !errors.Is(err, boom) || !strings.Contains(err.Error(), "baseline") {
		t.Errorf("WriteSensorFieldIfAbsent = %v, %v; want false and the wrapped error naming the sensor", got, err)
	}
}
