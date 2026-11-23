package lambda

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/stretchr/testify/assert"
)

func TestExtractSensorData(t *testing.T) {
	pk, sk := events.NewStringAttribute("PIPELINE#p"), events.NewStringAttribute("SENSOR#s")
	ready := events.NewStringAttribute("ready")
	tests := []struct {
		name  string
		image map[string]events.DynamoDBAttributeValue
		want  map[string]interface{}
	}{
		{"NilImage", nil, nil},
		{"DataMapUnwrapped", map[string]events.DynamoDBAttributeValue{"PK": pk, "SK": sk,
			"data": events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{"status": ready})}, map[string]interface{}{"status": "ready"}},
		{"FlatFields", map[string]events.DynamoDBAttributeValue{"PK": pk, "SK": sk, "status": ready}, map[string]interface{}{"status": "ready"}},
		{"TTLSkipped", map[string]events.DynamoDBAttributeValue{"PK": pk, "SK": sk,
			"ttl": events.NewNumberAttribute("1709280000"), "status": ready}, map[string]interface{}{"status": "ready"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ExtractSensorData(tt.image))
		})
	}
}

func TestConvertAttributeValue(t *testing.T) {
	tests := []struct {
		name string
		av   events.DynamoDBAttributeValue
		want interface{}
	}{
		{"String", events.NewStringAttribute("ready"), "ready"},
		{"Integer", events.NewNumberAttribute("10"), float64(10)},
		{"Float", events.NewNumberAttribute("1.5"), 1.5},
		{"Bool", events.NewBooleanAttribute(true), true},
		{"Map", events.NewMapAttribute(map[string]events.DynamoDBAttributeValue{"inner": events.NewStringAttribute("value")}),
			map[string]interface{}{"inner": "value"}},
		{"List", events.NewListAttribute([]events.DynamoDBAttributeValue{events.NewStringAttribute("a"), events.NewStringAttribute("b")}),
			[]interface{}{"a", "b"}},
		{"Null", events.NewNullAttribute(), nil},
		{"Binary", events.NewBinaryAttribute([]byte{1}), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ConvertAttributeValue(tt.av))
		})
	}
}

func TestSensorKeyMatches(t *testing.T) {
	tests := []struct {
		sensorKey, ruleKey string
		want               bool
	}{
		{"orders", "orders", true},
		{"orders#2026-03-10", "orders", true},
		{"orders#20260310", "orders", true},
		{"orders#2026-03-10T07", "orders", true},
		{"orders-count", "orders", false},
		{"orders2", "orders", false},
		{"ordersX#2026-03-10", "orders", false},
		{"order", "orders", false},
		{"orders", "", false},
		{"", "", true},
		{"orders#eu#2026-03-10", "orders#eu", true},
		{"orders#europe", "orders#eu", false},
	}
	for _, tt := range tests {
		t.Run(tt.sensorKey+"/"+tt.ruleKey, func(t *testing.T) {
			assert.Equal(t, tt.want, SensorKeyMatches(tt.sensorKey, tt.ruleKey))
		})
	}
}
