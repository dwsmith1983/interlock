package dynamodb

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// PutSensorData stores externally-landed sensor data.
func (p *DynamoDBProvider) PutSensorData(ctx context.Context, data types.SensorData) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = p.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &p.tableName,
		Item: map[string]ddbtypes.AttributeValue{
			"PK":   &ddbtypes.AttributeValueMemberS{Value: pipelinePK(data.PipelineID)},
			"SK":   &ddbtypes.AttributeValueMemberS{Value: sensorSK(data.SensorType)},
			"data": &ddbtypes.AttributeValueMemberS{Value: string(raw)},
		},
	})
	return err
}

// GetSensorData retrieves the latest sensor reading for a pipeline and sensor type.
func (p *DynamoDBProvider) GetSensorData(ctx context.Context, pipelineID, sensorType string) (*types.SensorData, error) {
	out, err := p.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &p.tableName,
		Key: map[string]ddbtypes.AttributeValue{
			"PK": &ddbtypes.AttributeValueMemberS{Value: pipelinePK(pipelineID)},
			"SK": &ddbtypes.AttributeValueMemberS{Value: sensorSK(sensorType)},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}

	data, err := attributeStr(out.Item, "data")
	if err != nil {
		return nil, err
	}
	var sd types.SensorData
	if err := json.Unmarshal([]byte(data), &sd); err != nil {
		return nil, err
	}
	return &sd, nil
}
