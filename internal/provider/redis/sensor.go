package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func (p *RedisProvider) sensorKey(pipelineID, sensorType string) string {
	return p.prefix + "sensor:" + pipelineID + ":" + sensorType
}

// PutSensorData stores externally-landed sensor data.
func (p *RedisProvider) PutSensorData(ctx context.Context, data types.SensorData) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling sensor data %q for pipeline %q: %w", data.SensorType, data.PipelineID, err)
	}
	if err := p.client.Set(ctx, p.sensorKey(data.PipelineID, data.SensorType), raw, 0).Err(); err != nil {
		return fmt.Errorf("storing sensor data %q for pipeline %q: %w", data.SensorType, data.PipelineID, err)
	}
	return nil
}

// GetSensorData retrieves the latest sensor reading for a pipeline and sensor type.
func (p *RedisProvider) GetSensorData(ctx context.Context, pipelineID, sensorType string) (*types.SensorData, error) {
	raw, err := p.client.Get(ctx, p.sensorKey(pipelineID, sensorType)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("getting sensor data %q for pipeline %q: %w", sensorType, pipelineID, err)
	}
	var sd types.SensorData
	if err := json.Unmarshal(raw, &sd); err != nil {
		return nil, fmt.Errorf("unmarshaling sensor data %q for pipeline %q: %w", sensorType, pipelineID, err)
	}
	return &sd, nil
}
