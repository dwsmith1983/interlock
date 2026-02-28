package redis

import (
	"context"
	"encoding/json"
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func (p *RedisProvider) quarantineKey(pipelineID, date, hour string) string {
	return p.prefix + "quarantine:" + pipelineID + ":" + date + ":" + hour
}

// PutQuarantineRecord stores a quarantine record.
func (p *RedisProvider) PutQuarantineRecord(ctx context.Context, record types.QuarantineRecord) error {
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return p.client.Set(ctx, p.quarantineKey(record.PipelineID, record.Date, record.Hour), raw, 0).Err()
}

// GetQuarantineRecord retrieves a quarantine record.
func (p *RedisProvider) GetQuarantineRecord(ctx context.Context, pipelineID, date, hour string) (*types.QuarantineRecord, error) {
	raw, err := p.client.Get(ctx, p.quarantineKey(pipelineID, date, hour)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var record types.QuarantineRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	return &record, nil
}
