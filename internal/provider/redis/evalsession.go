package redis

import (
	"context"
	"encoding/json"
	"errors"

	goredis "github.com/redis/go-redis/v9"

	"github.com/dwsmith1983/interlock/pkg/types"
)

func (p *RedisProvider) evalSessionKey(sessionID string) string {
	return p.prefix + "evalsession:" + sessionID
}

func (p *RedisProvider) evalSessionIndexKey(pipelineID string) string {
	return p.prefix + "evalsessions:" + pipelineID
}

const defaultEvalSessionIndexMax = 200

// PutEvaluationSession stores an evaluation session record.
func (p *RedisProvider) PutEvaluationSession(ctx context.Context, session types.EvaluationSession) error {
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	pipe := p.client.Pipeline()
	pipe.Set(ctx, p.evalSessionKey(session.SessionID), data, p.retentionTTL)
	pipe.ZAdd(ctx, p.evalSessionIndexKey(session.PipelineID), goredis.Z{
		Score:  float64(session.StartedAt.UnixMilli()),
		Member: session.SessionID,
	})
	pipe.ZRemRangeByRank(ctx, p.evalSessionIndexKey(session.PipelineID), 0, -(defaultEvalSessionIndexMax + 1))
	_, err = pipe.Exec(ctx)
	return err
}

// GetEvaluationSession retrieves a session by ID.
func (p *RedisProvider) GetEvaluationSession(ctx context.Context, sessionID string) (*types.EvaluationSession, error) {
	data, err := p.client.Get(ctx, p.evalSessionKey(sessionID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session types.EvaluationSession
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, err
	}
	return &session, nil
}

// ListEvaluationSessions returns recent sessions for a pipeline, newest first.
func (p *RedisProvider) ListEvaluationSessions(ctx context.Context, pipelineID string, limit int) ([]types.EvaluationSession, error) {
	if limit <= 0 {
		limit = 20
	}
	ids, err := p.client.ZRangeArgs(ctx, goredis.ZRangeArgs{
		Key:   p.evalSessionIndexKey(pipelineID),
		Start: 0,
		Stop:  int64(limit - 1),
		Rev:   true,
	}).Result()
	if err != nil {
		return nil, err
	}

	var sessions []types.EvaluationSession
	for _, id := range ids {
		s, err := p.GetEvaluationSession(ctx, id)
		if err != nil || s == nil {
			if err != nil {
				p.logger.Warn("skipping unreadable eval session", "sessionID", id, "error", err)
			}
			continue
		}
		sessions = append(sessions, *s)
	}
	return sessions, nil
}
