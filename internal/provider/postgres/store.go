package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is a Postgres-backed archival store for Interlock data.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Postgres Store and verifies the connection.
func New(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Migrate runs the schema DDL to create tables and indexes.
func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaDDL)
	if err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	return nil
}

// Close closes the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}
