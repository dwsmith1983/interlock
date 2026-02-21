// Package postgres implements a durable Postgres store for archived Interlock data.
package postgres

const schemaDDL = `
CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    pipeline_id  TEXT NOT NULL,
    status       TEXT NOT NULL,
    version      INTEGER NOT NULL,
    metadata     JSONB,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL,
    archived_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_runs_pipeline_status ON runs (pipeline_id, status);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs (created_at);

CREATE TABLE IF NOT EXISTS run_logs (
    pipeline_id      TEXT NOT NULL,
    date             TEXT NOT NULL,
    schedule_id      TEXT NOT NULL DEFAULT 'daily',
    status           TEXT NOT NULL,
    attempt_number   INTEGER NOT NULL,
    run_id           TEXT NOT NULL,
    failure_message  TEXT,
    failure_category TEXT,
    alert_sent       BOOLEAN NOT NULL DEFAULT FALSE,
    started_at       TIMESTAMPTZ NOT NULL,
    completed_at     TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ NOT NULL,
    archived_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, date, schedule_id)
);
CREATE INDEX IF NOT EXISTS idx_run_logs_status ON run_logs (status);
CREATE INDEX IF NOT EXISTS idx_run_logs_date ON run_logs (date);

CREATE TABLE IF NOT EXISTS reruns (
    rerun_id        TEXT PRIMARY KEY,
    pipeline_id     TEXT NOT NULL,
    original_date   TEXT NOT NULL,
    original_run_id TEXT,
    reason          TEXT NOT NULL,
    description     TEXT,
    status          TEXT NOT NULL,
    rerun_run_id    TEXT,
    metadata        JSONB,
    requested_at    TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_reruns_pipeline_status ON reruns (pipeline_id, status);

CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    stream_id   TEXT,
    kind        TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    run_id      TEXT,
    trait_type  TEXT,
    status      TEXT,
    message     TEXT,
    details     JSONB,
    timestamp   TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup
    ON events (pipeline_id, stream_id) WHERE stream_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_pipeline_kind ON events (pipeline_id, kind);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp);

CREATE TABLE IF NOT EXISTS trait_evaluations (
    id               BIGSERIAL PRIMARY KEY,
    pipeline_id      TEXT NOT NULL,
    trait_type       TEXT NOT NULL,
    status           TEXT NOT NULL,
    value            JSONB,
    reason           TEXT,
    failure_category TEXT,
    evaluated_at     TIMESTAMPTZ NOT NULL,
    archived_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trait_evals_pipeline_type ON trait_evaluations (pipeline_id, trait_type);
CREATE INDEX IF NOT EXISTS idx_trait_evals_evaluated_at ON trait_evaluations (evaluated_at);

CREATE TABLE IF NOT EXISTS archive_cursors (
    pipeline_id  TEXT NOT NULL,
    data_type    TEXT NOT NULL,
    cursor_value TEXT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pipeline_id, data_type)
);
`
