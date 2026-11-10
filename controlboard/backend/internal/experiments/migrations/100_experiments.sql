-- Experiment engine state (schema controlboard). Prefix 1xx: the board's own migrations use 0xx.
CREATE TABLE engine_settings (
    id         int PRIMARY KEY CHECK (id = 1),
    doc        jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- One row per experiment; doc holds the full (unredacted) experiment plus its revert data.
CREATE TABLE experiments (
    id         text PRIMARY KEY,
    status     text NOT NULL,
    created_at timestamptz NOT NULL,
    doc        jsonb NOT NULL
);
CREATE INDEX experiments_unfinished ON experiments (status) WHERE status IN ('scheduled', 'running', 'recovering');
CREATE INDEX experiments_created ON experiments (created_at DESC);
