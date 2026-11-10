-- Alert history of the controlboard (Alertmanager webhook). One row per alert occurrence.
CREATE TABLE IF NOT EXISTS alerts (
    fingerprint text        NOT NULL,
    starts_at   timestamptz NOT NULL,
    status      text        NOT NULL,
    ends_at     timestamptz,
    updated_at  timestamptz NOT NULL,
    alert       jsonb       NOT NULL,
    PRIMARY KEY (fingerprint, starts_at)
);
CREATE INDEX IF NOT EXISTS alerts_updated_at ON alerts (updated_at);
