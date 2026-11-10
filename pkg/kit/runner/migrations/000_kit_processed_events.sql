-- Owned by pkg/kit/runner. One row per input event this service has processed, with the
-- outputs it produced, so a redelivered input re-publishes exactly the same outputs.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     text        PRIMARY KEY,
    topic        text        NOT NULL,
    outputs      jsonb       NOT NULL DEFAULT '[]',
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS processed_events_processed_at ON processed_events (processed_at);
