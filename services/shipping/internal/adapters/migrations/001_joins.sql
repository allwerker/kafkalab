-- One row per order: the shipping join of docs/contracts/events.md.
CREATE TABLE joins (
    order_id         text        PRIMARY KEY,
    payment          text        NOT NULL DEFAULT '',     -- '' | ok | failed
    stock            text        NOT NULL DEFAULT '',     -- '' | ok | failed
    status           text        NOT NULL DEFAULT 'open', -- open | ready | shipped | cancelled
    payment_id       text        NOT NULL DEFAULT '',
    reservation_id   text        NOT NULL DEFAULT '',
    warehouse        text        NOT NULL DEFAULT '',
    cause_event_id   text        NOT NULL DEFAULT '',
    order_created_at timestamptz,
    first_seen_at    timestamptz NOT NULL,
    updated_at       timestamptz NOT NULL
);
-- The sweeper only looks at the few non-terminal rows (deadline, held-back shipments).
CREATE INDEX joins_waiting ON joins (status, first_seen_at) WHERE status IN ('open', 'ready');
-- Purging old terminal rows.
CREATE INDEX joins_terminal ON joins (updated_at) WHERE status IN ('shipped', 'cancelled');
