-- One row per order: the payment state machine of docs/contracts/events.md.
CREATE TABLE payments (
    order_id     text        PRIMARY KEY,
    state        text        NOT NULL DEFAULT '',
    payment_id   text        NOT NULL DEFAULT '',
    amount_cents bigint      NOT NULL DEFAULT 0,
    method       text        NOT NULL DEFAULT '',
    updated_at   timestamptz NOT NULL DEFAULT now()
);
