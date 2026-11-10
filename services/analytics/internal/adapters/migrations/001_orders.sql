-- One small row per order in flight (plus an hour after its outcome): what analytics needs to
-- count revenue at shipping time and to recognise orders created twice.
CREATE TABLE orders (
    order_id     text        PRIMARY KEY,
    created      boolean     NOT NULL DEFAULT false,
    total_cents  bigint      NOT NULL DEFAULT 0,
    region       text        NOT NULL DEFAULT '',
    outcome      text        NOT NULL DEFAULT '', -- '' | shipped | cancelled
    forget_after timestamptz NOT NULL
);
CREATE INDEX orders_forget_after ON orders (forget_after);
