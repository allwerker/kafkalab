-- Outbox log of sent notifications (one row per notifications.sent). Pruned after a retention
-- period; deduplication is done by the kit's processed_events, not here.
CREATE TABLE notifications (
    id       bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id text        NOT NULL,
    template text        NOT NULL,
    channel  text        NOT NULL,
    sent_at  timestamptz NOT NULL
);
CREATE INDEX notifications_sent_at ON notifications (sent_at);
CREATE INDEX notifications_order_id ON notifications (order_id);
