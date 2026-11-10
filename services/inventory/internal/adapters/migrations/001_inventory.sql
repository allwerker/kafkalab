-- One row per order: the inventory state machine of docs/contracts/events.md.
CREATE TABLE reservations (
    order_id       text        PRIMARY KEY,
    state          text        NOT NULL DEFAULT '',
    reservation_id text        NOT NULL DEFAULT '',
    warehouse      text        NOT NULL DEFAULT '',
    lines          jsonb       NOT NULL DEFAULT '[]',
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Units on hand per product. Rows are locked in SKU order by every writer.
CREATE TABLE stock (
    sku          text        PRIMARY KEY,
    capacity     integer     NOT NULL CHECK (capacity > 0),
    available    integer     NOT NULL CHECK (available >= 0),
    restocked_at timestamptz NOT NULL DEFAULT now()
);

-- The 40 catalogue products (services/order-generator/internal/domain Catalogue). Capacity
-- follows popularity like domain.Capacity: 3000/rank rounded to 50, at least 200.
INSERT INTO stock(sku, capacity, available)
SELECT format('sku-%s', lpad(n::text, 3, '0')), c, c
FROM generate_series(1, 40) AS n, LATERAL (SELECT GREATEST(200, (3000 / n + 25) / 50 * 50) AS c) AS cap;
