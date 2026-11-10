// Package adapters stores reservations and stock in PostgreSQL and runs the restock job.
package adapters

import (
	"context"
	"embed"
	"io/fs"

	"github.com/jackc/pgx/v5"

	"kafkalab/services/inventory/internal/app"
	"kafkalab/services/inventory/internal/domain"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the inventory schema migrations (tables and the seeded stock).
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrations, "migrations")
	return sub
}

// Postgres implements app.Repository.
type Postgres struct{}

// Lock creates the order's row if needed and locks it until the transaction ends. The upsert
// makes "first input for this order" and "later input" take the same locking path.
func (Postgres) Lock(ctx context.Context, tx pgx.Tx, orderID string) (app.Reservation, error) {
	r := app.Reservation{OrderID: orderID}
	var state string
	err := tx.QueryRow(ctx, `
		INSERT INTO reservations(order_id) VALUES ($1)
		ON CONFLICT (order_id) DO UPDATE SET order_id = EXCLUDED.order_id
		RETURNING state, reservation_id, warehouse, lines`, orderID).Scan(&state, &r.ReservationID, &r.Warehouse, &r.Lines)
	r.State = domain.State(state)
	return r, err
}

// Save stores the reservation.
func (Postgres) Save(ctx context.Context, tx pgx.Tx, r app.Reservation) error {
	_, err := tx.Exec(ctx, `
		UPDATE reservations SET state = $2, reservation_id = $3, warehouse = $4, lines = COALESCE($5, '[]'::jsonb), updated_at = now()
		WHERE order_id = $1`, r.OrderID, string(r.State), r.ReservationID, r.Warehouse, r.Lines)
	return err
}

// LockStock locks the stock rows of skus in SKU order (the sort runs below the row locking, so
// every transaction acquires its locks in the same order) and returns the units on hand.
func (Postgres) LockStock(ctx context.Context, tx pgx.Tx, skus []string) (map[string]int, error) {
	rows, err := tx.Query(ctx, `SELECT sku, available FROM stock WHERE sku = ANY($1) ORDER BY sku FOR UPDATE`, skus)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	var sku string
	var n int
	_, err = pgx.ForEachRow(rows, []any{&sku, &n}, func() error { out[sku] = n; return nil })
	return out, err
}

// Take removes the reserved quantities from stock.
func (Postgres) Take(ctx context.Context, tx pgx.Tx, lines []domain.Line) error {
	skus, qty := columns(lines)
	_, err := tx.Exec(ctx, `
		UPDATE stock s SET available = s.available - u.q
		FROM unnest($1::text[], $2::int[]) AS u(sku, q) WHERE s.sku = u.sku`, skus, qty)
	return err
}

// Return puts released quantities back on the shelf (never above its capacity: a restock may
// have refilled it in the meantime).
func (Postgres) Return(ctx context.Context, tx pgx.Tx, lines []domain.Line) error {
	skus, qty := columns(lines)
	_, err := tx.Exec(ctx, `
		UPDATE stock s SET available = LEAST(s.capacity, s.available + u.q)
		FROM unnest($1::text[], $2::int[]) AS u(sku, q) WHERE s.sku = u.sku`, skus, qty)
	return err
}

func columns(lines []domain.Line) ([]string, []int32) {
	skus, qty := make([]string, len(lines)), make([]int32, len(lines))
	for i, l := range lines {
		skus[i], qty[i] = l.SKU, int32(l.Quantity)
	}
	return skus, qty
}
