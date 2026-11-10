// Package adapters stores reservations and stock in PostgreSQL and runs the restock job.
package adapters

import (
	"context"
	"embed"
	"io/fs"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/runner"
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

// Stock access has two modes:
//
//   - Inside a runner batch (production) the first stock access of the batch locks ALL stock
//     rows once, in SKU order, into an in-memory ledger; reservations and releases of the whole
//     batch are decided against the ledger and the changed rows are written with one UPDATE just
//     before the batch commits. Every batch takes the same locks in the same order, so batches
//     on different partitions can no longer deadlock, and the locks are held for a few
//     milliseconds instead of the whole per-order processing time (Black Friday recovery).
//   - Outside a batch (direct calls) every call locks and updates its rows immediately.

type ledgerKey struct{}

type shelf struct{ capacity, available int }

// ledger is the batch's view of the locked stock table.
type ledger struct {
	shelves map[string]*shelf
	dirty   map[string]bool
}

func (Postgres) ledger(ctx context.Context, tx pgx.Tx) (*ledger, error) {
	b := runner.BatchOf(ctx)
	if b == nil {
		return nil, nil
	}
	l := b.Value(ledgerKey{}, func() any { return &ledger{} }).(*ledger)
	if l.shelves != nil {
		return l, nil
	}
	rows, err := tx.Query(ctx, `SELECT sku, capacity, available FROM stock ORDER BY sku FOR UPDATE`)
	if err != nil {
		return nil, err
	}
	shelves := map[string]*shelf{}
	var sku string
	var c, a int
	if _, err := pgx.ForEachRow(rows, []any{&sku, &c, &a}, func() error {
		shelves[sku] = &shelf{capacity: c, available: a}
		return nil
	}); err != nil { // coverage-ignore: integer columns only fail to scan on connection loss; the batch is retried
		return nil, err
	}
	l.shelves, l.dirty = shelves, map[string]bool{}
	b.BeforeCommit(l.flush)
	return l, nil
}

// flush writes every changed shelf with one statement.
func (l *ledger) flush(ctx context.Context, tx pgx.Tx) error {
	if len(l.dirty) == 0 {
		return nil
	}
	skus, qty := make([]string, 0, len(l.dirty)), make([]int32, 0, len(l.dirty))
	for sku := range l.dirty {
		skus, qty = append(skus, sku), append(qty, int32(l.shelves[sku].available))
	}
	_, err := tx.Exec(ctx, `
		UPDATE stock s SET available = u.a FROM unnest($1::text[], $2::int[]) AS u(sku, a) WHERE s.sku = u.sku`, skus, qty)
	return err
}

// LockStock returns the units on hand of skus, holding their row locks until commit (all rows
// of the table inside a batch, see above). Unknown products are missing from the result.
func (p Postgres) LockStock(ctx context.Context, tx pgx.Tx, skus []string) (map[string]int, error) {
	l, err := p.ledger(ctx, tx)
	if err != nil {
		return nil, err
	}
	if l != nil {
		out := map[string]int{}
		for _, sku := range skus {
			if s, ok := l.shelves[sku]; ok {
				out[sku] = s.available
			}
		}
		return out, nil
	}
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
func (p Postgres) Take(ctx context.Context, tx pgx.Tx, lines []domain.Line) error {
	return p.adjust(ctx, tx, lines, func(s *shelf, q int) { s.available -= q }, `
		UPDATE stock s SET available = s.available - u.q
		FROM unnest($1::text[], $2::int[]) AS u(sku, q) WHERE s.sku = u.sku`)
}

// Return puts released quantities back on the shelf (never above its capacity: a restock may
// have refilled it in the meantime).
func (p Postgres) Return(ctx context.Context, tx pgx.Tx, lines []domain.Line) error {
	return p.adjust(ctx, tx, lines, func(s *shelf, q int) { s.available = min(s.capacity, s.available+q) }, `
		UPDATE stock s SET available = LEAST(s.capacity, s.available + u.q)
		FROM unnest($1::text[], $2::int[]) AS u(sku, q) WHERE s.sku = u.sku`)
}

func (p Postgres) adjust(ctx context.Context, tx pgx.Tx, lines []domain.Line, apply func(*shelf, int), direct string) error {
	l, err := p.ledger(ctx, tx)
	if err != nil {
		return err
	}
	if l == nil {
		skus, qty := columns(lines)
		_, err := tx.Exec(ctx, direct, skus, qty)
		return err
	}
	for _, line := range lines {
		if s, ok := l.shelves[line.SKU]; ok {
			apply(s, line.Quantity)
			l.dirty[line.SKU] = true
		}
	}
	return nil
}

func columns(lines []domain.Line) ([]string, []int32) {
	skus, qty := make([]string, len(lines)), make([]int32, len(lines))
	for i, l := range lines {
		skus[i], qty[i] = l.SKU, int32(l.Quantity)
	}
	return skus, qty
}
