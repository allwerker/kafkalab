// Package adapters stores shipping joins in PostgreSQL, elects the pod that exports the join
// gauge and publishes the sweeper's decisions to Kafka.
package adapters

import (
	"context"
	"embed"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/services/shipping/internal/app"
	"kafkalab/services/shipping/internal/domain"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the shipping schema migrations.
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrations, "migrations")
	return sub
}

// Postgres implements app.Repository and app.SweepRepository.
type Postgres struct{}

const columns = `order_id, payment, stock, status, payment_id, reservation_id, warehouse, cause_event_id,
	order_created_at, first_seen_at`

// Lock creates the order's row if needed and locks it until the transaction ends. The upsert
// makes "first half of this order" and "second half" take the same locking path, so two pods
// receiving the two halves at the same moment are serialised on the row.
func (Postgres) Lock(ctx context.Context, tx pgx.Tx, orderID string, now time.Time) (app.Order, error) {
	// A failed Query is reported by the collector (rows carry the error), the pgx idiom.
	rows, _ := tx.Query(ctx, `
		INSERT INTO joins(order_id, first_seen_at, updated_at) VALUES ($1, $2, $2)
		ON CONFLICT (order_id) DO UPDATE SET order_id = EXCLUDED.order_id
		RETURNING `+columns, orderID, now)
	return pgx.CollectExactlyOneRow(rows, scan)
}

// Save stores the order's join state.
func (Postgres) Save(ctx context.Context, tx pgx.Tx, o app.Order) error {
	var created *time.Time
	if !o.OrderCreatedAt.IsZero() {
		created = &o.OrderCreatedAt
	}
	_, err := tx.Exec(ctx, `
		UPDATE joins SET payment = $2, stock = $3, status = $4, payment_id = $5, reservation_id = $6,
			warehouse = $7, cause_event_id = $8, order_created_at = $9, updated_at = now()
		WHERE order_id = $1`, o.OrderID, string(o.Join.Payment), string(o.Join.Stock), string(o.Join.Status),
		o.PaymentID, o.ReservationID, o.Warehouse, o.CauseEventID, created)
	return err
}

// Overdue implements app.SweepRepository.
func (Postgres) Overdue(ctx context.Context, tx pgx.Tx, before time.Time, limit int) ([]app.Order, error) {
	rows, _ := tx.Query(ctx, `SELECT `+columns+` FROM joins
		WHERE status = 'open' AND first_seen_at < $1
		ORDER BY first_seen_at LIMIT $2 FOR UPDATE SKIP LOCKED`, before, limit)
	return pgx.CollectRows(rows, scan)
}

// Ready implements app.SweepRepository.
func (Postgres) Ready(ctx context.Context, tx pgx.Tx, limit int) ([]app.Order, error) {
	rows, _ := tx.Query(ctx, `SELECT `+columns+` FROM joins
		WHERE status = 'ready'
		ORDER BY first_seen_at LIMIT $1 FOR UPDATE SKIP LOCKED`, limit)
	return pgx.CollectRows(rows, scan)
}

// Waiting implements app.SweepRepository.
func (Postgres) Waiting(ctx context.Context, tx pgx.Tx) (int64, error) {
	var n int64
	err := tx.QueryRow(ctx, `SELECT count(*) FROM joins WHERE status IN ('open', 'ready')`).Scan(&n)
	return n, err
}

// Purge implements app.SweepRepository.
func (Postgres) Purge(ctx context.Context, tx pgx.Tx, before time.Time, limit int) (int64, error) {
	tag, err := tx.Exec(ctx, `DELETE FROM joins WHERE order_id IN (
		SELECT order_id FROM joins WHERE status IN ('shipped', 'cancelled') AND updated_at < $1
		LIMIT $2 FOR UPDATE SKIP LOCKED)`, before, limit)
	return tag.RowsAffected(), err
}

func scan(row pgx.CollectableRow) (app.Order, error) {
	var o app.Order
	var payment, stock, status string
	var created *time.Time
	err := row.Scan(&o.OrderID, &payment, &stock, &status, &o.PaymentID, &o.ReservationID, &o.Warehouse,
		&o.CauseEventID, &created, &o.FirstSeenAt)
	o.Join = domain.Join{Payment: domain.Half(payment), Stock: domain.Half(stock), Status: domain.Status(status)}
	if created != nil {
		o.OrderCreatedAt = created.UTC()
	}
	o.FirstSeenAt = o.FirstSeenAt.UTC()
	return o, err
}
