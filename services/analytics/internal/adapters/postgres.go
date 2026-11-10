// Package adapters stores analytics' order rows in PostgreSQL and counts dead letters with a
// plain Kafka consumer.
package adapters

import (
	"context"
	"embed"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/services/analytics/internal/domain"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the analytics schema migrations.
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrations, "migrations")
	return sub
}

// Postgres implements app.Repository and app.Purger.
type Postgres struct{}

// Upsert inserts seed if the order is unknown, otherwise locks and returns the stored row.
// One statement either way: the common case (orders.created for a new order) needs no
// further write. xmax = 0 identifies a freshly inserted row version.
func (Postgres) Upsert(ctx context.Context, tx pgx.Tx, seed domain.Order, forgetAfter time.Time) (domain.Order, bool, error) {
	o := domain.Order{OrderID: seed.OrderID}
	var outcome string
	var fresh bool
	err := tx.QueryRow(ctx, `
		INSERT INTO orders(order_id, created, total_cents, region, outcome, forget_after) VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (order_id) DO UPDATE SET order_id = EXCLUDED.order_id
		RETURNING created, total_cents, region, outcome, xmax = 0`,
		seed.OrderID, seed.Created, seed.TotalCents, seed.Region, string(seed.Outcome), forgetAfter).
		Scan(&o.Created, &o.TotalCents, &o.Region, &outcome, &fresh)
	o.Outcome = domain.Outcome(outcome)
	return o, fresh, err
}

// Save stores an order row.
func (Postgres) Save(ctx context.Context, tx pgx.Tx, o domain.Order, forgetAfter time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE orders SET created = $2, total_cents = $3, region = $4, outcome = $5, forget_after = $6
		WHERE order_id = $1`, o.OrderID, o.Created, o.TotalCents, o.Region, string(o.Outcome), forgetAfter)
	return err
}

// Purge deletes up to limit rows whose retention passed before now.
func (Postgres) Purge(ctx context.Context, tx pgx.Tx, now time.Time, limit int) (int64, error) {
	tag, err := tx.Exec(ctx, `DELETE FROM orders WHERE order_id IN (
		SELECT order_id FROM orders WHERE forget_after < $1 LIMIT $2 FOR UPDATE SKIP LOCKED)`, now, limit)
	return tag.RowsAffected(), err
}
