// Package adapters stores payments in PostgreSQL.
package adapters

import (
	"context"
	"embed"
	"io/fs"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/events"
	"kafkalab/services/payment/internal/app"
	"kafkalab/services/payment/internal/domain"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the payment schema migrations.
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrations, "migrations")
	return sub
}

// Postgres implements app.Repository.
type Postgres struct{}

// Lock creates the order's row if needed and locks it until the transaction ends. The upsert
// makes "first input for this order" and "later input" take the same locking path.
func (Postgres) Lock(ctx context.Context, tx pgx.Tx, orderID string) (app.Payment, error) {
	p := app.Payment{OrderID: orderID}
	var state, method string
	err := tx.QueryRow(ctx, `
		INSERT INTO payments(order_id) VALUES ($1)
		ON CONFLICT (order_id) DO UPDATE SET order_id = EXCLUDED.order_id
		RETURNING state, payment_id, amount_cents, method`, orderID).Scan(&state, &p.PaymentID, &p.AmountCents, &method)
	p.State, p.Method = domain.State(state), events.PaymentMethod(method)
	return p, err
}

// Save stores the payment.
func (Postgres) Save(ctx context.Context, tx pgx.Tx, p app.Payment) error {
	_, err := tx.Exec(ctx, `
		UPDATE payments SET state = $2, payment_id = $3, amount_cents = $4, method = $5, updated_at = now()
		WHERE order_id = $1`, p.OrderID, string(p.State), p.PaymentID, p.AmountCents, string(p.Method))
	return err
}
