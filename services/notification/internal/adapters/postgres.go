// Package adapters keeps the notification log in PostgreSQL and simulates the delivery provider.
package adapters

import (
	"cmp"
	"context"
	"embed"
	"io/fs"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"kafkalab/services/notification/internal/app"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the notification schema migrations.
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrations, "migrations")
	return sub
}

// Postgres implements app.Repository.
type Postgres struct{}

// Record appends a sent notification to the log.
func (Postgres) Record(ctx context.Context, tx pgx.Tx, n app.Notification) error {
	_, err := tx.Exec(ctx, `INSERT INTO notifications(order_id, template, channel, sent_at) VALUES ($1, $2, $3, $4)`,
		n.OrderID, n.Template, string(n.Channel), n.SentAt)
	return err
}

// Janitor deletes log entries older than Keep, so the log stays small under sustained load.
type Janitor struct {
	Pool  *pgxpool.Pool
	Keep  time.Duration // default 1 h
	Every time.Duration // default 1 min
	Log   *slog.Logger
}

// Run prunes periodically until ctx ends.
func (j *Janitor) Run(ctx context.Context) {
	log := cmp.Or(j.Log, slog.New(slog.DiscardHandler))
	t := time.NewTicker(cmp.Or(j.Every, time.Minute))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if _, err := j.Prune(ctx, time.Now().Add(-cmp.Or(j.Keep, time.Hour))); err != nil && ctx.Err() == nil {
			log.WarnContext(ctx, "pruning the notification log failed", "error", err)
		}
	}
}

// Prune deletes entries sent before the given instant, in batches, and returns how many.
func (j *Janitor) Prune(ctx context.Context, before time.Time) (int64, error) {
	var total int64
	for {
		tag, err := j.Pool.Exec(ctx, `DELETE FROM notifications WHERE id IN (
			SELECT id FROM notifications WHERE sent_at < $1 LIMIT 5000)`, before)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < 5000 {
			return total, nil
		}
	}
}

// SimulatedProvider stands in for the e-mail, push and SMS gateways: every send takes Latency.
type SimulatedProvider struct {
	Latency time.Duration
}

// Send waits for the gateway's answer (or until ctx ends).
func (p SimulatedProvider) Send(ctx context.Context, _ app.Notification) error {
	if p.Latency <= 0 {
		return nil
	}
	t := time.NewTimer(p.Latency)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
