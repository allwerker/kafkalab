package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Transactor runs fn in a transaction (implemented by *pg.DB).
type Transactor interface {
	InTx(ctx context.Context, fn func(pgx.Tx) error) error
}

// Purger deletes order rows whose retention (domain.ForgetAfter) has passed.
type Purger interface {
	Purge(ctx context.Context, tx pgx.Tx, now time.Time, limit int) (int64, error)
}

// Janitor keeps the orders table small by deleting expired rows in short transactions.
type Janitor struct {
	DB    Transactor
	Repo  Purger
	Log   *slog.Logger
	Now   func() time.Time
	Every time.Duration
	Batch int // rows per transaction, > 0
}

// Run purges every j.Every until ctx ends.
func (j *Janitor) Run(ctx context.Context) error {
	t := time.NewTicker(j.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			j.Purge(ctx)
		}
	}
}

// Purge deletes every expired row, one batch per transaction, and returns how many.
func (j *Janitor) Purge(ctx context.Context) int64 {
	var total int64
	for {
		var n int64
		err := j.DB.InTx(ctx, func(tx pgx.Tx) error {
			var err error
			n, err = j.Repo.Purge(ctx, tx, j.Now(), j.Batch)
			return err
		})
		if err != nil {
			j.Log.WarnContext(ctx, "purging expired orders failed", "error", err)
			return total
		}
		total += n
		if n < int64(j.Batch) {
			return total
		}
	}
}
