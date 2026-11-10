// Package pg provides PostgreSQL access for lab services: pooled connections, embedded
// migrations and transactions that honour the "Database Slowdown" knob.
package pg

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB is a connection pool plus lab instrumentation.
type DB struct {
	Pool    *pgxpool.Pool
	latency func() time.Duration
	sleep   func(context.Context, time.Duration) error
	txTime  *metrics.Histogram
}

// Options configures Open.
type Options struct {
	URL      string
	MaxConns int32
	// Latency returns the extra delay injected into every transaction (Database Slowdown knob).
	Latency func() time.Duration
	Metrics *metrics.Set
	Log     *slog.Logger
	// RetryEvery is the pause between connection attempts while the database is unavailable.
	RetryEvery time.Duration
}

// Open connects and waits until the database answers or ctx ends. Services start before
// PostgreSQL is ready in a fresh cluster, so failing fast would only cause crash loops.
func Open(ctx context.Context, o Options) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(o.URL)
	if err != nil {
		return nil, fmt.Errorf("database url: %w", err)
	}
	if o.MaxConns > 0 {
		cfg.MaxConns = o.MaxConns
	}
	if o.RetryEvery == 0 {
		o.RetryEvery = 2 * time.Second
	}
	if o.Latency == nil {
		o.Latency = func() time.Duration { return 0 }
	}
	if o.Metrics == nil {
		o.Metrics = metrics.NewSet()
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil { // coverage-ignore: only fails for configs not created by ParseConfig
		return nil, err
	}
	for {
		err = pool.Ping(ctx)
		if err == nil {
			break
		}
		if o.Log != nil {
			o.Log.WarnContext(ctx, "database not reachable yet", "error", err)
		}
		if serr := sleepCtx(ctx, o.RetryEvery); serr != nil {
			pool.Close()
			return nil, fmt.Errorf("database not reachable: %w", err)
		}
	}
	return &DB{
		Pool:    pool,
		latency: o.Latency,
		sleep:   sleepCtx,
		txTime:  o.Metrics.GetOrCreateHistogram("lab_db_tx_duration_seconds"),
	}, nil
}

// Close releases all connections.
func (db *DB) Close() { db.Pool.Close() }

// InTx runs fn in a transaction and commits it. The injected latency is spent inside the
// transaction (holding its locks), which is how a slow database really hurts throughput.
func (db *DB) InTx(ctx context.Context, fn func(pgx.Tx) error) (err error) {
	start := time.Now()
	defer func() { db.txTime.UpdateDuration(start) }()
	tx, err := db.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err = fn(tx); err != nil {
		return err
	}
	if d := db.latency(); d > 0 {
		if err = db.sleep(ctx, d); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Migrate applies the *.sql files of migrations in lexical order, each exactly once, inside the
// current search_path schema. Applied versions are recorded in schema_migrations.
func (db *DB) Migrate(ctx context.Context, migrations fs.FS) error {
	names, err := fs.Glob(migrations, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	if _, err := db.Pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	for _, name := range names {
		body, err := fs.ReadFile(migrations, name)
		if err != nil {
			return err
		}
		err = db.InTx(ctx, func(tx pgx.Tx) error {
			// Serialise concurrent pods migrating at the same time.
			if _, err := tx.Exec(ctx, `LOCK TABLE schema_migrations IN EXCLUSIVE MODE`); err != nil { // coverage-ignore: connection loss mid-transaction
				return err
			}
			var done bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name).Scan(&done); err != nil { // coverage-ignore: connection loss mid-transaction
				return err
			}
			if done {
				return nil
			}
			if _, err := tx.Exec(ctx, string(body)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES ($1)`, name)
			return err
		})
		if err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
	}
	return nil
}

// IsUnavailable reports whether err means the database could not be reached or the
// transaction was aborted for infrastructure reasons (retry), as opposed to a bug.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := err.Error()
	for _, s := range []string{"connection refused", "connect:", "broken pipe", "conn closed", "deadlock detected", "could not serialize"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
