package adapters

import (
	"context"
	"log/slog"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"kafkalab/services/inventory/internal/domain"
)

// restockLockKey is the PostgreSQL advisory lock that elects the restocking pod ("invrestk").
const restockLockKey int64 = 0x696e76726573746b

// GaugedSKUs is how many of the most popular products are exported as lab_stock_units.
const GaugedSKUs = 20

// Restocker refills products that fell below domain.RestockBelow of their capacity. Exactly
// one pod restocks: the one holding the advisory lock on a dedicated database session. That
// pod also exports lab_stock_units, so the gauges are never duplicated across replicas.
type Restocker struct {
	Pool    *pgxpool.Pool
	Metrics *metrics.Set
	Log     *slog.Logger
	// Every is the restock interval (default 10 s).
	Every time.Duration
	// GaugeEvery is how often the leader refreshes lab_stock_units (default 2 s).
	GaugeEvery time.Duration
	Now        func() time.Time

	conn        *pgxpool.Conn // holds the advisory lock while leading
	lastRestock time.Time
	exported    []string
}

func (r *Restocker) defaults() {
	if r.Every == 0 {
		r.Every = 10 * time.Second
	}
	if r.GaugeEvery == 0 {
		r.GaugeEvery = 2 * time.Second
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.Log == nil {
		r.Log = slog.New(slog.DiscardHandler)
	}
}

// Run restocks and exports gauges until ctx ends, then gives up the leadership.
func (r *Restocker) Run(ctx context.Context) {
	r.defaults()
	defer r.Resign()
	t := time.NewTicker(r.GaugeEvery)
	defer t.Stop()
	for {
		r.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick tries to become the leader; the leader restocks when due and refreshes the gauges.
// It reports whether this pod leads.
func (r *Restocker) Tick(ctx context.Context) bool {
	r.defaults()
	if r.conn == nil && !r.tryLead(ctx) {
		return false
	}
	if now := r.Now(); now.Sub(r.lastRestock) >= r.Every {
		tag, err := r.conn.Exec(ctx, `
			UPDATE stock SET available = capacity, restocked_at = now() WHERE sku IN (
				SELECT sku FROM stock WHERE available < capacity * $1::float8 ORDER BY sku FOR UPDATE)`, domain.RestockBelow)
		if err != nil {
			r.lost(ctx, err)
			return false
		}
		r.lastRestock = now
		if n := tag.RowsAffected(); n > 0 {
			r.Log.InfoContext(ctx, "restocked products", "count", n)
		}
	}
	if err := r.exportStock(ctx); err != nil {
		r.lost(ctx, err)
		return false
	}
	return true
}

func (r *Restocker) tryLead(ctx context.Context) bool {
	c, err := r.Pool.Acquire(ctx)
	if err != nil {
		r.Log.WarnContext(ctx, "restock: database not reachable", "error", err)
		return false
	}
	var ok bool
	if err := c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, restockLockKey).Scan(&ok); err != nil || !ok {
		c.Release()
		return false
	}
	r.conn, r.lastRestock = c, time.Time{}
	r.Log.InfoContext(ctx, "this pod restocks and exports lab_stock_units")
	return true
}

func (r *Restocker) exportStock(ctx context.Context) error {
	// A failed query returns rows in an error state, which ForEachRow reports.
	rows, _ := r.conn.Query(ctx, `SELECT sku, available FROM stock ORDER BY sku LIMIT $1`, GaugedSKUs)
	var sku string
	var n int
	exported := make([]string, 0, GaugedSKUs)
	_, err := pgx.ForEachRow(rows, []any{&sku, &n}, func() error {
		name := `lab_stock_units{sku="` + sku + `"}`
		r.Metrics.GetOrCreateGauge(name, nil).Set(float64(n))
		exported = append(exported, name)
		return nil
	})
	if err == nil {
		r.exported = exported
	}
	return err
}

func (r *Restocker) lost(ctx context.Context, err error) {
	if ctx.Err() == nil {
		r.Log.WarnContext(ctx, "restock leadership lost", "error", err)
	}
	r.Resign()
}

// Resign gives up the leadership: the session holding the advisory lock is closed (which
// releases the lock even if the connection is broken) and the stock gauges disappear.
func (r *Restocker) Resign() {
	if r.conn == nil {
		return
	}
	c := r.conn.Hijack()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Unlock explicitly so another pod can take over at once; closing the session is the
	// fallback when the connection is already broken.
	_, _ = c.Exec(ctx, `SELECT pg_advisory_unlock($1)`, restockLockKey)
	_ = c.Close(ctx)
	r.conn = nil
	for _, name := range r.exported {
		r.Metrics.UnregisterMetric(name)
	}
	r.exported = nil
}
