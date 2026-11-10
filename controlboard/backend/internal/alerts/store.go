package alerts

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"sync"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/kit/pg"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the SQL migrations of the alert history (schema = search_path).
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrations, "migrations")
	return sub
}

// PG is the PostgreSQL alert history.
type PG struct{ DB *pg.DB }

// Upsert implements Store.
func (p PG) Upsert(ctx context.Context, a api.Alert, updated time.Time) error {
	doc, _ := json.Marshal(a) // plain strings, times and maps: cannot fail
	_, err := p.DB.Pool.Exec(ctx, `
		INSERT INTO alerts (fingerprint, starts_at, status, ends_at, updated_at, alert)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (fingerprint, starts_at) DO UPDATE
		SET status = EXCLUDED.status, ends_at = EXCLUDED.ends_at, updated_at = EXCLUDED.updated_at, alert = EXCLUDED.alert`,
		a.Fingerprint, a.StartsAt, a.Status, a.EndsAt, updated, doc)
	return err
}

// Load implements Store.
func (p PG) Load(ctx context.Context, since time.Time) ([]api.Alert, error) {
	rows, err := p.DB.Pool.Query(ctx, `SELECT alert FROM alerts WHERE updated_at >= $1 ORDER BY starts_at`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []api.Alert{}
	for rows.Next() {
		var a api.Alert
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// Prune implements Store.
func (p PG) Prune(ctx context.Context, before time.Time) error {
	_, err := p.DB.Pool.Exec(ctx, `DELETE FROM alerts WHERE updated_at < $1`, before)
	return err
}

// Memory is an in-memory Store (local runs without DATABASE_URL, tests).
type Memory struct {
	mu   sync.Mutex
	rows map[string]memRow
	Err  error // returned by every call when set (database outage)
}

type memRow struct {
	alert   api.Alert
	updated time.Time
}

// Upsert implements Store.
func (m *Memory) Upsert(_ context.Context, a api.Alert, updated time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	if m.rows == nil {
		m.rows = map[string]memRow{}
	}
	m.rows[key(a)] = memRow{a, updated}
	return nil
}

// Load implements Store.
func (m *Memory) Load(_ context.Context, since time.Time) ([]api.Alert, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	out := []api.Alert{}
	for _, r := range m.rows {
		if !r.updated.Before(since) {
			out = append(out, r.alert)
		}
	}
	return out, nil
}

// Prune implements Store.
func (m *Memory) Prune(_ context.Context, before time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	for k, r := range m.rows {
		if r.updated.Before(before) {
			delete(m.rows, k)
		}
	}
	return nil
}
