package experiments

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/kit/pg"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations returns the engine's migrations (100_*.sql) for schema controlboard.
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrationFiles, "migrations")
	return sub
}

// settings is the engine's persisted global state.
type settings struct {
	Armed      bool                  `json:"armed"`
	Simulation string                `json:"simulation"`
	Manual     map[string]api.Values `json:"manual"`
	Tuning     map[string]api.Values `json:"tuning"`
	// Scaled records manual scale changes: service → replicas before the first change.
	Scaled map[string]int `json:"scaled"`
	// Desired is the manually requested replica count per service.
	Desired map[string]int `json:"desired"`
}

func (s *settings) init() {
	if s.Simulation == "" {
		s.Simulation = "running"
	}
	if s.Manual == nil {
		s.Manual = map[string]api.Values{}
	}
	if s.Tuning == nil {
		s.Tuning = map[string]api.Values{}
	}
	if s.Scaled == nil {
		s.Scaled = map[string]int{}
	}
	if s.Desired == nil {
		s.Desired = map[string]int{}
	}
}

type store struct {
	db *pg.DB
}

func (s *store) migrate(ctx context.Context) error {
	if err := s.db.Migrate(ctx, Migrations()); err != nil {
		return fmt.Errorf("experiments: migrate: %w", err)
	}
	return nil
}

func (s *store) loadSettings(ctx context.Context) (settings, error) {
	var st settings
	var raw []byte
	err := s.db.Pool.QueryRow(ctx, `SELECT doc FROM engine_settings WHERE id = 1`).Scan(&raw)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return st, fmt.Errorf("experiments: load settings: %w", err)
	default:
		_ = json.Unmarshal(raw, &st) // written by saveSettings only
	}
	st.init()
	return st, nil
}

func (s *store) saveSettings(ctx context.Context, st settings) error {
	raw, _ := json.Marshal(st)
	_, err := s.db.Pool.Exec(ctx, `INSERT INTO engine_settings (id, doc) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET doc = EXCLUDED.doc, updated_at = now()`, raw)
	if err != nil {
		return fmt.Errorf("%w: persist settings: %v", api.ErrUnavailable, err)
	}
	return nil
}

func (s *store) saveRecord(ctx context.Context, r *record) error {
	raw, _ := json.Marshal(r)
	_, err := s.db.Pool.Exec(ctx, `INSERT INTO experiments (id, status, created_at, doc) VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, doc = EXCLUDED.doc`,
		r.Exp.ID, r.Exp.Status, r.Exp.CreatedAt, raw)
	if err != nil {
		return fmt.Errorf("%w: persist experiment: %v", api.ErrUnavailable, err)
	}
	return nil
}

// loadRecords returns the unfinished experiments plus the newest `recent` ended ones, oldest first.
func (s *store) loadRecords(ctx context.Context, recent int) ([]*record, error) {
	// pgx always returns rows that carry a query error into CollectRows.
	rows, _ := s.db.Pool.Query(ctx, `SELECT doc FROM (
		  SELECT doc, created_at FROM experiments WHERE status IN ('scheduled', 'running', 'recovering')
		  UNION ALL
		  (SELECT doc, created_at FROM experiments WHERE status NOT IN ('scheduled', 'running', 'recovering')
		   ORDER BY created_at DESC LIMIT $1)
		) x ORDER BY created_at`, recent)
	recs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*record, error) {
		var raw []byte
		if err := row.Scan(&raw); err != nil { // coverage-ignore: a jsonb column always scans into []byte
			return nil, err
		}
		r := &record{}
		_ = json.Unmarshal(raw, r) // written by saveRecord only
		return r, nil
	})
	if err != nil {
		return nil, fmt.Errorf("experiments: load: %w", err)
	}
	return recs, nil
}
