// Package drills is the Incident Drill game: the engine runs a random scenario from a
// difficulty pool as a hidden experiment, the player gets progressive hints, guesses the
// scenario and scores points; solved drills enter a leaderboard in PostgreSQL.
package drills

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/kit/pg"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// Migrations returns the drill tables' migrations (101_*.sql).
func Migrations() fs.FS {
	sub, _ := fs.Sub(migrationFiles, "migrations")
	return sub
}

// Scoring.
const (
	BaseScore          = 1000
	PointsPerSecond    = 1
	HintPenalty        = 150
	WrongGuessPenalty  = 100
	MaxHints           = 3
	leaderboardEntries = 20
)

// Score computes the points of a solved drill (never below 0).
func Score(elapsed time.Duration, hints, wrong int) float64 {
	return math.Max(0, BaseScore-math.Floor(elapsed.Seconds())*PointsPerSecond-float64(hints*HintPenalty+wrong*WrongGuessPenalty))
}

// Host is the experiment engine as seen by the drills.
type Host interface {
	// Pool returns the drillable scenarios of a difficulty that can run right now.
	Pool(difficulty string) []api.ScenarioDef
	// Scenario returns a scenario definition (category, dashboards for hints).
	Scenario(id string) api.ScenarioDef
	// Candidates returns every scenario a player can guess.
	Candidates() []api.DrillCandidate
	// StartHidden starts the scenario as hidden experiment "Incident Drill #no".
	StartHidden(ctx context.Context, scenario, drillID string, no int) (experimentID string, err error)
	// Affected returns the (real) affected components of the experiment.
	Affected(experimentID string) []string
	// Reveal makes the experiment visible (backfills timeline/audit/annotation); Stop ends it.
	Reveal(ctx context.Context, experimentID string)
	Stop(ctx context.Context, experimentID string)
	// Notify publishes a drill change (WebSocket) and an optional timeline entry.
	Notify(d api.Drill, severity, title, detail string)
	Audit(ctx context.Context, action string, details map[string]string)
}

// Config configures New.
type Config struct {
	DB      *pg.DB
	Host    Host
	Now     func() time.Time
	Rand    func() float64
	Metrics *metrics.Set
}

// state is a drill with its secret parts.
type state struct {
	Drill        api.Drill `json:"drill"`
	No           int       `json:"no"`
	Player       string    `json:"player"`
	Scenario     string    `json:"scenario"`
	ExperimentID string    `json:"experimentId"`
	Wrong        int       `json:"wrong"`
}

// Service implements api.DrillAPI.
type Service struct {
	cfg     Config
	mu      sync.Mutex
	current *state
	board   []api.LeaderboardEntry
	score   float64
}

var _ api.DrillAPI = (*Service)(nil)

// New migrates, loads the leaderboard and the latest drill. A drill that was running when the
// previous controlboard stopped is expired (its experiment is reverted by the engine).
func New(ctx context.Context, cfg Config) (*Service, error) {
	if err := cfg.DB.Migrate(ctx, Migrations()); err != nil {
		return nil, fmt.Errorf("drills: migrate: %w", err)
	}
	s := &Service{cfg: cfg}
	cfg.Metrics.GetOrCreateGauge("lab_drill_score", func() float64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.score
	})
	var raw []byte
	err := cfg.DB.Pool.QueryRow(ctx, `SELECT doc FROM drills ORDER BY no DESC LIMIT 1`).Scan(&raw)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("drills: load: %w", err)
	}
	if err == nil {
		st := &state{}
		_ = json.Unmarshal(raw, st)
		s.current = st
		if st.Drill.Status == "running" {
			st.Drill.Status = "expired"
			st.Drill.Scenario = st.Scenario
			if err := s.save(ctx, st); err != nil {
				return nil, err
			}
		}
	}
	return s, s.loadBoard(ctx)
}

func (s *Service) loadBoard(ctx context.Context) error {
	// pgx always returns rows that carry a query error into CollectRows.
	rows, _ := s.cfg.DB.Pool.Query(ctx, `SELECT player, score, scenario, duration_sec, started_at FROM drills
		WHERE status = 'solved' ORDER BY score DESC, started_at LIMIT $1`, leaderboardEntries)
	board, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (api.LeaderboardEntry, error) {
		var e api.LeaderboardEntry
		err := r.Scan(&e.Player, &e.Score, &e.Scenario, &e.DurationSec, &e.At)
		e.At = e.At.UTC()
		return e, err
	})
	if err != nil {
		return fmt.Errorf("drills: leaderboard: %w", err)
	}
	s.mu.Lock()
	s.board = board
	s.mu.Unlock()
	return nil
}

func (s *Service) save(ctx context.Context, st *state) error {
	raw, _ := json.Marshal(st)
	var score, dur *float64
	if st.Drill.Score != nil {
		score = st.Drill.Score
		d := st.Drill.SolvedAt.Sub(st.Drill.StartedAt).Seconds()
		dur = &d
	}
	_, err := s.cfg.DB.Pool.Exec(ctx, `INSERT INTO drills (id, no, status, player, scenario, score, duration_sec, started_at, doc)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status, score = EXCLUDED.score, duration_sec = EXCLUDED.duration_sec, doc = EXCLUDED.doc`,
		st.Drill.ID, st.No, st.Drill.Status, st.Player, st.Scenario, score, dur, st.Drill.StartedAt, raw)
	if err != nil {
		return fmt.Errorf("%w: persist drill: %v", api.ErrUnavailable, err)
	}
	return nil
}

func clone(d api.Drill) api.Drill {
	d.Hints = slices.Clone(d.Hints)
	d.Guesses = slices.Clone(d.Guesses)
	d.Candidates = slices.Clone(d.Candidates)
	return d
}

// Start implements api.DrillAPI.
func (s *Service) Start(ctx context.Context, difficulty, player string) (api.Drill, error) {
	switch difficulty {
	case "easy", "medium", "hard":
	default:
		return api.Drill{}, fmt.Errorf("%w: difficulty must be easy, medium or hard", api.ErrInvalid)
	}
	player = strings.TrimSpace(player)
	if player == "" {
		player = "anonymous"
	}
	if len(player) > 40 {
		player = player[:40]
	}
	s.mu.Lock()
	if s.current != nil && s.current.Drill.Status == "running" {
		s.mu.Unlock()
		return api.Drill{}, fmt.Errorf("%w: Incident Drill #%d is still running", api.ErrDrillActive, s.current.No)
	}
	no := 1
	if s.current != nil {
		no = s.current.No + 1
	}
	s.mu.Unlock()
	pool := s.cfg.Host.Pool(difficulty)
	if len(pool) == 0 {
		return api.Drill{}, fmt.Errorf("%w: no %s scenario can run right now", api.ErrUnavailable, difficulty)
	}
	pick := pool[min(int(s.cfg.Rand()*float64(len(pool))), len(pool)-1)]
	now := s.cfg.Now().UTC()
	st := &state{No: no, Player: player, Scenario: pick.ID, Drill: api.Drill{ID: fmt.Sprintf("drill-%d", no), Status: "running",
		Difficulty: difficulty, StartedAt: now, Hints: []string{}, Guesses: []api.DrillGuess{}, Candidates: s.cfg.Host.Candidates()}}
	expID, err := s.cfg.Host.StartHidden(ctx, pick.ID, st.Drill.ID, no)
	if err != nil {
		return api.Drill{}, err
	}
	st.ExperimentID = expID
	if err := s.save(ctx, st); err != nil {
		s.cfg.Host.Stop(ctx, expID)
		return api.Drill{}, err
	}
	s.mu.Lock()
	s.current = st
	d := clone(st.Drill)
	s.mu.Unlock()
	s.cfg.Host.Audit(ctx, "drill.started", map[string]string{"drill": d.ID, "difficulty": difficulty, "player": player})
	s.cfg.Host.Notify(d, api.SeverityWarning, fmt.Sprintf("Incident Drill #%d started (%s) by %s", no, difficulty, player), "something is wrong — find out what")
	return d, nil
}

// running returns the current drill if id matches and it still runs (mu held).
func (s *Service) running(id string) (*state, error) {
	if s.current == nil || s.current.Drill.ID != id {
		return nil, fmt.Errorf("%w: drill %s", api.ErrNotFound, id)
	}
	if s.current.Drill.Status != "running" {
		return nil, fmt.Errorf("%w: drill %s is %s", api.ErrDrillFinished, id, s.current.Drill.Status)
	}
	return s.current, nil
}

// Hint implements api.DrillAPI: category, then the affected component, then a dashboard.
func (s *Service) Hint(ctx context.Context, id string) (api.Drill, error) {
	s.mu.Lock()
	st, err := s.running(id)
	if err != nil {
		s.mu.Unlock()
		return api.Drill{}, err
	}
	if st.Drill.HintsUsed >= MaxHints {
		s.mu.Unlock()
		return api.Drill{}, fmt.Errorf("%w: all %d hints are used", api.ErrConflict, MaxHints)
	}
	expID, scenario := st.ExperimentID, st.Scenario
	s.mu.Unlock()
	def := s.cfg.Host.Scenario(scenario)
	var hint string
	switch st.Drill.HintsUsed {
	case 0:
		hint = "The incident is in the " + def.Category + " area."
	case 1:
		hint = "Affected: " + strings.Join(s.cfg.Host.Affected(expID), ", ") + "."
	default:
		hint = "Look at the " + def.Dashboards[0].Title + " dashboard: " + def.Dashboards[0].URL
	}
	s.mu.Lock()
	st.Drill.HintsUsed++
	st.Drill.Hints = append(st.Drill.Hints, hint)
	d := clone(st.Drill)
	s.mu.Unlock()
	if err := s.save(ctx, st); err != nil {
		return api.Drill{}, err
	}
	s.cfg.Host.Audit(ctx, "drill.hint", map[string]string{"drill": id, "hint": fmt.Sprint(d.HintsUsed)})
	s.cfg.Host.Notify(d, api.SeverityInfo, fmt.Sprintf("Incident Drill #%d: hint %d", st.No, d.HintsUsed), hint)
	return d, nil
}

// Guess implements api.DrillAPI. A correct guess solves the drill: the experiment is revealed
// and stopped, the score enters the leaderboard.
func (s *Service) Guess(ctx context.Context, id, scenario string) (api.Drill, error) {
	s.mu.Lock()
	st, err := s.running(id)
	if err != nil {
		s.mu.Unlock()
		return api.Drill{}, err
	}
	if !slices.ContainsFunc(st.Drill.Candidates, func(c api.DrillCandidate) bool { return c.ID == scenario }) {
		s.mu.Unlock()
		return api.Drill{}, fmt.Errorf("%w: %q is not a candidate scenario", api.ErrInvalid, scenario)
	}
	now := s.cfg.Now().UTC()
	correct := scenario == st.Scenario
	st.Drill.Guesses = append(st.Drill.Guesses, api.DrillGuess{Scenario: scenario, Correct: correct, At: now})
	if !correct {
		st.Wrong++
	} else {
		score := Score(now.Sub(st.Drill.StartedAt), st.Drill.HintsUsed, st.Wrong)
		st.Drill.Status, st.Drill.SolvedAt, st.Drill.Score, st.Drill.Scenario = "solved", &now, &score, st.Scenario
		s.score = score
	}
	d := clone(st.Drill)
	s.mu.Unlock()
	if err := s.save(ctx, st); err != nil {
		return api.Drill{}, err
	}
	s.cfg.Host.Audit(ctx, "drill.guess", map[string]string{"drill": id, "guess": scenario, "correct": fmt.Sprint(correct)})
	if !correct {
		s.cfg.Host.Notify(d, api.SeverityInfo, fmt.Sprintf("Incident Drill #%d: wrong guess", st.No), scenario+" is not it")
		return d, nil
	}
	s.end(ctx, st)
	s.cfg.Host.Notify(d, api.SeveritySuccess, fmt.Sprintf("Incident Drill #%d solved by %s: %.0f points", st.No, st.Player, *d.Score), "it was "+st.Scenario)
	return d, s.loadBoard(ctx)
}

// end reveals and stops the hidden experiment (never with mu held: the engine calls back).
func (s *Service) end(ctx context.Context, st *state) {
	s.cfg.Host.Reveal(ctx, st.ExperimentID)
	s.cfg.Host.Stop(ctx, st.ExperimentID)
}

// Reveal implements api.DrillAPI: the player gives up (no score).
func (s *Service) Reveal(ctx context.Context, id string) (api.Drill, error) {
	s.mu.Lock()
	st, err := s.running(id)
	if err != nil {
		s.mu.Unlock()
		return api.Drill{}, err
	}
	st.Drill.Status, st.Drill.Scenario = "revealed", st.Scenario
	d := clone(st.Drill)
	s.mu.Unlock()
	if err := s.save(ctx, st); err != nil {
		return api.Drill{}, err
	}
	s.cfg.Host.Audit(ctx, "drill.revealed", map[string]string{"drill": id})
	s.end(ctx, st)
	s.cfg.Host.Notify(d, api.SeverityInfo, fmt.Sprintf("Incident Drill #%d revealed", st.No), "it was "+st.Scenario)
	return d, nil
}

// ExperimentEnded is called by the engine when a drill experiment ended. An unsolved drill
// expires and its experiment is revealed.
func (s *Service) ExperimentEnded(ctx context.Context, experimentID string) {
	s.mu.Lock()
	st := s.current
	if st == nil || st.ExperimentID != experimentID || st.Drill.Status != "running" {
		s.mu.Unlock()
		return
	}
	st.Drill.Status, st.Drill.Scenario = "expired", st.Scenario
	d := clone(st.Drill)
	s.mu.Unlock()
	_ = s.save(ctx, st) // best effort: a lost update is repaired on the next start
	s.cfg.Host.Reveal(ctx, experimentID)
	s.cfg.Host.Notify(d, api.SeverityWarning, fmt.Sprintf("Incident Drill #%d expired", st.No), "it was "+st.Scenario)
}

// Leaderboard implements api.DrillAPI.
func (s *Service) Leaderboard() []api.LeaderboardEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.board)
}

// Current implements api.DrillAPI.
func (s *Service) Current() *api.Drill {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil {
		return nil
	}
	d := clone(s.current.Drill)
	return &d
}
