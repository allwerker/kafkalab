package drills

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
)

var ctx = context.Background()

type fakeHost struct {
	startErr error
	started  []string
	revealed []string
	stopped  []string
	notes    []string
	audits   []string
}

var defs = map[string]api.ScenarioDef{
	"black-friday":   {ID: "black-friday", Name: "Black Friday", Category: "traffic", Dashboards: []api.DashboardLink{{Title: "Business KPIs", URL: "http://g/d/lab-business"}}},
	"poison-message": {ID: "poison-message", Name: "Poison Message", Category: "data", Dashboards: []api.DashboardLink{{Title: "Services", URL: "http://g/d/lab-services"}}},
}

func (h *fakeHost) Pool(difficulty string) []api.ScenarioDef {
	if difficulty == "hard" {
		return nil
	}
	return []api.ScenarioDef{defs["black-friday"], defs["poison-message"]}
}
func (h *fakeHost) Scenario(id string) api.ScenarioDef { return defs[id] }
func (h *fakeHost) Candidates() []api.DrillCandidate {
	return []api.DrillCandidate{{ID: "black-friday", Name: "Black Friday"}, {ID: "poison-message", Name: "Poison Message"}}
}
func (h *fakeHost) StartHidden(_ context.Context, scenario, _ string, _ int) (string, error) {
	if h.startErr != nil {
		return "", h.startErr
	}
	h.started = append(h.started, scenario)
	return "exp-1", nil
}
func (h *fakeHost) Affected(string) []string               { return []string{"order-generator"} }
func (h *fakeHost) Reveal(_ context.Context, id string)    { h.revealed = append(h.revealed, id) }
func (h *fakeHost) Stop(_ context.Context, id string)      { h.stopped = append(h.stopped, id) }
func (h *fakeHost) Notify(_ api.Drill, _, title, _ string) { h.notes = append(h.notes, title) }
func (h *fakeHost) Audit(_ context.Context, a string, _ map[string]string) {
	h.audits = append(h.audits, a)
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func setup(t *testing.T) (*Service, *fakeHost, *clock, *pg.DB) {
	t.Helper()
	db, err := pg.Open(ctx, pg.Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	h := &fakeHost{}
	c := &clock{t: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)}
	s, err := New(ctx, Config{DB: db, Host: h, Now: c.now, Rand: func() float64 { return 0 }, Metrics: metrics.NewSet()})
	if err != nil {
		t.Fatal(err)
	}
	return s, h, c, db
}

func Test_shouldScoreTimeHintsAndWrongGuessesWhenADrillIsSolved(t *testing.T) {
	// GIVEN: a drill solved after 100 s with 2 hints and 1 wrong guess, and a very slow one
	slow := Score(30*time.Minute, 0, 0)

	// WHEN: the score is computed
	score := Score(100*time.Second, 2, 1)

	// THEN: 1000 − 100 − 300 − 100 = 500, and never below zero
	if score != 500 || slow != 0 {
		t.Fatalf("score %v slow %v", score, slow)
	}
}

func Test_shouldPutTheBestPlayersFirstWhenDrillsAreSolved(t *testing.T) {
	// GIVEN: ada solves drill 1 after 50 s; bob solves drill 2 after 10 s
	s, h, c, db := setup(t)
	d1, _ := s.Start(ctx, "easy", "ada")
	c.t = c.t.Add(50 * time.Second)
	_, _ = s.Guess(ctx, d1.ID, "black-friday")
	d2, _ := s.Start(ctx, "medium", "  bob  ")
	c.t = c.t.Add(10 * time.Second)
	_, _ = s.Guess(ctx, d2.ID, "black-friday")

	// WHEN: the leaderboard is read, also by a restarted controlboard
	board := s.Leaderboard()
	again, err := New(ctx, Config{DB: db, Host: h, Now: c.now, Rand: func() float64 { return 0 }, Metrics: metrics.NewSet()})

	// THEN: bob (990) before ada (950), with durations; the experiment was revealed and stopped each time
	if len(board) != 2 || board[0].Player != "bob" || board[0].Score != 990 || board[1].DurationSec != 50 || board[0].Scenario != "black-friday" {
		t.Fatalf("board %+v", board)
	}
	if err != nil || len(again.Leaderboard()) != 2 || again.Current().ID != "drill-2" || len(h.revealed) != 2 || len(h.stopped) != 2 {
		t.Fatalf("again %v %+v", err, h)
	}
}

func Test_shouldRefuseASecondDrillWhenOneIsRunning(t *testing.T) {
	// GIVEN: a running drill
	s, _, _, _ := setup(t)
	_, _ = s.Start(ctx, "easy", "ada")

	// WHEN: another one and one with an unknown difficulty are started
	_, err1 := s.Start(ctx, "easy", "bob")
	_, err2 := s.Start(ctx, "nightmare", "bob")

	// THEN: busy and invalid
	if !errors.Is(err1, api.ErrDrillActive) || !errors.Is(err2, api.ErrInvalid) {
		t.Fatalf("%v %v", err1, err2)
	}
}

func Test_shouldRefuseDrillActionsWhenTheDrillIsUnknownOrOver(t *testing.T) {
	// GIVEN: a revealed drill
	s, _, _, _ := setup(t)
	d, _ := s.Start(ctx, "easy", "")
	_, _ = s.Reveal(ctx, d.ID)

	// WHEN: hint, guess and reveal are tried on it and on an unknown id
	_, e1 := s.Hint(ctx, d.ID)
	_, e2 := s.Guess(ctx, d.ID, "black-friday")
	_, e3 := s.Reveal(ctx, d.ID)
	_, e4 := s.Hint(ctx, "drill-99")

	// THEN: finished / not found
	if !errors.Is(e1, api.ErrDrillFinished) || !errors.Is(e2, api.ErrDrillFinished) || !errors.Is(e3, api.ErrDrillFinished) || !errors.Is(e4, api.ErrNotFound) {
		t.Fatalf("%v %v %v %v", e1, e2, e3, e4)
	}
}

func Test_shouldRejectAGuessWhenItIsNotACandidate(t *testing.T) {
	// GIVEN: a running drill
	s, _, _, _ := setup(t)
	d, _ := s.Start(ctx, "easy", "ada")

	// WHEN: a non-candidate is guessed
	_, err := s.Guess(ctx, d.ID, "meteor-strike")

	// THEN: invalid, and nothing counted
	if !errors.Is(err, api.ErrInvalid) || len(s.Current().Guesses) != 0 {
		t.Fatalf("%v", err)
	}
}

func Test_shouldExpireTheDrillWhenItsExperimentEndsUnsolved(t *testing.T) {
	// GIVEN: a running drill
	s, h, _, _ := setup(t)
	d, _ := s.Start(ctx, "easy", "ada")

	// WHEN: its experiment ends, and an unrelated experiment ends too
	s.ExperimentEnded(ctx, "exp-other")
	s.ExperimentEnded(ctx, "exp-1")

	// THEN: it expired, the scenario is revealed
	cur := s.Current()
	if cur.ID != d.ID || cur.Status != "expired" || cur.Scenario != "black-friday" || len(h.revealed) != 1 {
		t.Fatalf("%+v", cur)
	}
}

func Test_shouldReportUnavailableWhenNoScenarioOrDatabaseIsThere(t *testing.T) {
	// GIVEN: an empty hard pool, a host that cannot start, and later a closed database
	s, h, _, db := setup(t)

	// WHEN: drills are started in each situation
	_, e1 := s.Start(ctx, "hard", "ada")
	h.startErr = errors.New("kafka down")
	_, e2 := s.Start(ctx, "easy", "ada")
	h.startErr = nil
	long := "a-very-long-player-name-that-goes-on-and-on-and-on"
	d, e3 := s.Start(ctx, "easy", long)
	db.Close()
	_, e4 := s.Hint(ctx, d.ID)
	_, e5 := s.Guess(ctx, d.ID, "poison-message")
	_, e6 := s.Reveal(ctx, d.ID)
	s.current.Drill.Status = "running"
	_, e7 := s.Start(ctx, "easy", "x")

	// THEN: unavailable / the host error, names are truncated
	if !errors.Is(e1, api.ErrUnavailable) || e2 == nil || e3 != nil || !errors.Is(e4, api.ErrUnavailable) || !errors.Is(e5, api.ErrUnavailable) || !errors.Is(e6, api.ErrUnavailable) || !errors.Is(e7, api.ErrDrillActive) {
		t.Fatalf("%v %v %v %v %v %v %v", e1, e2, e3, e4, e5, e6, e7)
	}
	if _, err := New(ctx, Config{DB: db, Host: h, Metrics: metrics.NewSet()}); err == nil {
		t.Fatal("expected migrate error")
	}
}

func Test_shouldStopTheExperimentWhenTheDrillCannotBePersisted(t *testing.T) {
	// GIVEN: a closed database
	s, h, _, db := setup(t)
	db.Close()

	// WHEN: a drill is started
	_, err := s.Start(ctx, "easy", "ada")

	// THEN: it fails and the hidden experiment is stopped again
	if !errors.Is(err, api.ErrUnavailable) || len(h.stopped) != 1 || s.Current() != nil {
		t.Fatalf("%v %+v", err, h)
	}
}

func Test_shouldFailToLoadWhenTheDrillTableIsBroken(t *testing.T) {
	// GIVEN: a database whose drills table lost its doc column, and one without score column
	s, h, _, db := setup(t)
	_ = s
	_, _ = db.Pool.Exec(ctx, `ALTER TABLE drills RENAME COLUMN doc TO body`)

	// WHEN: the drills service starts
	_, err := New(ctx, Config{DB: db, Host: h, Metrics: metrics.NewSet()})
	_, _ = db.Pool.Exec(ctx, `ALTER TABLE drills RENAME COLUMN body TO doc; ALTER TABLE drills RENAME COLUMN score TO points`)
	_, err2 := New(ctx, Config{DB: db, Host: h, Metrics: metrics.NewSet()})
	_, _ = db.Pool.Exec(ctx, `ALTER TABLE drills RENAME COLUMN points TO score; ALTER TABLE drills ALTER COLUMN duration_sec TYPE text`)
	_, _ = db.Pool.Exec(ctx, `INSERT INTO drills VALUES ('d', 1, 'solved', 'p', 's', 1, 'x', now(), '{}')`)
	_, err3 := New(ctx, Config{DB: db, Host: h, Metrics: metrics.NewSet()})

	// THEN: it refuses to start
	if err == nil || err2 == nil || err3 == nil {
		t.Fatalf("%v %v %v", err, err2, err3)
	}
}

func Test_shouldExpireARunningDrillWhenTheServiceRestartsButFailIfItCannotBeSaved(t *testing.T) {
	// GIVEN: a running drill and a database that rejects updates afterwards
	s, h, _, db := setup(t)
	_, _ = s.Start(ctx, "easy", "ada")
	_, _ = db.Pool.Exec(ctx, `CREATE FUNCTION no_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'read only'; END $$;
		CREATE TRIGGER ro BEFORE UPDATE ON drills FOR EACH ROW EXECUTE FUNCTION no_update()`)

	// WHEN: the service restarts
	_, err := New(ctx, Config{DB: db, Host: h, Metrics: metrics.NewSet()})

	// THEN: it refuses to start with a stale running drill
	if !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("%v", err)
	}
}

func Test_shouldRevealMoreWithEveryHintAndCostPointsWhenThePlayerStruggles(t *testing.T) {
	// GIVEN: a running drill (Black Friday) and a metrics registry
	set := metrics.NewSet()
	s, h, c, _ := setup(t)
	s, _ = New(ctx, Config{DB: s.cfg.DB, Host: h, Now: c.now, Rand: func() float64 { return 0 }, Metrics: set})
	d, _ := s.Start(ctx, "easy", "ada")

	// WHEN: three hints are taken, a fourth is refused, one guess is wrong, then 60 s later right
	var last api.Drill
	for range 3 {
		last, _ = s.Hint(ctx, d.ID)
	}
	_, noMore := s.Hint(ctx, d.ID)
	wrong, _ := s.Guess(ctx, d.ID, "poison-message")
	c.t = c.t.Add(60 * time.Second)
	solved, _ := s.Guess(ctx, d.ID, "black-friday")

	// THEN: category, affected component and dashboard were revealed in that order
	if last.Hints[0] != "The incident is in the traffic area." || last.Hints[1] != "Affected: order-generator." ||
		last.Hints[2] != "Look at the Business KPIs dashboard: http://g/d/lab-business" || !errors.Is(noMore, api.ErrConflict) {
		t.Fatalf("hints %v %v", last.Hints, noMore)
	}
	// AND: 1000 − 60 − 3×150 − 100 = 390 points, exported as lab_drill_score
	var b strings.Builder
	set.WritePrometheus(&b)
	if wrong.Guesses[0].Correct || *solved.Score != 390 || !strings.Contains(b.String(), "lab_drill_score 390") || len(h.audits) < 5 {
		t.Fatalf("score %v metrics %s", *solved.Score, b.String())
	}
}
