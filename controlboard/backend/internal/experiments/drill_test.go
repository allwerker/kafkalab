package experiments

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

// A drill picks from its pool with Rand; 0 picks the first easy scenario (black-friday).
func drillHarness(t *testing.T) (*harness, api.Drill) {
	h := newHarness(t, func(c *Config) { c.Rand = func() float64 { return 0 } })
	d, err := h.e.Drills().Start(ctx, "easy", "ada")
	if err != nil {
		t.Fatal(err)
	}
	return h, d
}

// secrets are strings that must not appear anywhere while the drill is unsolved.
var secrets = []string{"black-friday", "Black Friday", "traffic", "ordersPerSecond", "paymentLatencyMs"}

func leaks(s string) string {
	for _, x := range secrets {
		if strings.Contains(s, x) {
			return x
		}
	}
	return ""
}

func Test_shouldPublishOnlyTheOpaqueIdInTheSnapshotWhenADrillIsUnsolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill (Black Friday underneath)
	h, _ := drillHarness(t)

	// WHEN: the generator's snapshot is inspected
	s := h.last("order-generator")

	// THEN: it carries the knobs it needs but only the opaque experiment id
	if len(s.Experiments) != 1 || !strings.HasPrefix(s.Experiments[0], "exp-") || leaks(s.Experiments[0]) != "" || *s.Knobs.OrdersPerSecond != 1000 {
		t.Fatalf("snapshot %+v", s)
	}
}

func Test_shouldAuditTheDrillAsHiddenWhenItIsUnsolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill
	h, _ := drillHarness(t)

	// WHEN: the audit events are inspected
	b, _ := json.Marshal(h.pub.audits)

	// THEN: the experiment's scenario is "hidden" and no parameter or name leaks
	if leak := leaks(string(b)); leak != "" || !strings.Contains(string(b), `"scenario":"hidden"`) || !contains(h.auditActions(), "drill.started") {
		t.Fatalf("leak %q in %s", leak, b)
	}
}

func Test_shouldLabelTheMetricsHiddenWhenADrillIsUnsolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill
	h, _ := drillHarness(t)

	// WHEN: the metrics are scraped
	m := h.exposition()

	// THEN: the active series is labelled hidden for scenario, category and severity
	if leak := leaks(m); leak != "" || !strings.Contains(m, `scenario="hidden",category="hidden",severity="hidden"} 1`) {
		t.Fatalf("leak %q in %s", leak, m)
	}
}

func Test_shouldShowARedactedExperimentInTheApiWhenADrillIsUnsolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill that has been observed for a while
	h, d := drillHarness(t)
	h.advance(10 * time.Second)

	// WHEN: the board lists the experiments
	b, _ := json.Marshal(h.e.List())

	// THEN: the experiment is "Incident Drill #1", scenario hidden, without params, targets, observations, report
	x := h.e.List()[0]
	if leak := leaks(string(b)); leak != "" || x.Scenario != "hidden" || x.Name != "Incident Drill #1" || len(x.Params) != 0 ||
		len(x.Affected) != 0 || len(x.Observations) != 0 || x.Report != nil || x.Category != "" {
		t.Fatalf("leak %q: %s", leak, b)
	}
	// AND: the drill itself shows no scenario but every candidate
	if d.Scenario != "" || len(d.Candidates) < 10 || d.Status != "running" {
		t.Fatalf("drill %+v", d)
	}
}

func Test_shouldWriteNeutralTimelineEntriesWhenADrillIsUnsolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill that runs to its revert
	h, _ := drillHarness(t)
	h.advance(5 * time.Minute)

	// WHEN: the timeline is read
	b, _ := json.Marshal(h.timeline())

	// THEN: no entry names the scenario
	if leak := leaks(string(b)); leak != "" || !strings.Contains(string(b), "Incident Drill #1") {
		t.Fatalf("leak %q in %s", leak, b)
	}
}

func Test_shouldAnnotateGrafanaNeutrallyAndBackfillWhenTheDrillIsSolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill with a hidden Grafana annotation
	h, d := drillHarness(t)
	before := strings.Join(h.ann.starts, " ")

	// WHEN: the player guesses wrong, then right
	wrong, err1 := h.e.Drills().Guess(ctx, d.ID, "poison-message")
	solved, err2 := h.e.Drills().Guess(ctx, d.ID, "black-friday")

	// THEN: the start annotation was neutral, the backfill names the scenario, the drill is solved
	if leaks(before) != "" || !strings.Contains(before, "hidden") || err1 != nil || err2 != nil {
		t.Fatalf("annotation %q: %v %v", before, err1, err2)
	}
	if !strings.Contains(strings.Join(h.ann.updates, " "), "Incident Drill #1: Black Friday [chaos,black-friday,traffic]") {
		t.Fatalf("updates %v", h.ann.updates)
	}
	if wrong.Guesses[0].Correct || solved.Status != "solved" || solved.Scenario != "black-friday" || *solved.Score != 900 {
		t.Fatalf("drill %+v", solved)
	}
	// AND: the experiment is now visible and stopped, and the leaderboard has the player
	x := h.e.List()[0]
	if x.Scenario != "black-friday" || x.Status != api.StatusAborted || h.e.Drills().Leaderboard()[0].Player != "ada" {
		t.Fatalf("experiment %+v board %+v", x, h.e.Drills().Leaderboard())
	}
	if !strings.Contains(h.exposition(), `lab_drill_score 900`) {
		t.Fatal("score metric missing")
	}
}

func Test_shouldExpireAndRevealTheDrillWhenItsExperimentEndsUnsolved(t *testing.T) {
	// GIVEN: an unsolved Incident Drill
	h, d := drillHarness(t)

	// WHEN: its 5 minutes pass and the system recovers
	h.advance(5 * time.Minute)
	h.appliedAll()
	h.advance(10 * time.Second)

	// THEN: the drill expired, the experiment is revealed, further guesses are refused
	cur := h.e.Drills().Current()
	_, err := h.e.Drills().Guess(ctx, d.ID, "black-friday")
	if cur.Status != "expired" || cur.Scenario != "black-friday" || h.e.List()[0].Scenario != "black-friday" || !errors.Is(err, api.ErrDrillFinished) {
		t.Fatalf("drill %+v err %v", cur, err)
	}
}

func Test_shouldRevealTheScenarioWhenThePlayerGivesUp(t *testing.T) {
	// GIVEN: an unsolved Incident Drill after one hint
	h, d := drillHarness(t)
	hinted, err := h.e.Drills().Hint(ctx, d.ID)

	// WHEN: the player reveals it
	got, err2 := h.e.Drills().Reveal(ctx, d.ID)

	// THEN: the hint named the category, the scenario is shown, the experiment stopped, no score
	if err != nil || err2 != nil || hinted.Hints[0] != "The incident is in the traffic area." || got.Status != "revealed" || got.Score != nil || got.Scenario != "black-friday" {
		t.Fatalf("%+v %+v %v %v", hinted, got, err, err2)
	}
	if x := h.e.List()[0]; x.Status != api.StatusAborted || x.Scenario != "black-friday" {
		t.Fatalf("experiment %+v", x)
	}
}

func Test_shouldGiveProgressiveHintsWhenThePlayerAsks(t *testing.T) {
	// GIVEN: an unsolved Incident Drill
	h, d := drillHarness(t)

	// WHEN: all three hints and a fourth are requested
	var last api.Drill
	for range 3 {
		last, _ = h.e.Drills().Hint(ctx, d.ID)
	}
	_, err := h.e.Drills().Hint(ctx, d.ID)

	// THEN: category, affected components, dashboard — then no more
	if len(last.Hints) != 3 || !strings.Contains(last.Hints[1], "order-generator") || !strings.Contains(last.Hints[2], "/d/lab-business") || !errors.Is(err, api.ErrConflict) {
		t.Fatalf("hints %v err %v", last.Hints, err)
	}
}

func Test_shouldExpireARunningDrillWhenTheControlboardRestarts(t *testing.T) {
	// GIVEN: an unsolved Incident Drill
	h, d := drillHarness(t)

	// WHEN: the controlboard restarts
	h.restart()

	// THEN: the drill is expired, its experiment aborted and visible, a new drill gets number 2
	cur := h.e.Drills().Current()
	if cur.ID != d.ID || cur.Status != "expired" || h.e.List()[0].Status != api.StatusAborted || h.e.List()[0].Scenario != "black-friday" {
		t.Fatalf("drill %+v list %+v", cur, h.e.List()[0])
	}
	next, err := h.e.Drills().Start(ctx, "medium", "bob")
	if err != nil || next.ID != "drill-2" {
		t.Fatalf("%+v %v", next, err)
	}
}

func Test_shouldOnlyDrillScenariosThatCanRunWhenTheClusterIsUnavailable(t *testing.T) {
	// GIVEN: an engine without Kubernetes
	h := newHarness(t, func(c *Config) { c.Kube = nil; c.Rand = func() float64 { return 0.99 } })

	// WHEN: a hard drill starts
	d, err := h.e.Drills().Start(ctx, "hard", "eve")

	// THEN: it runs a scenario without Kubernetes and no candidate needs the cluster
	if err != nil || h.e.runs[h.e.order[len(h.e.order)-1]].scenario.Kube {
		t.Fatalf("%+v %v", d, err)
	}
	for _, c := range d.Candidates {
		if c.ID == "consumer-crash-loop" || c.ID == "network-chaos" {
			t.Fatalf("candidate %s should not be offered", c.ID)
		}
	}
}

func Test_shouldFailTheDrillStartWhenItsExperimentCannotStart(t *testing.T) {
	// GIVEN: Kafka is down
	h := newHarness(t, func(c *Config) { c.Rand = func() float64 { return 0 } })
	h.pub.failSnap = errBoom

	// WHEN: a drill is started
	_, err := h.e.Drills().Start(ctx, "easy", "ada")

	// THEN: it is unavailable and no drill is current
	if !errors.Is(err, api.ErrUnavailable) || h.e.Drills().Current() != nil {
		t.Fatalf("err %v", err)
	}
}
