package fakeexp

import (
	"context"
	"errors"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

var ctx = context.Background()

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newFake() (*Fake, *clock) {
	c := &clock{t: time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)}
	return New(c.now), c
}

func Test_shouldRunAnExperimentUntilItsDurationPassesWhenATrafficScenarioIsStarted(t *testing.T) {
	// GIVEN: a board with the fake catalog and nobody listening yet
	f, c := newFake()
	events, cancel := f.Subscribe()
	defer cancel()

	// WHEN: a 30 s traffic spike is started and 31 s pass
	e, err := f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic, DurationSec: 30, AutoRecover: true})
	manual, _ := f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic, DurationSec: 30})
	if err != nil {
		t.Fatal(err)
	}
	running := f.List()[1].Status
	c.t = c.t.Add(31 * time.Second)
	list := f.List()
	done, kept := list[1], list[0]

	// THEN: it was running, is completed now with a report, and the change was announced
	// AND: the experiment without auto-recovery keeps running until it is stopped
	if kept.ID != manual.ID || kept.Status != api.StatusRunning || kept.Category != "traffic" {
		t.Fatalf("manual %+v", kept)
	}
	if e.Status != api.StatusRunning || running != api.StatusRunning || len(f.Scenarios()) != 2 {
		t.Fatalf("started %+v", e)
	}
	if done.Status != api.StatusCompleted || done.EndedAt == nil || done.Report == nil || len(done.Params) != 0 {
		t.Fatalf("ended %+v", done)
	}
	if ev := <-events; ev.Experiment == nil || ev.Experiment.ID != e.ID {
		t.Fatalf("first event %+v", ev)
	}
}

func Test_shouldStartAScheduledExperimentLaterWhenAStartDelayIsGiven(t *testing.T) {
	// GIVEN: a traffic spike scheduled to start in 10 s with the default duration
	f, c := newFake()
	e, _ := f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic, StartDelaySec: 10, AutoRecover: true, Params: api.Values{"factor": 3.0}})

	// WHEN: 10 s pass
	c.t = c.t.Add(10 * time.Second)
	later := f.List()[0]

	// THEN: it was scheduled first, runs now and ends after the 60 s default duration
	if e.Status != api.StatusScheduled || later.Status != api.StatusRunning || later.EndsAt.Sub(later.StartsAt) != 60*time.Second {
		t.Fatalf("scheduled %+v later %+v", e, later)
	}
	if later.Params["factor"] != 3.0 {
		t.Fatalf("params %+v", later.Params)
	}
}

func Test_shouldRefuseTheStartWhenTheScenarioIsUnknownOrChaosIsNotArmedOrNotConfirmed(t *testing.T) {
	// GIVEN: a board with chaos disarmed
	f, _ := newFake()

	// WHEN: an unknown scenario, a disarmed broker stop and an unconfirmed armed broker stop are requested
	_, unknown := f.Start(ctx, api.StartRequest{Scenario: "nope"})
	_, disarmed := f.Start(ctx, api.StartRequest{Scenario: ScenarioBrokerStop})
	_ = f.Arm(ctx, true)
	_, unconfirmed := f.Start(ctx, api.StartRequest{Scenario: ScenarioBrokerStop, Confirm: "yes"})

	// THEN: each is refused with the matching reason
	if !errors.Is(unknown, api.ErrUnknownScenario) || !errors.Is(disarmed, api.ErrChaosDisarmed) || !errors.Is(unconfirmed, api.ErrConfirmationRequired) {
		t.Fatalf("errors %v / %v / %v", unknown, disarmed, unconfirmed)
	}
	if !f.Armed() {
		t.Fatal("chaos should be armed")
	}
}

func Test_shouldRefuseASecondBrokerFaultWhenOneIsAlreadyActive(t *testing.T) {
	// GIVEN: armed chaos and a running, confirmed broker stop
	f, _ := newFake()
	_ = f.Arm(ctx, true)
	req := api.StartRequest{Scenario: ScenarioBrokerStop, Confirm: ScenarioBrokerStop}
	if _, err := f.Start(ctx, req); err != nil {
		t.Fatal(err)
	}

	// WHEN: a second broker stop is requested
	_, err := f.Start(ctx, req)

	// THEN: it is refused because only one broker fault may be active
	if !errors.Is(err, api.ErrBrokerFaultActive) {
		t.Fatalf("got %v", err)
	}
}

func Test_shouldAbortAnExperimentOnceWhenItIsStoppedTwice(t *testing.T) {
	// GIVEN: a running traffic spike
	f, c := newFake()
	e, _ := f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic})

	// WHEN: it is stopped, time passes, and it is stopped again; an unknown id is stopped too
	first, _ := f.Stop(ctx, e.ID)
	c.t = c.t.Add(time.Minute)
	second, _ := f.Stop(ctx, e.ID)
	_, missing := f.Stop(ctx, "exp-999")

	// THEN: it was aborted once and the second stop returns it unchanged
	if first.Status != api.StatusAborted || second.EndedAt == nil || !second.EndedAt.Equal(*first.EndedAt) {
		t.Fatalf("first %+v second %+v", first, second)
	}
	if !errors.Is(missing, api.ErrNotFound) {
		t.Fatalf("missing %v", missing)
	}
}

func Test_shouldListActiveExperimentsAndOnlyTheLast20EndedWhenManyHaveEnded(t *testing.T) {
	// GIVEN: 22 ended experiments and one still running
	f, _ := newFake()
	for range 22 {
		e, _ := f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic})
		_, _ = f.Stop(ctx, e.ID)
	}
	_, _ = f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic})

	// WHEN: the board lists them
	list := f.List()

	// THEN: the running one comes first, followed by the 20 most recent ended ones
	if len(list) != 21 || list[0].Status != api.StatusRunning {
		t.Fatalf("got %d, first %s", len(list), list[0].Status)
	}
}

func Test_shouldRevertExperimentsKnobsAndScaleAndDisarmWhenPanicIsPressed(t *testing.T) {
	// GIVEN: armed chaos, a running experiment, paused payment and scaled-down shipping
	f, _ := newFake()
	_ = f.Arm(ctx, true)
	_, _ = f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic})
	_ = f.SetKnobs(ctx, "payment", api.Values{"paused": true})
	_ = f.Scale(ctx, "shipping", 0)

	// WHEN: panic is pressed
	n, err := f.Panic(ctx)

	// THEN: three things were reverted, chaos is disarmed and every service is back to baseline
	if err != nil || n != 3 || f.Armed() {
		t.Fatalf("reverted %d err %v armed %v", n, err, f.Armed())
	}
	for _, c := range f.Controls() {
		if c.Paused || len(c.Knobs) != 0 || c.Replicas != nil {
			t.Fatalf("not reverted: %+v", c)
		}
	}
}

func Test_shouldSwitchTheSimulationWhenStartStopAndResetAreRequested(t *testing.T) {
	// GIVEN: a running simulation
	f, _ := newFake()

	// WHEN: it is stopped, reset, started and asked for an unknown action
	stopped, _ := f.Simulation(ctx, "stop")
	state := f.SimulationState()
	reset, _ := f.Simulation(ctx, "reset")
	started, _ := f.Simulation(ctx, "start")
	_, err := f.Simulation(ctx, "explode")

	// THEN: the state follows the actions and the unknown action is invalid
	if stopped != "stopped" || state != "stopped" || reset != "running" || started != "running" || !errors.Is(err, api.ErrInvalid) {
		t.Fatalf("%s %s %s %s %v", stopped, state, reset, started, err)
	}
}

func Test_shouldTrackManualKnobsAndReplicasWhenAServiceIsOverridden(t *testing.T) {
	// GIVEN: payment with no overrides
	f, _ := newFake()

	// WHEN: payment is paused with extra latency, then the latency is removed, then it is scaled to 2
	_ = f.SetKnobs(ctx, "payment", api.Values{"paused": true, "processingLatencyMs": 200.0})
	_ = f.SetKnobs(ctx, "payment", api.Values{"processingLatencyMs": nil})
	_ = f.Scale(ctx, "payment", 2)
	c := f.Controls()[1]

	// THEN: payment is paused, keeps only the pause knob, has two generations and two replicas
	if c.Service != "payment" || !c.Paused || len(c.Knobs) != 1 || c.DesiredGeneration != 2 || *c.Replicas != 2 {
		t.Fatalf("got %+v", c)
	}
}

func Test_shouldGoBackToBaselineWhenTheKnobsOfAServiceAreReset(t *testing.T) {
	// GIVEN: a paused payment service
	f, _ := newFake()
	_ = f.SetKnobs(ctx, "payment", api.Values{"paused": true})

	// WHEN: its knobs are reset
	err := f.ResetKnobs(ctx, "payment")

	// THEN: it runs with the baseline again
	if c := f.Controls()[1]; err != nil || c.Paused || len(c.Knobs) != 0 {
		t.Fatalf("got %+v %v", c, err)
	}
}

func Test_shouldRejectOverridesWhenTheServiceIsUnknown(t *testing.T) {
	// GIVEN: a board that only knows the commerce services
	f, _ := newFake()

	// WHEN: knobs, a reset, scaling and tuning are requested for an unknown service
	errs := []error{
		f.SetKnobs(ctx, "billing", api.Values{"paused": true}),
		f.ResetKnobs(ctx, "billing"),
		f.Scale(ctx, "billing", 1),
	}
	_, terr := f.SetTuning(ctx, "billing", api.Values{"acks": "all"})

	// THEN: each is refused as an unknown service
	for _, err := range append(errs, terr) {
		if !errors.Is(err, api.ErrUnknownService) {
			t.Fatalf("got %v", err)
		}
	}
}

func Test_shouldStoreTuningValuesWhenTheyAreKnownSettings(t *testing.T) {
	// GIVEN: the Tuning Lab with default settings for payment
	f, _ := newFake()

	// WHEN: acks and linger are set, then linger is reset, then an unknown setting is sent
	_, _ = f.SetTuning(ctx, "payment", api.Values{"acks": "leader", "lingerMs": 50.0})
	state, _ := f.SetTuning(ctx, "payment", api.Values{"lingerMs": nil})
	_, err := f.SetTuning(ctx, "payment", api.Values{"turbo": true})

	// THEN: payment keeps acks=leader only, the catalog has eight settings and the unknown one is invalid
	if len(state.Services["payment"]) != 1 || state.Services["payment"]["acks"] != "leader" || len(f.Tuning().Settings) != 8 {
		t.Fatalf("got %+v", state.Services["payment"])
	}
	if !errors.Is(err, api.ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func Test_shouldDropEventsForASubscriberWhenItDoesNotKeepUp(t *testing.T) {
	// GIVEN: a subscriber that never reads
	f, _ := newFake()
	events, cancel := f.Subscribe()

	// WHEN: far more changes happen than its buffer holds, then it cancels twice
	for range 100 {
		_ = f.Arm(ctx, true)
	}
	cancel()
	cancel()

	// THEN: the engine was never blocked and the channel delivers the buffered events, then closes
	n := 0
	for range events {
		n++
	}
	if n != 64 {
		t.Fatalf("got %d events", n)
	}
}

func Test_shouldScoreTheDrillWhenThePlayerGuessesRightAfterAHintAndAWrongGuess(t *testing.T) {
	// GIVEN: no drill yet, then a medium drill for player "ada"
	f, c := newFake()
	d := f.Drills()
	none := d.Current()
	dr, err := d.Start(ctx, "medium", "ada")
	if err != nil {
		t.Fatal(err)
	}

	// WHEN: she takes a hint, guesses wrong, then right after 30 s
	_, _ = d.Hint(ctx, dr.ID)
	_, _ = d.Guess(ctx, dr.ID, ScenarioBrokerStop)
	c.t = c.t.Add(30 * time.Second)
	solved, _ := d.Guess(ctx, dr.ID, ScenarioTraffic)

	// THEN: the drill is solved with 100-10-5 points and she leads the leaderboard
	if none != nil || solved.Status != "solved" || *solved.Score != 85 || solved.Scenario != ScenarioTraffic || len(solved.Hints) != 1 {
		t.Fatalf("got %+v", solved)
	}
	if lb := d.Leaderboard(); len(lb) != 1 || lb[0].Player != "ada" || lb[0].DurationSec != 30 {
		t.Fatalf("leaderboard %+v", lb)
	}
	if cur := d.Current(); cur == nil || cur.Status != "solved" {
		t.Fatalf("current %+v", cur)
	}
}

func Test_shouldRankTheBetterScoreFirstWhenTwoDrillsWereSolved(t *testing.T) {
	// GIVEN: one drill solved after two hints
	f, _ := newFake()
	d := f.Drills()
	first, _ := d.Start(ctx, "easy", "bob")
	_, _ = d.Hint(ctx, first.ID)
	_, _ = d.Hint(ctx, first.ID)
	_, _ = d.Guess(ctx, first.ID, ScenarioTraffic)

	// WHEN: a second drill is solved without help
	second, _ := d.Start(ctx, "hard", "eve")
	_, _ = d.Guess(ctx, second.ID, ScenarioTraffic)

	// THEN: eve's perfect score ranks first
	if lb := d.Leaderboard(); lb[0].Player != "eve" || lb[1].Player != "bob" {
		t.Fatalf("leaderboard %+v", lb)
	}
}

func Test_shouldRefuseDrillActionsWhenTheDrillIsUnknownEndedOrAlreadyRunning(t *testing.T) {
	// GIVEN: a running drill
	f, _ := newFake()
	d := f.Drills()
	dr, _ := d.Start(ctx, "easy", "ada")

	// WHEN: a bad difficulty, a second drill, a hint for an unknown drill, a reveal and a guess after the reveal are requested
	_, badLevel := d.Start(ctx, "insane", "ada")
	_, twice := d.Start(ctx, "easy", "ada")
	_, unknown := d.Hint(ctx, "drill-0")
	revealed, _ := d.Reveal(ctx, dr.ID)
	_, ended := d.Guess(ctx, dr.ID, ScenarioTraffic)

	// THEN: each is refused with the matching reason and the reveal shows the scenario
	if !errors.Is(badLevel, api.ErrInvalid) || !errors.Is(twice, api.ErrDrillActive) || !errors.Is(unknown, api.ErrNotFound) || !errors.Is(ended, api.ErrDrillFinished) {
		t.Fatalf("%v / %v / %v / %v", badLevel, twice, unknown, ended)
	}
	if revealed.Status != "revealed" || revealed.Scenario != ScenarioTraffic {
		t.Fatalf("revealed %+v", revealed)
	}
}

func Test_shouldUseTheSystemClockWhenNoClockIsGiven(t *testing.T) {
	// GIVEN: a fake created without a clock
	f := New(nil)

	// WHEN: an experiment is started
	e, _ := f.Start(ctx, api.StartRequest{Scenario: ScenarioTraffic})

	// THEN: it is stamped with the current UTC time
	if time.Since(e.CreatedAt) > time.Minute || e.CreatedAt.Location() != time.UTC {
		t.Fatalf("created %v", e.CreatedAt)
	}
}
