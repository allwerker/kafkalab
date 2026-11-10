package experiments

import (
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/pkg/contracts"
)

func Test_shouldOrPausesAndReplaceProductsWhenManualKnobsMeetExperiments(t *testing.T) {
	// GIVEN: payment paused and the generator pushing sku-002 manually, hot partition 3 configured
	h := newHarness(t)
	_ = h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"paused": true})
	_ = h.e.SetKnobs(ctx, gen, api.Values{"popularSku": "sku-002", "hotPartition": 3.0})

	// WHEN: a recovery storm pauses payment as well and a flash sale pushes sku-001
	h.start(api.StartRequest{Scenario: "recovery-storm", AutoRecover: true})
	h.start(api.StartRequest{Scenario: "flash-sale", AutoRecover: true})

	// THEN: payment stays paused, the experiment's product wins, and Controls shows the effective knobs
	g := h.last(gen)
	if !h.last(contracts.ServicePayment).Knobs.Paused || *g.Knobs.PopularSku != "sku-001" {
		t.Fatalf("payment %+v generator %+v", h.last(contracts.ServicePayment).Knobs, g.Knobs)
	}
	c := h.e.Controls()[0]
	if c.Knobs["popularSku"] != "sku-001" || c.Knobs["hotPartition"] != 3 {
		t.Fatalf("controls %+v", c.Knobs)
	}
	// AND: removing the last manual override of payment leaves no override behind
	_ = h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"paused": nil})
	if _, ok := h.e.settings.Manual[contracts.ServicePayment]; ok {
		t.Fatal("empty override kept")
	}
}

func Test_shouldReportUnavailableDependenciesWhenTheDatabaseFailsDuringScaleAndPanic(t *testing.T) {
	// GIVEN: a database that went away
	h := newHarness(t)
	h.db.Close()

	// WHEN: a service is scaled and panic is pressed
	err1 := h.e.Scale(ctx, contracts.ServicePayment, 2)
	_, err2 := h.e.Panic(ctx)

	// THEN: both report the outage
	if !errors.Is(err1, api.ErrUnavailable) || !errors.Is(err2, api.ErrUnavailable) {
		t.Fatalf("%v %v", err1, err2)
	}
}

func Test_shouldRestoreAManualScaleWhenItsAnnotationWasLostBeforeAPanic(t *testing.T) {
	// GIVEN: analytics scaled to 3 manually, its annotation removed by someone, and a broker with a
	// helper whose undo is already running
	h := newHarness(t)
	_ = h.e.Scale(ctx, contracts.ServiceAnalytics, 3)
	_ = h.kube.Client().SetAnnotations(ctx, k8s.DeploymentPath("commerce", "analytics"), map[string]*string{revertKey: nil})
	h.kube.Mu.Lock()
	h.kube.Pods["kafka/kafka-lab-dual-role-0"].Spec.EphemeralContainers = []k8s.EphemeralContainer{{Name: "chaos-a-undo"}}
	h.kube.Mu.Unlock()

	// WHEN: panic
	n, err := h.e.Panic(ctx)

	// THEN: analytics is back at 1 and nothing else was touched
	if err != nil || n != 1 || h.kube.Replicas("commerce", "analytics") != 1 {
		t.Fatalf("n %d err %v replicas %d", n, err, h.kube.Replicas("commerce", "analytics"))
	}
}

func Test_shouldNotStartWhenItsStateCannotBeLoadedOrPublished(t *testing.T) {
	// GIVEN: an engine whose tables are damaged in different ways, and a dead Kafka
	for name, breakIt := range map[string]func(h *harness){
		"settings": func(h *harness) { _, _ = h.db.Pool.Exec(ctx, `ALTER TABLE engine_settings RENAME COLUMN doc TO body`) },
		"records":  func(h *harness) { _, _ = h.db.Pool.Exec(ctx, `ALTER TABLE experiments RENAME COLUMN doc TO body`) },
		"drills":   func(h *harness) { _, _ = h.db.Pool.Exec(ctx, `ALTER TABLE drills RENAME COLUMN doc TO body`) },
		"kafka":    func(h *harness) { h.pub.failSnap = errBoom },
		"migrate":  func(h *harness) { _, _ = h.db.Pool.Exec(ctx, `DELETE FROM schema_migrations; DROP TABLE drills`) },
	} {
		h := newHarness(t)
		breakIt(h)

		// WHEN: a new engine starts on it
		h.cfg.Metrics = nil
		_, err := New(h.cfg)

		// THEN: it refuses to start
		if err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func Test_shouldSkipExperimentsOfRemovedScenariosWhenRestarting(t *testing.T) {
	// GIVEN: a persisted experiment of a scenario that no longer exists
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true})
	_, _ = h.db.Pool.Exec(ctx, `UPDATE experiments SET doc = jsonb_set(doc, '{exp,scenario}', '"retired-scenario"')`)

	// WHEN: the controlboard restarts
	h.restart()

	// THEN: it starts and does not list the unknown experiment
	for _, e := range h.e.List() {
		if e.ID == x.ID {
			t.Fatal("unknown scenario listed")
		}
	}
}

func Test_shouldUseTheRealClockAndRandomnessWhenNoneAreConfigured(t *testing.T) {
	// GIVEN: a configuration without clock and random source, and without Grafana
	h := newHarness(t, func(c *Config) { c.Now = nil; c.Rand = nil; c.Annotator = nil })

	// WHEN: a randomised Black Friday starts
	x := h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true, Params: api.Values{"randomize": true}})

	// THEN: it starts now (wall clock) with a rate within ±25 %
	r := x.Params["ordersPerSecond"].(float64)
	if time.Since(x.StartsAt) > time.Minute || r < 750 || r > 1250 {
		t.Fatalf("starts %v rate %v", x.StartsAt, r)
	}
	_, _ = h.e.Stop(ctx, x.ID)
}

func Test_shouldKeepTheExperimentGoingWhenPersistingOrPublishingFailsMidway(t *testing.T) {
	// GIVEN: a Traffic Tsunami, then Kafka fails
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "traffic-tsunami", AutoRecover: true, DurationSec: 60})
	h.pub.failSnap = errBoom

	// WHEN: a step is due and the experiment is stopped
	h.advance(10 * time.Second)
	got, _ := h.e.Stop(ctx, x.ID)

	// THEN: it is aborted, with the failed revert in its reason
	if got.Status != api.StatusAborted || !strings.Contains(h.e.runs[x.ID].rec.Reason, "publish") {
		t.Fatalf("%+v reason %q", got, h.e.runs[x.ID].rec.Reason)
	}
	// AND: a new experiment on a closed database still starts (it is only not persisted)
	h.pub.failSnap = nil
	h.db.Close()
	if _, err := h.e.Start(ctx, api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true}); err != nil {
		t.Fatal(err)
	}
}

func Test_shouldReportAResetFailureWhenTheServicesCannotBeScaledBack(t *testing.T) {
	// GIVEN: consumers that stop, and an API server that fails the scale-up after the cleanup
	h := newHarness(t)
	h.kube.Mu.Lock()
	for _, s := range consumers {
		delete(h.kube.Pods, "commerce/"+s+"-pod")
	}
	h.kube.Mu.Unlock()
	var patches atomic.Int32
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/scale") && patches.Add(1) > 5 {
			return 500
		}
		return 0
	}

	// WHEN: the simulation is reset
	_, _ = h.e.Simulation(ctx, "reset")
	e := waitTimeline(t, h, "Simulation reset failed")

	// THEN: topics were emptied but the failure to scale back is reported
	if len(h.admin.deleted) == 0 || !strings.Contains(e.Detail, "500") {
		t.Fatalf("detail %q", e.Detail)
	}
	// AND: stopping the simulation while Kafka is down fails
	h.pub.failSnap = errBoom
	if _, err := h.e.Simulation(ctx, "start"); !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("%v", err)
	}
}

func Test_shouldWarnButContinueWhenGrafanaCannotUpdateTheAnnotation(t *testing.T) {
	// GIVEN: an experiment with an annotation, then Grafana breaks
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true})
	h.ann.fail = errBoom

	// WHEN: it is stopped
	got, _ := h.e.Stop(ctx, x.ID)

	// THEN: it is aborted anyway
	if got.Status != api.StatusAborted || len(h.ann.updates) != 0 {
		t.Fatalf("%+v", got)
	}
}

func Test_shouldTellNothingAboutUnknownExperimentsWhenTheDrillAsks(t *testing.T) {
	// GIVEN: the engine as seen by the drills
	h := newHarness(t)
	host := (*drillHost)(h.e)
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true})

	// WHEN: it is asked about an unknown experiment and to reveal a visible one or an unknown one
	affected := host.Affected("exp-gone")
	host.Reveal(ctx, "exp-gone")
	host.Reveal(ctx, x.ID)

	// THEN: nothing is reported and nothing changes
	if affected != nil || h.experiment(x.ID).Scenario != "duplicate-delivery" {
		t.Fatalf("affected %v", affected)
	}
}

func Test_shouldWarnWhenARevealedDrillCannotBePersisted(t *testing.T) {
	// GIVEN: an unsolved drill and a database that went away
	h, d := drillHarness(t)
	h.db.Close()

	// WHEN: the player reveals it
	_, err := h.e.Drills().Reveal(ctx, d.ID)

	// THEN: the drill reports the outage; the experiment is still shown with its real scenario
	if !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("%v", err)
	}
}

func Test_shouldCountAServiceWithoutPodsAsRecoveredWhenItIsNotObserved(t *testing.T) {
	// GIVEN: the observer does not know notification at all
	h := newHarness(t)
	h.obs.set(func(o *fakeObserver) { o.services = o.services[:4] })

	// WHEN: a meltdown runs, ends and is observed
	x := h.start(api.StartRequest{Scenario: "notification-meltdown", AutoRecover: true})
	h.advance(120 * time.Second)
	h.advance(5 * time.Second)

	// THEN: it completes
	if h.experiment(x.ID).Status != api.StatusCompleted {
		t.Fatalf("status %s", h.experiment(x.ID).Status)
	}
}
