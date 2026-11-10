package experiments

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/contracts"
)

func Test_shouldPublishABaselineForEveryServiceWhenTheEngineStarts(t *testing.T) {
	// GIVEN: platform.control already holds generation 41 from an earlier controlboard
	h := newHarness(t, func(c *Config) {
		c.Generations = func(context.Context) (int64, error) { return 41, nil }
	})

	// WHEN: the engine has started
	snaps := h.pub.snaps

	// THEN: every service got a full baseline snapshot without expiry, numbered after 41
	if len(snaps) != len(contracts.Services) {
		t.Fatalf("%d snapshots", len(snaps))
	}
	for i, s := range snaps {
		if s.Generation != int64(42+i) || s.ExpiresAt != nil || len(s.Experiments) != 0 || s.Knobs.ProcessingLatencyMs != 0 {
			t.Fatalf("snapshot %+v", s)
		}
	}
	if h.e.SimulationState() != "running" || h.e.Armed() {
		t.Fatal("expected running simulation and disarmed chaos")
	}
}

func Test_shouldRaiseTheOrderRateUntilShortlyAfterTheEndWhenBlackFridayStarts(t *testing.T) {
	// GIVEN: a calm shop
	h := newHarness(t)

	// WHEN: Black Friday starts for 180 s with 1000 orders/s and slower payment and inventory
	x := h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true})

	// THEN: the generator, payment and inventory get full snapshots that expire 30 s after the end
	g := h.last(contracts.ServiceOrderGenerator)
	p := h.last(contracts.ServicePayment)
	want := x.EndsAt.Add(30 * time.Second)
	if *g.Knobs.OrdersPerSecond != 1000 || p.Knobs.ProcessingLatencyMs != 150 || h.last(contracts.ServiceInventory).Knobs.ProcessingLatencyMs != 100 {
		t.Fatalf("knobs %+v / %+v", g.Knobs, p.Knobs)
	}
	if g.ExpiresAt == nil || !g.ExpiresAt.Equal(want) || len(g.Experiments) != 1 || g.Experiments[0] != x.ID {
		t.Fatalf("snapshot %+v", g)
	}
	// AND: it is running, audited, annotated in Grafana and visible as a metric
	if x.Status != api.StatusRunning || x.Category != "traffic" || x.Severity != "high" || !contains(h.auditActions(), "experiment.started") {
		t.Fatalf("experiment %+v audit %v", x, h.auditActions())
	}
	if len(h.ann.starts) != 1 || !strings.Contains(h.exposition(), `lab_experiment_active{experiment_id="`+x.ID+`",scenario="black-friday",category="traffic",severity="high"} 1`) {
		t.Fatalf("annotations %v metrics %s", h.ann.starts, h.exposition())
	}
}

func Test_shouldRestorePaymentsBaselineKnobsWhenPaymentProviderDownEnds(t *testing.T) {
	// GIVEN: the payment provider has been down for its 90 s
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "payment-provider-down", AutoRecover: true})
	down := h.last(contracts.ServicePayment)

	// WHEN: the duration is over
	h.advance(90 * time.Second)

	// THEN: payment gets a freshly computed baseline snapshot with a higher generation and no expiry
	up := h.last(contracts.ServicePayment)
	if !down.Knobs.ProviderDown || up.Knobs.ProviderDown || up.Generation <= down.Generation || up.ExpiresAt != nil || len(up.Experiments) != 0 {
		t.Fatalf("down %+v up %+v", down, up)
	}
	if got := h.experiment(x.ID); got.Status != api.StatusRecovering {
		t.Fatalf("status %s", got.Status)
	}
	// AND: once payment applied it and the lag is normal, the experiment completes with a recovery time
	h.appliedAll()
	h.advance(6 * time.Second)
	got := h.experiment(x.ID)
	if got.Status != api.StatusCompleted || got.Report == nil || got.Report.TimeToRecoverSec == nil || *got.Report.TimeToRecoverSec != 6 {
		t.Fatalf("experiment %+v report %+v", got, got.Report)
	}
	if !strings.Contains(h.exposition(), `lab_experiments_total{scenario="payment-provider-down",outcome="completed"} 1`) ||
		!strings.Contains(h.exposition(), `lab_experiment_recovery_seconds_count{scenario="payment-provider-down"} 1`) ||
		strings.Contains(h.exposition(), "lab_experiment_active{") {
		t.Fatalf("metrics %s", h.exposition())
	}
	if len(h.ann.updates) != 1 || h.ann.ends[0].IsZero() {
		t.Fatalf("annotation region not closed: %v", h.ann.updates)
	}
}

func Test_shouldCompleteWithoutRecoveryTimeWhenTheSystemDoesNotRecoverInTime(t *testing.T) {
	// GIVEN: a silent failure whose lag never goes down after the revert
	h := newHarness(t, func(c *Config) { c.RecoveryTimeout = time.Minute })
	x := h.start(api.StartRequest{Scenario: "silent-failure", AutoRecover: true})
	h.obs.set(func(o *fakeObserver) { o.lag = 50000 })
	h.advance(120 * time.Second)

	// WHEN: the recovery timeout passes
	h.advance(61 * time.Second)

	// THEN: the experiment completes, but the report has no time to recover
	got := h.experiment(x.ID)
	if got.Status != api.StatusCompleted || got.Report.TimeToRecoverSec != nil || got.Report.PeakLag != 50000 {
		t.Fatalf("experiment %+v report %+v", got, got.Report)
	}
}

func Test_shouldKeepTheManualOverrideWhenAnExperimentOnTheSameServiceEnds(t *testing.T) {
	// GIVEN: inventory manually slowed to 20 ms, then a Warehouse Overload (×10 = 50 ms)
	h := newHarness(t)
	if err := h.e.SetKnobs(ctx, contracts.ServiceInventory, api.Values{"processingLatencyMs": 20.0}); err != nil {
		t.Fatal(err)
	}
	h.start(api.StartRequest{Scenario: "warehouse-overload", AutoRecover: true})
	during := h.last(contracts.ServiceInventory).Knobs.ProcessingLatencyMs

	// WHEN: the overload ends
	h.advance(120 * time.Second)

	// THEN: inventory is back at the manual 20 ms (recomputed, not "restored"), without expiry
	after := h.last(contracts.ServiceInventory)
	if during != 50 || after.Knobs.ProcessingLatencyMs != 20 || after.ExpiresAt != nil {
		t.Fatalf("during %d after %+v", during, after)
	}
}

func Test_shouldCombineConcurrentExperimentsWhenTheyTouchTheSameService(t *testing.T) {
	// GIVEN: Black Friday slows payment to 150 ms, a database slowdown adds 80 ms per transaction
	h := newHarness(t)
	h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true, DurationSec: 60})
	db := h.start(api.StartRequest{Scenario: "database-slowdown", AutoRecover: true, DurationSec: 300, Params: api.Values{"service": "payment"}})
	both := h.last(contracts.ServicePayment)

	// WHEN: Black Friday ends first
	h.advance(60 * time.Second)

	// THEN: payment had both faults and keeps only the database slowdown, expiring after its end
	after := h.last(contracts.ServicePayment)
	if both.Knobs.ProcessingLatencyMs != 150 || both.Knobs.DbLatencyMs != 80 || len(both.Experiments) != 2 {
		t.Fatalf("both %+v", both)
	}
	if after.Knobs.ProcessingLatencyMs != 0 || after.Knobs.DbLatencyMs != 80 || after.Experiments[0] != db.ID || !after.ExpiresAt.Equal(db.EndsAt.Add(30*time.Second)) {
		t.Fatalf("after %+v", after)
	}
}

func Test_shouldRefuseADestructiveScenarioWhenChaosIsDisarmedOrNotConfirmed(t *testing.T) {
	// GIVEN: a freshly started board (chaos disarmed)
	h := newHarness(t)
	req := api.StartRequest{Scenario: "network-chaos", AutoRecover: true}

	// WHEN: Network Chaos is started disarmed, then armed without and with the typed confirmation
	_, disarmed := h.e.Start(ctx, req)
	_ = h.e.Arm(ctx, true)
	_, unconfirmed := h.e.Start(ctx, req)
	req.Confirm = "network-chaos"
	x, err := h.e.Start(ctx, req)

	// THEN: only the armed and confirmed request starts it
	if !errors.Is(disarmed, api.ErrChaosDisarmed) || !errors.Is(unconfirmed, api.ErrConfirmationRequired) || err != nil || x.Status != api.StatusRunning {
		t.Fatalf("%v / %v / %v", disarmed, unconfirmed, err)
	}
	if k := h.last(contracts.ServicePayment).Knobs; k.NetLatencyMs != 200 || k.NetDropProbability != 0.1 {
		t.Fatalf("knobs %+v", k)
	}
}

func Test_shouldAllowOnlyOneBrokerFaultWhenAnotherIsStillActive(t *testing.T) {
	// GIVEN: armed chaos and a broker crash in progress
	h := newHarness(t)
	_ = h.e.Arm(ctx, true)
	h.start(api.StartRequest{Scenario: "broker-crash", AutoRecover: true, Confirm: "broker-crash"})

	// WHEN: a broker partition is requested as well
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "broker-outage", AutoRecover: true, Confirm: "broker-outage"})

	// THEN: it is refused with the broker-fault lock
	if !errors.Is(err, api.ErrBrokerFaultActive) || !strings.Contains(err.Error(), "Broker Crash") {
		t.Fatalf("err %v", err)
	}
}

func Test_shouldStartAScheduledExperimentWhenItsStartDelayHasPassed(t *testing.T) {
	// GIVEN: a Flash Sale scheduled to start in 30 s
	h := newHarness(t)
	before := h.snapCount()
	x := h.start(api.StartRequest{Scenario: "flash-sale", AutoRecover: true, StartDelaySec: 30})

	// WHEN: 10 s pass, then another 20 s
	h.advance(10 * time.Second)
	early := h.experiment(x.ID).Status
	h.advance(20 * time.Second)

	// THEN: nothing is published while scheduled; afterwards the popular SKU is pushed
	g := h.last(contracts.ServiceOrderGenerator)
	if x.Status != api.StatusScheduled || early != api.StatusScheduled || h.experiment(x.ID).Status != api.StatusRunning {
		t.Fatalf("statuses %s %s %s", x.Status, early, h.experiment(x.ID).Status)
	}
	if h.snapCount() != before+1 || *g.Knobs.PopularSku != "sku-001" || g.Knobs.PopularSkuProbability != 0.7 || *g.Knobs.OrdersPerSecond != 200 {
		t.Fatalf("snapshots %d→%d, knobs %+v", before, h.snapCount(), g.Knobs)
	}
	if !contains(h.auditActions(), "experiment.scheduled") {
		t.Fatalf("audit %v", h.auditActions())
	}
}

func Test_shouldKeepTheFaultAfterItsEndWhenAutoRecoverIsOff(t *testing.T) {
	// GIVEN: a Notification Meltdown without auto-recover, 120 s planned
	h := newHarness(t, func(c *Config) { c.SafetyTimeout = 10 * time.Minute })
	x := h.start(api.StartRequest{Scenario: "notification-meltdown", AutoRecover: false})
	snap := h.last(contracts.ServiceNotification)

	// WHEN: the planned end passes
	h.advance(5 * time.Minute)

	// THEN: it keeps running (expiry = safety timeout + 30 s) until the safety timeout aborts it
	if h.experiment(x.ID).Status != api.StatusRunning || snap.Knobs.ProcessingLatencyMs != 1000 ||
		!snap.ExpiresAt.Equal(x.StartsAt.Add(10*time.Minute+30*time.Second)) {
		t.Fatalf("experiment %+v snapshot %+v", h.experiment(x.ID), snap)
	}
	h.advance(5 * time.Minute)
	h.appliedAll()
	h.advance(10 * time.Second)
	if got := h.experiment(x.ID); got.Status != api.StatusAborted || h.last(contracts.ServiceNotification).Knobs.ProcessingLatencyMs != 0 {
		t.Fatalf("experiment %+v", got)
	}
}

func Test_shouldAbortAndRevertImmediatelyWhenTheUserStopsAnExperiment(t *testing.T) {
	// GIVEN: a running Silent Failure on shipping and a scheduled Poison Message
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "silent-failure", AutoRecover: true})
	later := h.start(api.StartRequest{Scenario: "poison-message", AutoRecover: true, StartDelaySec: 60})

	// WHEN: both are stopped, and the first one is stopped a second time
	stopped, err := h.e.Stop(ctx, x.ID)
	unscheduled, err2 := h.e.Stop(ctx, later.ID)
	again, err3 := h.e.Stop(ctx, x.ID)
	_, missing := h.e.Stop(ctx, "exp-unknown")

	// THEN: both are aborted, shipping resumes, a second stop changes nothing, unknown ids are not found
	if err != nil || err2 != nil || err3 != nil || stopped.Status != api.StatusAborted || unscheduled.Status != api.StatusAborted || again.EndedAt == nil || !again.EndedAt.Equal(*stopped.EndedAt) {
		t.Fatalf("%+v %+v %+v: %v %v %v", stopped, unscheduled, again, err, err2, err3)
	}
	if h.last(contracts.ServiceShipping).Knobs.Paused || !errors.Is(missing, api.ErrNotFound) {
		t.Fatalf("shipping still paused or %v", missing)
	}
	if !strings.Contains(h.exposition(), `lab_experiments_total{scenario="silent-failure",outcome="aborted"} 1`) {
		t.Fatalf("metrics %s", h.exposition())
	}
}

func Test_shouldFinishARecoveringExperimentWhenItIsStopped(t *testing.T) {
	// GIVEN: a Duplicate Delivery that ended and is recovering
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true})
	h.advance(90 * time.Second)

	// WHEN: the user stops it during recovery
	got, err := h.e.Stop(ctx, x.ID)

	// THEN: it ends with the outcome decided at its end (completed)
	if err != nil || got.Status != api.StatusCompleted {
		t.Fatalf("%+v %v", got, err)
	}
}

func Test_shouldRejectInvalidRequestsWhenStartingExperiments(t *testing.T) {
	// GIVEN: an armed board
	h := newHarness(t)
	_ = h.e.Arm(ctx, true)
	cases := map[string]api.StartRequest{
		"unknown scenario":    {Scenario: "meteor-strike"},
		"too short":           {Scenario: "black-friday", DurationSec: 5},
		"too long":            {Scenario: "black-friday", DurationSec: 7200},
		"negative delay":      {Scenario: "black-friday", StartDelaySec: -1},
		"unknown param":       {Scenario: "black-friday", Params: api.Values{"speed": 3.0}},
		"out of range":        {Scenario: "black-friday", Params: api.Values{"ordersPerSecond": 99999.0}},
		"not a number":        {Scenario: "black-friday", Params: api.Values{"ordersPerSecond": "fast"}},
		"NaN":                 {Scenario: "black-friday", Params: api.Values{"ordersPerSecond": math.NaN()}},
		"bad option":          {Scenario: "silent-failure", Params: api.Values{"service": "order-generator"}},
		"bad boolean":         {Scenario: "black-friday", Params: api.Values{"randomize": "yes"}},
		"half a broker":       {Scenario: "broker-crash", Confirm: "broker-crash", Params: api.Values{"broker": 1.5}},
		"broker out of range": {Scenario: "broker-crash", Confirm: "broker-crash", Params: api.Values{"broker": 3.0}},
	}
	for name, req := range cases {
		// WHEN: the request is made
		_, err := h.e.Start(ctx, req)

		// THEN: it is refused with the matching error
		want := api.ErrInvalid
		if name == "unknown scenario" {
			want = api.ErrUnknownScenario
		}
		if !errors.Is(err, want) {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

func Test_shouldVaryTheIntensityWithinBoundsWhenRandomisationIsRequested(t *testing.T) {
	// GIVEN: a random source at the upper end (+25 %)
	h := newHarness(t, func(c *Config) { c.Rand = func() float64 { return 0.99 } })

	// WHEN: Black Friday starts randomised with 4500 orders/s
	x := h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true, Params: api.Values{"ordersPerSecond": 4500.0, "randomize": true}})

	// THEN: the rate is raised but clamped to the 5000 maximum, latencies are rounded to their step
	if x.Params["ordersPerSecond"] != 5000.0 || x.Params["paymentLatencyMs"] != 190.0 {
		t.Fatalf("params %v", x.Params)
	}
	if *h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond != 5000 {
		t.Fatal("generator not at the randomised rate")
	}
}

func Test_shouldRampTheOrderRateStepByStepWhenATrafficTsunamiRuns(t *testing.T) {
	// GIVEN: a 100 s Traffic Tsunami up to 1050 orders/s (10 s steps)
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "traffic-tsunami", AutoRecover: true, DurationSec: 100, Params: api.Values{"peakOrdersPerSecond": 1050.0}})
	first := *h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond

	// WHEN: 5 s, then 10 s, then 50 s pass
	h.advance(5 * time.Second)
	n := h.snapCount()
	h.advance(5 * time.Second)
	second := *h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond
	h.advance(40 * time.Second)
	half := *h.last(contracts.ServiceOrderGenerator).Knobs.OrdersPerSecond

	// THEN: the rate starts at 50, climbs 100 per step and is republished only on step changes
	if first != 50 || second != 150 || half != 550 || h.snapCount() != n+2 {
		t.Fatalf("rates %v %v %v, snapshots %d→%d", first, second, half, n, h.snapCount())
	}
	if !strings.Contains(h.exposition(), `lab_experiment_intensity{experiment_id="`+x.ID+`"} 0.11`) {
		t.Fatalf("intensity %s", h.exposition())
	}
}

func Test_shouldRevertUnfinishedExperimentsWhenTheControlboardRestarts(t *testing.T) {
	// GIVEN: armed chaos, a running Regional Outage (region-a scaled to 0) and a scheduled Fat Messages
	h := newHarness(t)
	_ = h.e.Arm(ctx, true)
	outage := h.start(api.StartRequest{Scenario: "regional-outage", AutoRecover: true, Confirm: "regional-outage"})
	fat := h.start(api.StartRequest{Scenario: "fat-messages", AutoRecover: true, StartDelaySec: 600})
	scaledDown := h.kube.Replicas("commerce", "payment")
	lastGen := h.last(contracts.ServicePayment).Generation

	// WHEN: the controlboard restarts (generations continue after what is on the topic)
	h.cfg.Generations = func(context.Context) (int64, error) { return lastGen, nil }
	h.restart()

	// THEN: the outage is reverted from its persisted data and aborted, the scheduled one too,
	// chaos stays armed and every service has a newer baseline snapshot
	got := h.experiment(outage.ID)
	if scaledDown != 0 || h.kube.Replicas("commerce", "payment") != 1 || h.kube.Replicas("commerce", "inventory") != 1 {
		t.Fatalf("replicas %d → %d", scaledDown, h.kube.Replicas("commerce", "payment"))
	}
	if got.Status != api.StatusAborted || !strings.Contains(h.e.runs[outage.ID].rec.Reason, "restarted") || h.experiment(fat.ID).Status != api.StatusAborted {
		t.Fatalf("outage %+v fat %+v", got, h.experiment(fat.ID))
	}
	if _, ok := h.kube.DeploymentAnnotations("commerce", "payment")[revertKey]; ok || !h.e.Armed() || h.last(contracts.ServicePayment).Generation <= lastGen {
		t.Fatal("annotation left, chaos disarmed or generation not continued")
	}
}

func Test_shouldListActiveExperimentsAndTheLastTwentyEndedOnesWhenManyRan(t *testing.T) {
	// GIVEN: 22 Duplicate Delivery experiments that were stopped, and one running Poison Message
	h := newHarness(t)
	var first string
	for i := range 22 {
		x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true})
		if i == 0 {
			first = x.ID
		}
		_, _ = h.e.Stop(ctx, x.ID)
		h.clock.add(time.Second)
	}
	running := h.start(api.StartRequest{Scenario: "poison-message", AutoRecover: true})

	// WHEN: the experiments are listed, also after a restart
	list := h.e.List()
	h.restart()
	reloaded := h.e.List()

	// THEN: 21 entries, newest first, the oldest dropped; after the restart the running one was
	// reverted, so 20 ended ones remain
	if len(list) != 21 || list[0].ID != running.ID || list[20].ID == first || len(reloaded) != 20 || reloaded[0].Status != api.StatusAborted {
		t.Fatalf("%d listed (first %s), %d after restart", len(list), list[0].ID, len(reloaded))
	}
	if _, err := h.e.Stop(ctx, first); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("oldest still known: %v", err)
	}
}

func Test_shouldFailAndUndoTheStepsWhenAKubernetesActionCannotBeApplied(t *testing.T) {
	// GIVEN: armed chaos and an API server that refuses to scale notification
	h := newHarness(t)
	_ = h.e.Arm(ctx, true)
	h.kube.Fail = func(r *http.Request) int {
		if r.Method == http.MethodPatch && strings.HasSuffix(r.URL.Path, "/deployments/notification/scale") {
			return http.StatusForbidden
		}
		return 0
	}

	// WHEN: region-b (shipping, notification) goes down
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "regional-outage", AutoRecover: true, Confirm: "regional-outage",
		Params: api.Values{"region": "region-b (shipping, notification)"}})

	// THEN: the start fails, the experiment is recorded as failed, shipping is scaled back and
	// unannotated; notification keeps its revert annotation so a panic can still repair it
	list := h.e.List()
	if !errors.Is(err, api.ErrUnavailable) || list[0].Status != api.StatusFailed || h.kube.Replicas("commerce", "shipping") != 1 {
		t.Fatalf("err %v list %+v replicas %d", err, list, h.kube.Replicas("commerce", "shipping"))
	}
	if _, ok := h.kube.DeploymentAnnotations("commerce", "shipping")[revertKey]; ok {
		t.Fatal("revert annotation left behind on shipping")
	}
	if _, ok := h.kube.DeploymentAnnotations("commerce", "notification")[revertKey]; !ok {
		t.Fatal("notification lost its revert annotation")
	}
}

func Test_shouldFailTheExperimentWhenItsSnapshotCannotBePublished(t *testing.T) {
	// GIVEN: Kafka is unreachable
	h := newHarness(t)
	h.pub.failSnap = errBoom

	// WHEN: Payment Provider Down is started
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "payment-provider-down", AutoRecover: true})

	// THEN: the board is told the dependency is unavailable and the experiment failed
	if !errors.Is(err, api.ErrUnavailable) || h.e.List()[0].Status != api.StatusFailed {
		t.Fatalf("err %v", err)
	}
	if !strings.Contains(h.exposition(), `outcome="failed"`) {
		t.Fatal("failure not counted")
	}
}

func Test_shouldRefuseKubernetesScenariosWhenTheBoardRunsOutsideTheCluster(t *testing.T) {
	// GIVEN: no Kubernetes API (local development)
	h := newHarness(t, func(c *Config) { c.Kube = nil })

	// WHEN: a Consumer Crash Loop is requested
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "consumer-crash-loop", AutoRecover: true})

	// THEN: it is unavailable
	if !errors.Is(err, api.ErrUnavailable) {
		t.Fatalf("err %v", err)
	}
}

func Test_shouldDeliverChangesToSubscribersWithoutEverBlockingWhenOneIsSlow(t *testing.T) {
	// GIVEN: a subscriber that never reads
	h := newHarness(t)
	_, cancel := h.e.Subscribe()

	// WHEN: far more changes happen than its buffer holds, and it unsubscribes twice
	for range 150 {
		_ = h.e.SetKnobs(ctx, contracts.ServicePayment, api.Values{"processingJitterMs": 1.0})
		_ = h.e.ResetKnobs(ctx, contracts.ServicePayment)
	}
	cancel()
	cancel()

	// THEN: the engine kept going and the reading subscriber saw the timeline entries
	if n := len(h.timeline()); n < 300 {
		t.Fatalf("only %d timeline entries", n)
	}
}

func Test_shouldRunTheLoopInTheBackgroundWhenATickIntervalIsConfigured(t *testing.T) {
	// GIVEN: an engine ticking every 5 ms with a 10 s Duplicate Delivery
	h := newHarness(t, func(c *Config) { c.TickEvery = 5 * time.Millisecond })
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true, DurationSec: 10})

	// WHEN: the (fake) clock passes the end and the loop gets time to tick
	h.clock.add(11 * time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for h.experiment(x.ID).Status == api.StatusRunning && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// THEN: the loop reverted it on its own; closing twice is harmless
	if h.experiment(x.ID).Status == api.StatusRunning {
		t.Fatal("loop did not tick")
	}
	h.e.Close()
	h.e.Close()
}

func Test_shouldRequireItsCollaboratorsWhenTheEngineIsCreated(t *testing.T) {
	// GIVEN: a configuration without database, publisher and observer
	cfg := Config{}

	// WHEN: the engine is created
	_, err := New(cfg)

	// THEN: it refuses to start
	if err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldNotStartWhenTheGenerationsCannotBeRead(t *testing.T) {
	// GIVEN: platform.control cannot be read
	h := newHarness(t)
	h.cfg.Generations = func(context.Context) (int64, error) { return 0, errBoom }
	h.cfg.Metrics = nil

	// WHEN: a new engine is created
	_, err := New(h.cfg)

	// THEN: it refuses to start (it could reuse generations otherwise)
	if !errors.Is(err, errBoom) {
		t.Fatalf("err %v", err)
	}
}
