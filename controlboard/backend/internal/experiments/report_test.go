package experiments

import (
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/contracts"
)

func Test_shouldReportDetectionLagAndRevenueWhenAPaymentOutageIsObserved(t *testing.T) {
	// GIVEN: 60 EUR/min revenue, 100 records lag and 50 orders in flight before the outage
	h := newHarness(t)
	h.obs.set(func(o *fakeObserver) {
		o.kpis = api.Kpis{RevenuePerMin: 60, InFlight: 50, PaymentSuccessRate: 0.97}
		o.lag = 100
	})
	x := h.start(api.StartRequest{Scenario: "payment-provider-down", AutoRecover: true, DurationSec: 60})

	// WHEN: payment applies the snapshot, payments fail, revenue halves, an alert fires after 20 s,
	// a test alert and an older alert are present too, and the lag peaks at 9000
	h.appliedAll()
	started := h.clock.Now()
	h.obs.set(func(o *fakeObserver) {
		o.kpis = api.Kpis{RevenuePerMin: 30, InFlight: 250, PaymentSuccessRate: 0.1, DlqPerMin: 6}
		o.lag = 9000
		o.alerts = []api.Alert{
			{Name: "LabPaymentFailures", Status: api.AlertFiring, StartsAt: started.Add(20 * time.Second)},
			{Name: "LabPaymentFailures", Status: api.AlertFiring, StartsAt: started.Add(25 * time.Second)},
			{Name: "LabConsumerLag", Status: api.AlertFiring, StartsAt: started.Add(15 * time.Second).Add(10 * time.Second)},
			{Name: "LabTestAlert", Status: api.AlertFiring, StartsAt: started.Add(time.Second)},
			{Name: "LabOldAlert", Status: api.AlertFiring, StartsAt: started.Add(-time.Hour)},
			{Name: "LabPending", Status: api.AlertPending, StartsAt: started.Add(time.Second)},
		}
	})
	h.advance(60 * time.Second)

	// THEN: observations confirm the applied snapshot and the failing payments
	running := h.e.runs[x.ID].rec.Exp.Observations
	if len(running) != 2 || !running[0].Confirmed || !running[1].Confirmed || running[1].Observed != "10.0 % successful" {
		t.Fatalf("observations %+v", running)
	}
	// AND: the report shows time to detect 20 s, the real alerts, peak lag and the revenue loss
	h.obs.set(func(o *fakeObserver) { o.lag = 100; o.kpis = api.Kpis{RevenuePerMin: 60} })
	h.advance(10 * time.Second)
	r := h.experiment(x.ID).Report
	if r.TimeToDetectSec == nil || *r.TimeToDetectSec != 20 || r.PeakLag != 9000 || len(r.Alerts) != 2 {
		t.Fatalf("report %+v", r)
	}
	if math.Abs(r.RevenueImpactEur-(-30)) > 0.01 || math.Abs(r.OrdersAffected-(200+6)) > 0.01 {
		t.Fatalf("revenue %v orders %v", r.RevenueImpactEur, r.OrdersAffected)
	}
}

func Test_shouldShowAnUnconfirmedObservationWhenTheServiceHasNotAppliedTheSnapshotYet(t *testing.T) {
	// GIVEN: Black Friday where the generator reports an old generation and only 300 orders/s
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true})
	h.obs.set(func(o *fakeObserver) { o.kpis.OrdersPerSec = 300 })

	// WHEN: the observations are evaluated, twice within the observation interval
	h.advance(5 * time.Second)
	first := h.experiment(x.ID).Observations
	h.advance(time.Second)

	// THEN: "applied" and "order rate" are unconfirmed with what is observed right now
	if first[0].Confirmed || !strings.Contains(first[0].Observed, "generation 0 of") || first[3].Confirmed || first[3].Observed != "300 orders/s" {
		t.Fatalf("observations %+v", first)
	}
}

func Test_shouldSeeNoPodsWhenTheAffectedServiceIsScaledToZero(t *testing.T) {
	// GIVEN: notification has no running pods
	h := newHarness(t)
	h.obs.set(func(o *fakeObserver) {
		for i := range o.services {
			if o.services[i].Name == contracts.ServiceNotification {
				o.services[i].ReadyReplicas = 0
			}
		}
	})

	// WHEN: a meltdown runs and ends
	x := h.start(api.StartRequest{Scenario: "notification-meltdown", AutoRecover: true})
	h.advance(5 * time.Second)
	during := h.experiment(x.ID).Observations[0]
	h.advance(120 * time.Second)
	h.advance(5 * time.Second)

	// THEN: "applied" cannot be confirmed without pods, but recovery does not wait for them
	if during.Confirmed || during.Observed != "no running pods" || h.experiment(x.ID).Status != api.StatusCompleted {
		t.Fatalf("observation %+v status %s", during, h.experiment(x.ID).Status)
	}
}

func Test_shouldConfirmEachScenariosSymptomWhenTheSystemReactsAsExpected(t *testing.T) {
	// GIVEN: a system that shows every symptom: lag, stockouts, dead letters, broker down, URPs, scaled-down services
	h := armed(t)
	h.obs.set(func(o *fakeObserver) {
		o.kpis = api.Kpis{ReservationSuccessRate: 0.5, DlqPerMin: 3, OrdersPerSec: 2000}
		o.brokers[1].Up = false
		o.brokers[0].UnderReplicated = 3
		o.lag = 5000
	})
	cases := []api.StartRequest{
		{Scenario: "flash-sale", Params: api.Values{"ordersPerSecond": 0.0}},
		{Scenario: "poison-message"},
		{Scenario: "warehouse-overload"},
		{Scenario: "recovery-storm"},
		{Scenario: "silent-failure"},
		{Scenario: "black-friday", Params: api.Values{"paymentLatencyMs": 0.0, "inventoryLatencyMs": 0.0}},
	}
	for _, req := range cases {
		// WHEN: the scenario runs and is observed
		req.AutoRecover = true
		h.obs.set(func(o *fakeObserver) { o.lag = 0 })
		x := h.start(req)
		h.appliedAll()
		h.obs.set(func(o *fakeObserver) { o.lag = 5000 })
		h.advance(5 * time.Second)

		// THEN: every observation is confirmed
		for _, o := range h.experiment(x.ID).Observations {
			if !o.Confirmed {
				t.Fatalf("%s: %+v", req.Scenario, o)
			}
		}
		_, _ = h.e.Stop(ctx, x.ID)
	}
	for _, sc := range []string{"broker-outage", "kafka-broker-failure"} {
		x := h.start(api.StartRequest{Scenario: sc, AutoRecover: true, Confirm: sc})
		h.advance(5 * time.Second)
		if o := h.experiment(x.ID).Observations[0]; !o.Confirmed {
			t.Fatalf("%s: %+v", sc, o)
		}
		_, _ = h.e.Stop(ctx, x.ID)
	}
	// AND: a broker that is not reported at all counts as down
	h.obs.set(func(o *fakeObserver) { o.brokers = nil })
	x := h.start(api.StartRequest{Scenario: "kafka-broker-failure", AutoRecover: true, Confirm: "kafka-broker-failure"})
	h.advance(5 * time.Second)
	if !h.experiment(x.ID).Observations[0].Confirmed {
		t.Fatal("missing broker not seen as down")
	}
	h.obs.healthy(1 << 40)
	h.advance(5 * time.Second)
	if o := h.experiment(x.ID).Observations[0]; o.Confirmed || o.Observed != "up" {
		t.Fatalf("broker up but %+v", o)
	}
}

func Test_shouldCountEveryScenarioIntoTheCatalogueWhenTheBoardAsksForIt(t *testing.T) {
	// GIVEN: the engine, with pods that come back after deletion
	h := newHarness(t)
	recreate(h)

	// WHEN: the scenarios are listed
	defs := h.e.Scenarios()

	// THEN: all 22 scenarios have unique ids, dashboards with absolute links and a default per param
	seen := map[string]bool{}
	for _, d := range defs {
		if seen[d.ID] || len(d.Dashboards) == 0 || !strings.HasPrefix(d.Dashboards[0].URL, "http://grafana.localhost:8080/d/") || len(d.ExpectedSymptoms) == 0 {
			t.Fatalf("scenario %+v", d)
		}
		seen[d.ID] = true
		for _, p := range d.Params {
			if p.Default == nil {
				t.Fatalf("%s.%s has no default", d.ID, p.Key)
			}
		}
	}
	if len(defs) != 22 || !seen["kafka-broker-failure"] || !seen["broker-outage"] {
		t.Fatalf("%d scenarios", len(defs))
	}
	// AND: every scenario starts with its defaults on an armed board
	_ = h.e.Arm(ctx, true)
	for _, d := range defs {
		x, err := h.e.Start(ctx, api.StartRequest{Scenario: d.ID, AutoRecover: true, Confirm: d.ID})
		if err != nil && !errors.Is(err, api.ErrBrokerFaultActive) {
			t.Fatalf("%s: %v", d.ID, err)
		}
		if err == nil {
			_, _ = h.e.Stop(ctx, x.ID)
		}
	}
}

func Test_shouldLogAndContinueWhenAuditOrAnnotationsAreUnavailable(t *testing.T) {
	// GIVEN: the audit topic and Grafana are down
	h := newHarness(t)
	h.pub.failAudit = errBoom
	h.ann.fail = errBoom

	// WHEN: an experiment runs to its end
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true})
	h.advance(90 * time.Second)

	// THEN: the experiment works anyway, without audit events or annotations
	if h.experiment(x.ID).Status != api.StatusRecovering || len(h.pub.audits) != 0 || len(h.ann.starts) != 0 {
		t.Fatalf("status %s audits %d", h.experiment(x.ID).Status, len(h.pub.audits))
	}
}

func Test_shouldStillTrackTheExperimentWhenPersistingItFails(t *testing.T) {
	// GIVEN: a running experiment, then the database goes away
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true, StartDelaySec: 0})
	h.db.Close()

	// WHEN: it ends and is stopped, and another one is scheduled
	h.advance(90 * time.Second)
	_, _ = h.e.Stop(ctx, x.ID)
	_, err := h.e.Start(ctx, api.StartRequest{Scenario: "duplicate-delivery", AutoRecover: true, StartDelaySec: 30})

	// THEN: the ended one is known in memory, the scheduling is refused (it could not be persisted)
	if h.experiment(x.ID).Status != api.StatusCompleted || !errors.Is(err, api.ErrUnavailable) || len(h.e.List()) != 1 {
		t.Fatalf("status %s err %v", h.experiment(x.ID).Status, err)
	}
	// AND: a new engine on a dead database does not start
	if _, err := New(h.cfg); err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldReturnCopiesWhenTheBoardReadsExperimentsAndTuning(t *testing.T) {
	// GIVEN: a running Black Friday and some tuning
	h := newHarness(t)
	x := h.start(api.StartRequest{Scenario: "black-friday", AutoRecover: true})
	_, _ = h.e.SetTuning(ctx, contracts.ServicePayment, api.Values{"acks": "all"})

	// WHEN: the caller mutates what it got
	x.Params["ordersPerSecond"] = 1.0
	x.Affected[0] = "hacked"
	st := h.e.Tuning()
	st.Services[contracts.ServicePayment]["acks"] = "none"

	// THEN: the engine's state is untouched
	again := h.experiment(x.ID)
	if again.Params["ordersPerSecond"] != 1000.0 || again.Affected[0] == "hacked" || h.e.Tuning().Services[contracts.ServicePayment]["acks"] != "all" {
		t.Fatalf("leaked %+v", again)
	}
	b, _ := json.Marshal(again)
	if !strings.Contains(string(b), `"report"`) {
		t.Fatalf("running experiment has no live report: %s", b)
	}
}
