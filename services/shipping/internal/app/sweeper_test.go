package app

import (
	"context"
	"strconv"
	"testing"
	"time"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/services/shipping/internal/domain"
)

func Test_shouldCancelAnOrderWhenItsSecondHalfMissesTheDeadline(t *testing.T) {
	// GIVEN: an order whose payment arrived 301 s ago while inventory never answered
	w := newWorld()
	w.handle(t, paid("o-1"))
	w.now = t0.Add(301 * time.Second)

	// WHEN: the sweeper runs
	w.sweeper.Tick(context.Background())

	// THEN: orders.cancelled DEADLINE_EXCEEDED asks for a refund, names the deadline as cause,
	// and the deadline counter goes up
	c := w.pub.published[0].Value.(*events.OrderCancelled)
	if len(w.pub.published) != 1 || c.Reason != events.CancellationReasonDeadlineExceeded || !c.RefundRequired ||
		c.ReleaseRequired || c.Meta.CausationID != "deadline:o-1" || c.Meta.EventID != contracts.DeriveEventID("o-1", "cancelled") ||
		!c.Meta.OrderCreatedAt.Equal(t0.Add(-2*time.Second)) {
		t.Fatalf("unexpected %+v", w.pub.published)
	}
	if w.repo.rows["o-1"].Join.Status != domain.StatusCancelled || w.counter("lab_join_deadline_exceeded_total") != 1 {
		t.Fatalf("row %+v, counter %d", w.repo.rows["o-1"], w.counter("lab_join_deadline_exceeded_total"))
	}
}

func Test_shouldLeaveOrdersAloneWhenTheyAreStillWithinTheDeadline(t *testing.T) {
	// GIVEN: an order first seen 299 s ago
	w := newWorld()
	w.handle(t, reserved("o-2"))
	w.now = t0.Add(299 * time.Second)

	// WHEN: the sweeper runs
	w.sweeper.Tick(context.Background())

	// THEN: it keeps waiting for its payment
	if len(w.pub.published) != 0 || w.repo.rows["o-2"].Join.Status != domain.StatusOpen {
		t.Fatalf("published %+v", w.pub.published)
	}
}

func Test_shouldShipHeldBackOrdersWhenTheWarehouseOutageIsOver(t *testing.T) {
	// GIVEN: an order completed during a warehouse outage
	w := newWorld()
	w.knobs.WarehouseOutage = true
	w.handle(t, paid("o-3"))
	res := reserved("o-3")
	w.handle(t, res)

	// WHEN: the sweeper runs during the outage, then after it ended (even past the deadline)
	w.sweeper.Tick(context.Background())
	during := len(w.pub.published)
	w.knobs.WarehouseOutage = false
	w.now = t0.Add(time.Hour)
	w.sweeper.Tick(context.Background())

	// THEN: it ships only after the outage, caused by the reservation that completed it
	s := w.pub.published[0].Value.(*events.OrderShipped)
	if during != 0 || len(w.pub.published) != 1 || s.Meta.CausationID != res.Meta.EventID || s.Warehouse != "WH-NORTH" ||
		w.repo.rows["o-3"].Join.Status != domain.StatusShipped || w.counter("lab_join_deadline_exceeded_total") != 0 {
		t.Fatalf("during=%d published=%+v", during, w.pub.published)
	}
}

func Test_shouldKeepOrdersOpenAndRetryWhenTheCancellationCannotBePublished(t *testing.T) {
	// GIVEN: an overdue order and a Kafka cluster that rejects the produce
	w := newWorld()
	w.handle(t, paid("o-4"))
	w.now = t0.Add(time.Hour)
	w.pub.failing = errDown

	// WHEN: the sweeper runs, then runs again after Kafka recovered
	w.sweeper.Tick(context.Background())
	afterFailure := w.repo.rows["o-4"].Join.Status
	w.pub.failing = nil
	w.sweeper.Tick(context.Background())

	// THEN: the failed sweep changed nothing (no decision without an event); the retry cancels it
	if afterFailure != domain.StatusOpen || len(w.pub.published) != 1 ||
		w.repo.rows["o-4"].Join.Status != domain.StatusCancelled || w.counter("lab_join_deadline_exceeded_total") != 1 {
		t.Fatalf("after failure %s, published %+v", afterFailure, w.pub.published)
	}
}

func Test_shouldWorkThroughABacklogWhenMoreOrdersAreOverdueThanOneBatchHolds(t *testing.T) {
	// GIVEN: 25 overdue orders and a batch size of 10
	w := newWorld()
	w.sweeper.Batch = 10
	for i := range 25 {
		w.handle(t, paid("o-"+strconv.Itoa(i)))
	}
	w.now = t0.Add(time.Hour)

	// WHEN: the sweeper ticks once
	w.sweeper.Tick(context.Background())

	// THEN: all of them are cancelled
	if len(w.pub.published) != 25 || w.counter("lab_join_deadline_exceeded_total") != 25 {
		t.Fatalf("published %d", len(w.pub.published))
	}
}

func Test_shouldStopSweepingWhenTheStoreFails(t *testing.T) {
	// GIVEN: overdue and held-back orders
	w := newWorld()
	w.handle(t, paid("o-overdue"))
	w.knobs.WarehouseOutage = true
	w.handle(t, paid("o-ready"))
	w.handle(t, reserved("o-ready"))
	w.knobs.WarehouseOutage = false
	w.now = t0.Add(time.Hour)
	failures := []func(){
		func() { w.repo.failOverdue = errDown },
		func() { w.repo.failReady = errDown },
		func() { w.repo.failSave = errDown },
	}
	for _, fail := range failures {
		w.repo.failOverdue, w.repo.failReady, w.repo.failSave = nil, nil, nil
		fail()

		// WHEN: the sweeper runs while one store operation fails
		w.sweeper.Tick(context.Background())

		// THEN: nothing is published or decided; the next tick tries again
		if len(w.pub.published) != 0 || w.repo.rows["o-overdue"].Join.Status != domain.StatusOpen ||
			w.repo.rows["o-ready"].Join.Status != domain.StatusReady {
			t.Fatalf("published %+v", w.pub.published)
		}
	}
}

func Test_shouldExportTheOpenJoinsWhenThisPodIsTheGaugeLeader(t *testing.T) {
	// GIVEN: two open orders, one held-back order and one shipped order
	w := newWorld()
	w.handle(t, paid("o-a"))
	w.handle(t, reserved("o-b"))
	w.knobs.WarehouseOutage = true
	w.handle(t, paid("o-c"))
	w.handle(t, reserved("o-c"))
	w.knobs.WarehouseOutage = false
	w.handle(t, paid("o-d"))
	w.handle(t, reserved("o-d"))

	// WHEN: the leader pod and a follower pod refresh the gauge
	w.sweeper.Tick(context.Background())
	leaderValue := w.gauge("lab_join_open_orders")
	follower := newWorld()
	follower.repo, follower.sweeper.Repo, follower.sweeper.DB = w.repo, w.repo, memDB{w.repo}
	follower.sweeper.Leader = leader(false)
	follower.sweeper.Tick(context.Background())

	// THEN: the leader reports the two orders still waiting for a half (the held-back one was
	// shipped by the same sweep), the follower reports 0, so the sum over pods is right
	if leaderValue != 2 || follower.gauge("lab_join_open_orders") != 0 {
		t.Fatalf("leader %v follower %v", leaderValue, follower.gauge("lab_join_open_orders"))
	}
}

func Test_shouldKeepTheLastGaugeValueWhenCountingFails(t *testing.T) {
	// GIVEN: a leader that exported one open order
	w := newWorld()
	w.handle(t, paid("o-1"))
	w.sweeper.Tick(context.Background())

	// WHEN: counting fails on the next tick
	w.repo.failWaiting = errDown
	w.handle(t, paid("o-2"))
	w.sweeper.Tick(context.Background())

	// THEN: the gauge keeps the last known value instead of dropping to 0
	if w.gauge("lab_join_open_orders") != 1 {
		t.Fatalf("got %v", w.gauge("lab_join_open_orders"))
	}
}

func Test_shouldForgetTerminalOrdersWhenTheirRetentionHasPassed(t *testing.T) {
	// GIVEN: 5 orders cancelled long ago, one cancelled just now and one still open
	w := newWorld()
	w.sweeper.Batch = 2
	for i := range 5 {
		w.handle(t, rejected("old-"+strconv.Itoa(i)))
	}
	w.now = t0.Add(3 * time.Hour)
	w.handle(t, rejected("recent"))
	w.handle(t, paid("open"))

	// WHEN: the sweeper runs (retention 2 h)
	w.sweeper.Tick(context.Background())

	// THEN: only the old terminal rows are gone
	if len(w.repo.rows) != 2 || w.repo.rows["recent"].OrderID == "" || w.repo.rows["open"].OrderID == "" {
		t.Fatalf("rows %v", w.repo.rows)
	}
}

func Test_shouldCarryOnWhenPurgingFails(t *testing.T) {
	// GIVEN: an old cancelled order and a failing delete
	w := newWorld()
	w.handle(t, rejected("old"))
	w.now = t0.Add(3 * time.Hour)
	w.repo.failPurge = errDown

	// WHEN: the sweeper runs
	w.sweeper.Tick(context.Background())

	// THEN: the row stays for a later attempt
	if len(w.repo.rows) != 1 {
		t.Fatalf("rows %v", w.repo.rows)
	}
}

func Test_shouldSweepPeriodicallyWhenRunningUntilTheServiceStops(t *testing.T) {
	// GIVEN: an overdue order and a sweeper ticking every millisecond
	w := newWorld()
	w.handle(t, paid("o-1"))
	w.now = t0.Add(time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- w.sweeper.Run(ctx) }()

	// WHEN: the order has been cancelled and the service shuts down
	for w.counter("lab_join_deadline_exceeded_total") == 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	// THEN: the sweeper returns cleanly
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
