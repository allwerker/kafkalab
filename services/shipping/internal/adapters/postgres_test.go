package adapters

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/runner"
	"kafkalab/pkg/kit/testkit"
	"kafkalab/services/shipping/internal/app"
	"kafkalab/services/shipping/internal/domain"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func openDB(t *testing.T) *pg.DB {
	t.Helper()
	db, err := pg.Open(context.Background(), pg.Options{URL: testkit.PostgresDSN(t), MaxConns: 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background(), Migrations()); err != nil {
		t.Fatal(err)
	}
	return db
}

func lock(t *testing.T, db *pg.DB, id string, now time.Time) app.Order {
	t.Helper()
	var o app.Order
	if err := db.InTx(context.Background(), func(tx pgx.Tx) error {
		var err error
		o, err = Postgres{}.Lock(context.Background(), tx, id, now)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return o
}

func save(t *testing.T, db *pg.DB, o app.Order) {
	t.Helper()
	if err := db.InTx(context.Background(), func(tx pgx.Tx) error { return Postgres{}.Save(context.Background(), tx, o) }); err != nil {
		t.Fatal(err)
	}
}

type noKnobs struct{}

func (noKnobs) Knobs() events.Knobs { return events.Knobs{} }

func Test_shouldOpenAJoinWhenAnOrderIsSeenForTheFirstTime(t *testing.T) {
	// GIVEN: an empty join store
	db := openDB(t)

	// WHEN: the first half of an unknown order is locked
	got := lock(t, db, "o-new", t0)

	// THEN: the order is open, knows neither half and remembers when it was first seen
	if got.OrderID != "o-new" || got.Join != (domain.Join{Status: domain.StatusOpen}) || !got.FirstSeenAt.Equal(t0) ||
		!got.OrderCreatedAt.IsZero() {
		t.Fatalf("got %+v", got)
	}
}

func Test_shouldRememberEverythingAboutTheJoinWhenItIsSaved(t *testing.T) {
	// GIVEN: an order held back during an outage, with payment and reservation details
	db := openDB(t)
	o := lock(t, db, "o-1", t0)
	o.Join = domain.Join{Payment: domain.HalfOK, Stock: domain.HalfOK, Status: domain.StatusReady}
	o.PaymentID, o.ReservationID, o.Warehouse, o.CauseEventID = "pay-1", "res-1", "WH-EAST", "evt-res-1"
	o.OrderCreatedAt = t0.Add(-time.Second)

	// WHEN: it is saved and the second half of a later transaction locks it again
	save(t, db, o)
	got := lock(t, db, "o-1", t0.Add(time.Minute))

	// THEN: everything needed to ship it later is there and the first sighting is unchanged
	if got != o {
		t.Fatalf("got %+v, want %+v", got, o)
	}
}

func Test_shouldShipEachOrderExactlyOnceWhenTwoPodsReceiveTheHalvesAtTheSameTime(t *testing.T) {
	// GIVEN: 40 orders whose payment and reservation are handled by two pods concurrently
	db := openDB(t)
	ctx := context.Background()
	h := &app.Handler{Repo: Postgres{}, Control: noKnobs{}, Now: func() time.Time { return t0 }}
	var mu sync.Mutex
	shipped := map[string]int{}
	var wg sync.WaitGroup
	run := func(v any) {
		defer wg.Done()
		err := db.InTx(ctx, func(tx pgx.Tx) error {
			outs, err := h.Handle(ctx, tx, runner.Input{Value: v})
			mu.Lock()
			for _, o := range outs {
				shipped[o.Key]++
			}
			mu.Unlock()
			return err
		})
		if err != nil {
			t.Error(err)
		}
	}

	// WHEN: both halves of every order are applied at the same moment in separate transactions
	for i := range 40 {
		id := "o-" + strconv.Itoa(i)
		wg.Add(2)
		go run(&events.PaymentCompleted{Meta: events.EventMeta{EventID: "p-" + id, OrderID: id}, PaymentID: "pay"})
		go run(&events.InventoryReserved{Meta: events.EventMeta{EventID: "r-" + id, OrderID: id}, Warehouse: "WH-1"})
	}
	wg.Wait()

	// THEN: the row lock serialises the halves: every order ships exactly once (no lost update
	// where both transactions think the other half is still missing)
	for i := range 40 {
		if n := shipped["o-"+strconv.Itoa(i)]; n != 1 {
			t.Fatalf("order o-%d shipped %d times", i, n)
		}
	}
}

func Test_shouldHandOverdueOrdersToOnlyOneSweeperWhenTwoPodsSweepAtOnce(t *testing.T) {
	// GIVEN: three overdue open orders, one young open order and one cancelled order
	db := openDB(t)
	ctx := context.Background()
	for i, seen := range []time.Duration{-10 * time.Minute, -9 * time.Minute, -8 * time.Minute, -time.Minute, -20 * time.Minute} {
		o := lock(t, db, "o-"+strconv.Itoa(i), t0.Add(seen))
		if i == 4 {
			o.Join.Status = domain.StatusCancelled
			save(t, db, o)
		}
	}
	before := t0.Add(-5 * time.Minute)
	locked := make(chan struct{})
	release := make(chan struct{})
	var first []app.Order
	done := make(chan error)
	go func() {
		done <- db.InTx(ctx, func(tx pgx.Tx) error {
			var err error
			first, err = Postgres{}.Overdue(ctx, tx, before, 2)
			close(locked)
			<-release
			return err
		})
	}()
	<-locked

	// WHEN: a second pod sweeps while the first one still holds its batch of two
	var second []app.Order
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		second, err = Postgres{}.Overdue(ctx, tx, before, 10)
		return err
	})
	close(release)

	// THEN: the first gets the two oldest, the second skips them instead of waiting and gets
	// the remaining overdue one; young and terminal orders are never swept
	if err != nil || <-done != nil || len(first) != 2 || first[0].OrderID != "o-0" || first[1].OrderID != "o-1" ||
		len(second) != 1 || second[0].OrderID != "o-2" {
		t.Fatalf("first %+v second %+v err %v", first, second, err)
	}
}

func Test_shouldFindHeldBackOrdersAndCountWaitingOnesWhenTheSweeperAsks(t *testing.T) {
	// GIVEN: an open order, two held-back orders and a shipped order
	db := openDB(t)
	ctx := context.Background()
	for i, s := range []domain.Status{domain.StatusOpen, domain.StatusReady, domain.StatusReady, domain.StatusShipped} {
		o := lock(t, db, "o-"+strconv.Itoa(i), t0.Add(time.Duration(i)*time.Second))
		o.Join.Status = s
		save(t, db, o)
	}

	// WHEN: the sweeper looks for held-back orders and counts the waiting ones
	var ready []app.Order
	var waiting int64
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		var err error
		if ready, err = (Postgres{}).Ready(ctx, tx, 10); err != nil {
			return err
		}
		waiting, err = Postgres{}.Waiting(ctx, tx)
		return err
	})

	// THEN: both held-back orders are found, oldest first, and three orders are not terminal
	if err != nil || len(ready) != 2 || ready[0].OrderID != "o-1" || ready[1].OrderID != "o-2" || waiting != 3 {
		t.Fatalf("ready %+v waiting %d err %v", ready, waiting, err)
	}
}

func Test_shouldDeleteOnlyOldTerminalOrdersWhenPurging(t *testing.T) {
	// GIVEN: a shipped, a cancelled and an open order
	db := openDB(t)
	ctx := context.Background()
	for i, s := range []domain.Status{domain.StatusShipped, domain.StatusCancelled, domain.StatusOpen} {
		o := lock(t, db, "o-"+strconv.Itoa(i), t0)
		o.Join.Status = s
		save(t, db, o)
	}

	// WHEN: everything last changed before a minute from now is purged, one row per transaction
	var deleted []int64
	for range 3 {
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			n, err := Postgres{}.Purge(ctx, tx, time.Now().Add(time.Minute), 1)
			deleted = append(deleted, n)
			return err
		})
	}

	// THEN: both terminal rows are deleted, the open order stays
	var left int64
	_ = db.InTx(ctx, func(tx pgx.Tx) error { var err error; left, err = Postgres{}.Waiting(ctx, tx); return err })
	if deleted[0] != 1 || deleted[1] != 1 || deleted[2] != 0 || left != 1 {
		t.Fatalf("deleted %v left %d", deleted, left)
	}
}
