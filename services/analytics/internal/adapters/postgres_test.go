package adapters

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
	"kafkalab/services/analytics/internal/domain"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func openDB(t *testing.T) *pg.DB {
	t.Helper()
	db, err := pg.Open(context.Background(), pg.Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(context.Background(), Migrations()); err != nil {
		t.Fatal(err)
	}
	return db
}

func upsert(t *testing.T, db *pg.DB, seed domain.Order, forget time.Time) (domain.Order, bool) {
	t.Helper()
	var o domain.Order
	var fresh bool
	if err := db.InTx(context.Background(), func(tx pgx.Tx) error {
		var err error
		o, fresh, err = Postgres{}.Upsert(context.Background(), tx, seed, forget)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return o, fresh
}

func Test_shouldStoreANewOrderInOneStepWhenItIsCreated(t *testing.T) {
	// GIVEN: an empty orders table
	db := openDB(t)
	seed := domain.Order{OrderID: "o-1", Created: true, TotalCents: 4990, Region: "NORTH"}

	// WHEN: the order is upserted, then upserted again (a duplicate)
	first, firstFresh := upsert(t, db, seed, t0)
	again, againFresh := upsert(t, db, domain.Order{OrderID: "o-1", Outcome: domain.OutcomeShipped}, t0)

	// THEN: the first insert is fresh; the second returns the stored order unchanged
	if !firstFresh || first != seed || againFresh || again != seed {
		t.Fatalf("first %+v %v, again %+v %v", first, firstFresh, again, againFresh)
	}
}

func Test_shouldRememberTheOutcomeAndValueWhenAnOrderIsSaved(t *testing.T) {
	// GIVEN: an order known only from its shipment
	db := openDB(t)
	upsert(t, db, domain.Order{OrderID: "o-2", Outcome: domain.OutcomeShipped}, t0)
	full := domain.Order{OrderID: "o-2", Created: true, TotalCents: 1200, Region: "WEST", Outcome: domain.OutcomeShipped}

	// WHEN: its value arrives and is saved
	err := db.InTx(context.Background(), func(tx pgx.Tx) error { return Postgres{}.Save(context.Background(), tx, full, t0) })
	got, _ := upsert(t, db, domain.Order{OrderID: "o-2"}, t0)

	// THEN: a later event sees the complete order
	if err != nil || got != full {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func Test_shouldDeleteOnlyExpiredOrdersWhenPurging(t *testing.T) {
	// GIVEN: two expired orders and one that is still kept
	db := openDB(t)
	ctx := context.Background()
	upsert(t, db, domain.Order{OrderID: "old-1"}, t0.Add(-time.Minute))
	upsert(t, db, domain.Order{OrderID: "old-2"}, t0.Add(-time.Second))
	upsert(t, db, domain.Order{OrderID: "kept"}, t0.Add(time.Minute))

	// WHEN: rows expired by now are purged, at most one per transaction
	var deleted []int64
	for range 3 {
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			n, err := Postgres{}.Purge(ctx, tx, t0, 1)
			deleted = append(deleted, n)
			return err
		})
	}

	// THEN: both expired rows are gone, the kept one is still there
	_, fresh := upsert(t, db, domain.Order{OrderID: "kept"}, t0)
	if deleted[0] != 1 || deleted[1] != 1 || deleted[2] != 0 || fresh {
		t.Fatalf("deleted %v, kept row missing=%v", deleted, fresh)
	}
}

func Test_shouldSerialiseTheOrderAndItsOutcomeWhenTwoPodsSeeThemAtOnce(t *testing.T) {
	// GIVEN: the creation and the shipment of one order, handled in two concurrent transactions
	db := openDB(t)
	ctx := context.Background()
	firstLocked, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	wg.Add(2)

	// WHEN: the first holds the row while the second upserts it
	go func() {
		defer wg.Done()
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			_, _, err := Postgres{}.Upsert(ctx, tx, domain.Order{OrderID: "o-race", Created: true, TotalCents: 100}, t0)
			close(firstLocked)
			<-release
			mu.Lock()
			order = append(order, "created")
			mu.Unlock()
			return err
		})
	}()
	<-firstLocked
	var second domain.Order
	var secondFresh bool
	go func() {
		defer wg.Done()
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			var err error
			second, secondFresh, err = Postgres{}.Upsert(ctx, tx, domain.Order{OrderID: "o-race", Outcome: domain.OutcomeShipped}, t0)
			mu.Lock()
			order = append(order, "shipped")
			mu.Unlock()
			return err
		})
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()

	// THEN: the shipment waited and then saw the created order, so its revenue can be counted
	if len(order) != 2 || order[0] != "created" || secondFresh || !second.Created || second.TotalCents != 100 {
		t.Fatalf("order %v, second %+v fresh=%v", order, second, secondFresh)
	}
}
