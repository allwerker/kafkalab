package adapters

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
	"kafkalab/services/payment/internal/app"
	"kafkalab/services/payment/internal/domain"
)

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

func Test_shouldStartWithAnEmptyStateWhenAnOrderIsSeenForTheFirstTime(t *testing.T) {
	// GIVEN: an empty payment store
	db := openDB(t)
	var got app.Payment

	// WHEN: an unknown order is locked
	err := db.InTx(context.Background(), func(tx pgx.Tx) error {
		var err error
		got, err = Postgres{}.Lock(context.Background(), tx, "o-new")
		return err
	})

	// THEN: it has no payment state yet
	if err != nil || got.State != domain.StateNone || got.OrderID != "o-new" {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func Test_shouldRememberTheChargeWhenAPaymentIsSaved(t *testing.T) {
	// GIVEN: a charged payment
	db := openDB(t)
	ctx := context.Background()
	charged := app.Payment{OrderID: "o-1", State: domain.StateCharged, PaymentID: "pay-1", AmountCents: 4200, Method: events.PaymentMethodCard}

	// WHEN: it is saved and loaded again in a later transaction
	_ = db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := (Postgres{}).Lock(ctx, tx, "o-1"); err != nil {
			return err
		}
		return Postgres{}.Save(ctx, tx, charged)
	})
	var got app.Payment
	_ = db.InTx(ctx, func(tx pgx.Tx) error { var err error; got, err = Postgres{}.Lock(ctx, tx, "o-1"); return err })

	// THEN: everything needed for a later refund is there
	if got != charged {
		t.Fatalf("got %+v", got)
	}
}

func Test_shouldSerialiseConcurrentInputsWhenTwoPodsHandleTheSameOrder(t *testing.T) {
	// GIVEN: two transactions for the same order, e.g. the order and its cancellation on two pods
	db := openDB(t)
	ctx := context.Background()
	firstLocked := make(chan struct{})
	release := make(chan struct{})
	var order []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(2)

	// WHEN: the first holds the row lock while the second tries to lock it
	go func() {
		defer wg.Done()
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			_, _ = Postgres{}.Lock(ctx, tx, "o-race")
			close(firstLocked)
			<-release
			mu.Lock()
			order = append(order, "first")
			mu.Unlock()
			return nil
		})
	}()
	<-firstLocked
	go func() {
		defer wg.Done()
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			_, _ = Postgres{}.Lock(ctx, tx, "o-race")
			mu.Lock()
			order = append(order, "second")
			mu.Unlock()
			return nil
		})
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()

	// THEN: the second waited for the first, so state transitions never interleave
	if len(order) != 2 || order[0] != "first" {
		t.Fatalf("unexpected order %v", order)
	}
}
