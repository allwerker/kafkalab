package adapters

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
	"kafkalab/services/inventory/internal/app"
	"kafkalab/services/inventory/internal/domain"
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

func stockOf(t *testing.T, db *pg.DB, sku string) int {
	t.Helper()
	var n int
	if err := db.Pool.QueryRow(context.Background(), `SELECT available FROM stock WHERE sku = $1`, sku).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func setStock(t *testing.T, db *pg.DB, sku string, n int) {
	t.Helper()
	if _, err := db.Pool.Exec(context.Background(), `UPDATE stock SET available = $2 WHERE sku = $1`, sku, n); err != nil {
		t.Fatal(err)
	}
}

func Test_shouldStockEveryCatalogueProductAtCapacityWhenTheWarehouseOpens(t *testing.T) {
	// GIVEN: a freshly migrated inventory database
	db := openDB(t)

	// WHEN: the shelves are inspected
	rows, _ := db.Pool.Query(context.Background(), `SELECT sku, capacity, available FROM stock ORDER BY sku`)
	type shelf struct {
		sku                 string
		capacity, available int
	}
	var got []shelf
	var s shelf
	_, err := pgx.ForEachRow(rows, []any{&s.sku, &s.capacity, &s.available}, func() error { got = append(got, s); return nil })

	// THEN: sku-001..sku-040 are full, sized by popularity like the domain plans it
	if err != nil || len(got) != 40 {
		t.Fatalf("got %d shelves, %v", len(got), err)
	}
	for i, s := range got {
		if s.sku != fmt.Sprintf("sku-%03d", i+1) || s.capacity != domain.Capacity(i+1) || s.available != s.capacity {
			t.Fatalf("unexpected shelf %+v", s)
		}
	}
}

func Test_shouldStartWithAnEmptyStateWhenAnOrderIsSeenForTheFirstTime(t *testing.T) {
	// GIVEN: an empty reservation store
	db := openDB(t)
	var got app.Reservation

	// WHEN: an unknown order is locked
	err := db.InTx(context.Background(), func(tx pgx.Tx) error {
		var err error
		got, err = Postgres{}.Lock(context.Background(), tx, "o-new")
		return err
	})

	// THEN: it has no reservation yet
	if err != nil || got.State != domain.StateNone || got.OrderID != "o-new" || len(got.Lines) != 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func Test_shouldReserveAndReleaseStockWhenAnOrderIsReservedAndLaterCancelled(t *testing.T) {
	// GIVEN: a full warehouse and an order for 3x sku-001 and 2x sku-040
	db := openDB(t)
	ctx := context.Background()
	lines := []domain.Line{{SKU: "sku-001", Quantity: 3}, {SKU: "sku-040", Quantity: 2}}
	repo := Postgres{}

	// WHEN: the order is reserved in one transaction
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		r, err := repo.Lock(ctx, tx, "o-1")
		if err != nil {
			return err
		}
		avail, err := repo.LockStock(ctx, tx, []string{"sku-001", "sku-040", "sku-999"})
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(avail, map[string]int{"sku-001": 3000, "sku-040": 200}) {
			return fmt.Errorf("available %v", avail)
		}
		if err := repo.Take(ctx, tx, lines); err != nil {
			return err
		}
		r.State, r.ReservationID, r.Warehouse, r.Lines = domain.StateReserved, "res-1", "hamburg", lines
		return repo.Save(ctx, tx, r)
	})

	// THEN: the stock went down and the reservation remembers its lines for a release
	if err != nil || stockOf(t, db, "sku-001") != 2997 || stockOf(t, db, "sku-040") != 198 {
		t.Fatalf("err=%v", err)
	}
	var stored app.Reservation
	_ = db.InTx(ctx, func(tx pgx.Tx) error { var err error; stored, err = repo.Lock(ctx, tx, "o-1"); return err })
	want := app.Reservation{OrderID: "o-1", State: domain.StateReserved, ReservationID: "res-1", Warehouse: "hamburg", Lines: lines}
	if !reflect.DeepEqual(stored, want) {
		t.Fatalf("stored %+v", stored)
	}

	// AND WHEN: the order is cancelled and the units are returned
	err = db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := repo.LockStock(ctx, tx, domain.SKUs(lines)); err != nil {
			return err
		}
		return repo.Return(ctx, tx, lines)
	})

	// THEN: the shelves are full again
	if err != nil || stockOf(t, db, "sku-001") != 3000 || stockOf(t, db, "sku-040") != 200 {
		t.Fatalf("err=%v", err)
	}
}

func Test_shouldNeverOverfillAShelfWhenUnitsComeBackAfterARestock(t *testing.T) {
	// GIVEN: sku-020 was refilled to its capacity while 5 units were still reserved
	db := openDB(t)
	ctx := context.Background()

	// WHEN: those 5 units are released
	err := db.InTx(ctx, func(tx pgx.Tx) error { return Postgres{}.Return(ctx, tx, []domain.Line{{SKU: "sku-020", Quantity: 5}}) })

	// THEN: the shelf holds exactly its capacity
	if err != nil || stockOf(t, db, "sku-020") != 200 {
		t.Fatalf("err=%v stock=%d", err, stockOf(t, db, "sku-020"))
	}
}

func Test_shouldStoreARejectionWhenTheOrderHasNoReservedLines(t *testing.T) {
	// GIVEN: an order that inventory rejected
	db := openDB(t)
	ctx := context.Background()

	// WHEN: the rejection is saved and loaded again
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		r, _ := Postgres{}.Lock(ctx, tx, "o-2")
		r.State = domain.StateRejected
		return Postgres{}.Save(ctx, tx, r)
	})
	var got app.Reservation
	_ = db.InTx(ctx, func(tx pgx.Tx) error { var err error; got, err = Postgres{}.Lock(ctx, tx, "o-2"); return err })

	// THEN: the order is rejected without lines
	if err != nil || got.State != domain.StateRejected || len(got.Lines) != 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func Test_shouldRefuseToOversellWhenStockWouldGoNegative(t *testing.T) {
	// GIVEN: only 2 units of sku-030 left
	db := openDB(t)
	ctx := context.Background()
	setStock(t, db, "sku-030", 2)

	// WHEN: 3 units are taken anyway (a bug bypassing the domain check)
	err := db.InTx(ctx, func(tx pgx.Tx) error { return Postgres{}.Take(ctx, tx, []domain.Line{{SKU: "sku-030", Quantity: 3}}) })

	// THEN: the database refuses and the stock is unchanged
	if err == nil || stockOf(t, db, "sku-030") != 2 {
		t.Fatalf("err=%v stock=%d", err, stockOf(t, db, "sku-030"))
	}
}

func Test_shouldFailTheStockLookupWhenTheTransactionIsBroken(t *testing.T) {
	// GIVEN: a transaction that already failed (e.g. after a statement error)
	db := openDB(t)
	ctx := context.Background()
	var err error

	// WHEN: stock is locked in it
	_ = db.InTx(ctx, func(tx pgx.Tx) error {
		_, _ = tx.Exec(ctx, `SELECT 1/0`)
		_, err = Postgres{}.LockStock(ctx, tx, []string{"sku-001"})
		return err
	})

	// THEN: the error is reported so the runner retries the batch
	if err == nil {
		t.Fatal("expected an error")
	}
}

func Test_shouldWaitForTheOtherOrderWhenTwoPodsReserveTheSameProduct(t *testing.T) {
	// GIVEN: two transactions reserving sku-001, the first holds its stock lock
	db := openDB(t)
	ctx := context.Background()
	firstLocked := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	wg.Add(2)

	// WHEN: the second tries to lock the same stock row
	go func() {
		defer wg.Done()
		_ = db.InTx(ctx, func(tx pgx.Tx) error {
			_, _ = Postgres{}.LockStock(ctx, tx, []string{"sku-001"})
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
			_, _ = Postgres{}.LockStock(ctx, tx, []string{"sku-001", "sku-002"})
			mu.Lock()
			order = append(order, "second")
			mu.Unlock()
			return nil
		})
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	wg.Wait()

	// THEN: it waited, so the same units can never be sold twice
	if len(order) != 2 || order[0] != "first" {
		t.Fatalf("unexpected order %v", order)
	}
}
