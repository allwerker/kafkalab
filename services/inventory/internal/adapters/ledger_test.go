package adapters

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/inventory/internal/domain"
)

// inBatch runs fn like the runner does: one transaction with a batch scope, before-commit work
// executed after the last record.
func inBatch(t *testing.T, db *pg.DB, fn func(ctx context.Context, tx pgx.Tx) error) error {
	t.Helper()
	ctx, batch := runner.NewBatch(context.Background())
	return db.InTx(ctx, func(tx pgx.Tx) error {
		if err := fn(ctx, tx); err != nil {
			return err
		}
		return batch.RunBeforeCommit(ctx, tx)
	})
}

func Test_shouldSeeEarlierReservationsOfTheSameBatchWhenSeveralOrdersWantTheSameProduct(t *testing.T) {
	// GIVEN: 10 units of a popular product and a batch with three orders for 4 units each
	db := openDB(t)
	setStock(t, db, "sku-001", 10)
	repo := Postgres{}
	var seen []int

	// WHEN: the batch reserves order by order against the ledger
	err := inBatch(t, db, func(ctx context.Context, tx pgx.Tx) error {
		for i := 0; i < 3; i++ {
			avail, err := repo.LockStock(ctx, tx, []string{"sku-001"})
			if err != nil {
				return err
			}
			seen = append(seen, avail["sku-001"])
			if avail["sku-001"] >= 4 {
				if err := repo.Take(ctx, tx, []domain.Line{{SKU: "sku-001", Quantity: 4}}); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// THEN: each order saw what the previous ones left, the third was short, and 2 units remain
	if err != nil || len(seen) != 3 || seen[0] != 10 || seen[1] != 6 || seen[2] != 2 || stockOf(t, db, "sku-001") != 2 {
		t.Fatalf("err=%v seen=%v stock=%d", err, seen, stockOf(t, db, "sku-001"))
	}
}

func Test_shouldNeverOverfillAShelfWhenACancelledOrderReturnsUnitsInABatch(t *testing.T) {
	// GIVEN: a nearly full shelf (capacity minus one) of a product
	db := openDB(t)
	var capacity int
	_ = db.Pool.QueryRow(context.Background(), `SELECT capacity FROM stock WHERE sku = 'sku-002'`).Scan(&capacity)
	setStock(t, db, "sku-002", capacity-1)

	// WHEN: a cancellation returns 5 units within a batch
	err := inBatch(t, db, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := (Postgres{}).LockStock(ctx, tx, []string{"sku-002"}); err != nil {
			return err
		}
		return Postgres{}.Return(ctx, tx, []domain.Line{{SKU: "sku-002", Quantity: 5}})
	})

	// THEN: the shelf is full but not above capacity
	if err != nil || stockOf(t, db, "sku-002") != capacity {
		t.Fatalf("err=%v stock=%d capacity=%d", err, stockOf(t, db, "sku-002"), capacity)
	}
}

func Test_shouldWriteNothingWhenABatchOnlyLooksAtStock(t *testing.T) {
	// GIVEN: a batch whose only order was rejected without touching stock, and a product unknown to the shelf
	db := openDB(t)
	before := stockOf(t, db, "sku-003")

	// WHEN: the batch looks up stock and adjusts only an unknown product
	err := inBatch(t, db, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := (Postgres{}).LockStock(ctx, tx, []string{"sku-003", "sku-999"}); err != nil {
			return err
		}
		return Postgres{}.Take(ctx, tx, []domain.Line{{SKU: "sku-999", Quantity: 1}})
	})

	// THEN: nothing changes on the shelves
	if err != nil || stockOf(t, db, "sku-003") != before {
		t.Fatalf("err=%v stock=%d before=%d", err, stockOf(t, db, "sku-003"), before)
	}
}

func Test_shouldNotDeadlockWhenTwoBatchesReserveProductsInOppositeOrder(t *testing.T) {
	// GIVEN: two batches on different partitions; one wants sku-010 then sku-001, the other the reverse
	db := openDB(t)
	repo := Postgres{}
	firstLocked := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)

	// WHEN: both run concurrently and the first one holds its locks for a moment
	go func() {
		defer wg.Done()
		errs[0] = inBatch(t, db, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := repo.LockStock(ctx, tx, []string{"sku-010"}); err != nil {
				return err
			}
			close(firstLocked)
			time.Sleep(200 * time.Millisecond)
			if _, err := repo.LockStock(ctx, tx, []string{"sku-001"}); err != nil {
				return err
			}
			return repo.Take(ctx, tx, []domain.Line{{SKU: "sku-010", Quantity: 1}, {SKU: "sku-001", Quantity: 1}})
		})
	}()
	<-firstLocked
	go func() {
		defer wg.Done()
		errs[1] = inBatch(t, db, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := repo.LockStock(ctx, tx, []string{"sku-001"}); err != nil {
				return err
			}
			if _, err := repo.LockStock(ctx, tx, []string{"sku-010"}); err != nil {
				return err
			}
			return repo.Take(ctx, tx, []domain.Line{{SKU: "sku-001", Quantity: 1}, {SKU: "sku-010", Quantity: 1}})
		})
	}()
	wg.Wait()

	// THEN: both commit (the second simply waited), so no deadlock retry is needed
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("errors: %v / %v", errs[0], errs[1])
	}
}

func Test_shouldRetryTheBatchWhenTheStockTableCannotBeLocked(t *testing.T) {
	// GIVEN: a batch whose transaction is already broken
	db := openDB(t)
	ctx, _ := runner.NewBatch(context.Background())

	// WHEN: stock is accessed
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		_, _ = tx.Exec(ctx, `SELECT broken`)
		_, err := (Postgres{}).LockStock(ctx, tx, []string{"sku-001"})
		if err == nil {
			return errors.New("expected failure")
		}
		if err := (Postgres{}).Take(ctx, tx, []domain.Line{{SKU: "sku-001", Quantity: 1}}); err == nil {
			return errors.New("expected failure")
		}
		return err
	})

	// THEN: the error surfaces so the runner retries the batch
	if err == nil {
		t.Fatal("expected an error")
	}
}
