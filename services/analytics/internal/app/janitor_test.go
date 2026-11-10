package app

import (
	"context"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/services/analytics/internal/domain"
)

type memDB struct{}

func (memDB) InTx(_ context.Context, fn func(pgx.Tx) error) error { return fn(nil) }

// lockedDB serialises transactions with the test goroutine that inspects the rows.
type lockedDB struct{}

var dbMu sync.Mutex

func (lockedDB) InTx(_ context.Context, fn func(pgx.Tx) error) error {
	dbMu.Lock()
	defer dbMu.Unlock()
	return fn(nil)
}

func rows(w *world) int {
	dbMu.Lock()
	defer dbMu.Unlock()
	return len(w.repo.rows)
}

func janitor(w *world) *Janitor {
	return &Janitor{DB: memDB{}, Repo: w.repo, Log: slog.New(slog.DiscardHandler), Now: func() time.Time { return w.now },
		Every: time.Millisecond, Batch: 2}
}

func Test_shouldForgetFinishedOrdersWhenTheirHourIsOver(t *testing.T) {
	// GIVEN: five orders shipped an hour ago and one still in flight
	w := newWorld()
	for i := range 5 {
		id := "o-" + strconv.Itoa(i)
		w.handle(t, order(id, "NORTH", 100))
		w.handle(t, shipped(id))
	}
	w.handle(t, order("in-flight", "NORTH", 100))
	w.now = t0.Add(domain.KeepAfterOutcome + time.Second)

	// WHEN: the janitor runs (two rows per transaction)
	n := janitor(w).Purge(context.Background())

	// THEN: the finished orders are gone, the open one stays
	if n != 5 || len(w.repo.rows) != 1 || w.repo.rows["in-flight"].OrderID == "" {
		t.Fatalf("purged %d, left %v", n, w.repo.rows)
	}
}

func Test_shouldKeepTheRowsWhenPurgingFails(t *testing.T) {
	// GIVEN: an expired order and a failing database
	w := newWorld()
	w.handle(t, cancelled("o-1"))
	w.now = t0.Add(2 * time.Hour)
	w.repo.failPurge = errDown

	// WHEN: the janitor runs
	n := janitor(w).Purge(context.Background())

	// THEN: nothing is deleted; the next run tries again
	if n != 0 || len(w.repo.rows) != 1 {
		t.Fatalf("purged %d", n)
	}
}

func Test_shouldPurgePeriodicallyWhenRunningUntilTheServiceStops(t *testing.T) {
	// GIVEN: an expired order and a janitor running every millisecond
	w := newWorld()
	w.handle(t, cancelled("o-1"))
	w.now = t0.Add(2 * time.Hour)
	j := janitor(w)
	j.DB = lockedDB{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- j.Run(ctx) }()

	// WHEN: the row is gone and the service shuts down
	for rows(w) != 0 {
		time.Sleep(time.Millisecond)
	}
	cancel()

	// THEN: the janitor returns cleanly
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
