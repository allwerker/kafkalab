package adapters

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/pkg/kit/pg"
)

func restocker(db *pg.DB, now *time.Time) (*Restocker, *metrics.Set) {
	set := metrics.NewSet()
	return &Restocker{Pool: db.Pool, Metrics: set, Now: func() time.Time { return *now }}, set
}

func exposition(set *metrics.Set) string {
	var buf bytes.Buffer
	set.WritePrometheus(&buf)
	return buf.String()
}

func Test_shouldRefillOnlyNearlyEmptyShelvesWhenTheRestockRuns(t *testing.T) {
	// GIVEN: sku-001 at 599 of 3000 (below 20%), sku-002 at 300 of 1500 (exactly 20%), sku-040 empty
	db := openDB(t)
	setStock(t, db, "sku-001", 599)
	setStock(t, db, "sku-002", 300)
	setStock(t, db, "sku-040", 0)
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r, _ := restocker(db, &now)
	defer r.Resign()

	// WHEN: the restock job runs
	leader := r.Tick(context.Background())

	// THEN: sku-001 and sku-040 are full again, sku-002 is left alone
	if !leader || stockOf(t, db, "sku-001") != 3000 || stockOf(t, db, "sku-002") != 300 || stockOf(t, db, "sku-040") != 200 {
		t.Fatalf("leader=%v stock %d/%d/%d", leader, stockOf(t, db, "sku-001"), stockOf(t, db, "sku-002"), stockOf(t, db, "sku-040"))
	}
}

func Test_shouldLetAFlashSaleEmptyAShelfWhenTheNextRestockIsNotDueYet(t *testing.T) {
	// GIVEN: a restock that just ran
	db := openDB(t)
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r, _ := restocker(db, &now)
	defer r.Resign()
	r.Tick(context.Background())

	// WHEN: a flash sale empties sku-005 and the job ticks again 5 s later, then 10 s later
	setStock(t, db, "sku-005", 0)
	now = now.Add(5 * time.Second)
	r.Tick(context.Background())
	early := stockOf(t, db, "sku-005")
	now = now.Add(5 * time.Second)
	r.Tick(context.Background())

	// THEN: the shelf stays empty until the restock interval has passed, then it is refilled (sawtooth)
	if early != 0 || stockOf(t, db, "sku-005") != 600 {
		t.Fatalf("after 5 s: %d, after 10 s: %d", early, stockOf(t, db, "sku-005"))
	}
}

func Test_shouldExportStockGaugesOnlyFromOnePodWhenTwoPodsRun(t *testing.T) {
	// GIVEN: two inventory pods sharing one database, sku-003 at 640 units
	db := openDB(t)
	setStock(t, db, "sku-003", 640)
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	a, setA := restocker(db, &now)
	b, setB := restocker(db, &now)
	defer a.Resign()
	defer b.Resign()

	// WHEN: both run the restock job
	leadA, leadB := a.Tick(context.Background()), b.Tick(context.Background())

	// THEN: only the first leads and exports the 20 most popular products
	expA, expB := exposition(setA), exposition(setB)
	if !leadA || leadB || strings.Count(expA, "lab_stock_units{") != GaugedSKUs || expB != "" {
		t.Fatalf("leaders %v/%v\nA:\n%s\nB:\n%s", leadA, leadB, expA, expB)
	}
	if !strings.Contains(expA, `lab_stock_units{sku="sku-003"} 640`) || strings.Contains(expA, `sku-021`) {
		t.Fatalf("unexpected gauges:\n%s", expA)
	}

	// AND WHEN: the leading pod shuts down
	a.Resign()
	leadB = b.Tick(context.Background())

	// THEN: the other pod takes over restocking and the gauges move with it
	if !leadB || exposition(setA) != "" || strings.Count(exposition(setB), "lab_stock_units{") != GaugedSKUs {
		t.Fatalf("handover failed: leaderB=%v\nA:\n%s", leadB, exposition(setA))
	}
}

func Test_shouldHandOverRestockingWhenTheLeadersDatabaseSessionDies(t *testing.T) {
	// GIVEN: a leading pod
	db := openDB(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r, set := restocker(db, &now)
	defer r.Resign()
	r.Tick(ctx)

	// WHEN: its session is killed (database failover) before the next restock, then the job ticks once more
	_, _ = db.Pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND pid <> pg_backend_pid()`)
	now = now.Add(10 * time.Second)
	afterKill := r.Tick(ctx)
	gauges := exposition(set)
	recovered := r.Tick(ctx)

	// THEN: it stops leading and exporting at once, and leads again on a fresh session
	if afterKill || gauges != "" || !recovered {
		t.Fatalf("afterKill=%v recovered=%v gauges:\n%s", afterKill, recovered, gauges)
	}
}

func Test_shouldStopExportingWhenTheLeaderCannotReadTheStock(t *testing.T) {
	// GIVEN: a leading pod that restocked recently
	db := openDB(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r, set := restocker(db, &now)
	defer r.Resign()
	r.Tick(ctx)

	// WHEN: its session dies between two restocks (only the gauge refresh runs)
	_, _ = db.Pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND pid <> pg_backend_pid()`)
	now = now.Add(2 * time.Second)
	leader := r.Tick(ctx)

	// THEN: it gives up the leadership and its gauges vanish
	if leader || exposition(set) != "" {
		t.Fatalf("leader=%v\n%s", leader, exposition(set))
	}
}

func Test_shouldNotLeadWhenTheDatabaseIsUnreachable(t *testing.T) {
	// GIVEN: a pod whose database pool is gone
	db := openDB(t)
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	r, set := restocker(db, &now)
	db.Close()

	// WHEN: the restock job ticks
	leader := r.Tick(context.Background())

	// THEN: it neither leads nor exports stock gauges
	if leader || exposition(set) != "" {
		t.Fatalf("leader=%v", leader)
	}
}

func Test_shouldRestockPeriodicallyUntilStoppedWhenTheJobRuns(t *testing.T) {
	// GIVEN: a running restock job with short intervals and an empty sku-007 shelf
	db := openDB(t)
	setStock(t, db, "sku-007", 0)
	set := metrics.NewSet()
	r := &Restocker{Pool: db.Pool, Metrics: set, Every: 10 * time.Millisecond, GaugeEvery: 5 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	// WHEN: the shelf is emptied again while the job runs, and the pod then stops
	deadline := time.Now().Add(5 * time.Second)
	for stockOf(t, db, "sku-007") != 450 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	setStock(t, db, "sku-007", 0)
	for stockOf(t, db, "sku-007") != 450 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	// THEN: the shelf was refilled both times and the stopped pod exports no gauges
	if stockOf(t, db, "sku-007") != 450 || exposition(set) != "" {
		t.Fatalf("stock=%d\n%s", stockOf(t, db, "sku-007"), exposition(set))
	}
}
