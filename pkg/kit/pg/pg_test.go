package pg

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/fstest"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/testkit"
)

func open(t *testing.T, latency time.Duration) *DB {
	t.Helper()
	db, err := Open(context.Background(), Options{URL: testkit.PostgresDSN(t), Latency: func() time.Duration { return latency }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	return db
}

var stockMigrations = fstest.MapFS{
	"001_stock.sql": {Data: []byte(`CREATE TABLE stock (sku text PRIMARY KEY, units int NOT NULL)`)},
	"002_seed.sql":  {Data: []byte(`INSERT INTO stock VALUES ('sku-1', 10)`)},
	"README.md":     {Data: []byte(`not a migration`)},
}

func Test_shouldApplyEveryMigrationOnceWhenTwoPodsStartWithTheSameMigrations(t *testing.T) {
	// GIVEN: an empty inventory database
	db := open(t, 0)
	ctx := context.Background()

	// WHEN: two inventory pods run the migrations one after the other
	if err := db.Migrate(ctx, stockMigrations); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, stockMigrations); err != nil {
		t.Fatal(err)
	}

	// THEN: the seed row exists exactly once and both versions are recorded
	var rows, versions int
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM stock`).Scan(&rows)
	_ = db.Pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&versions)
	if rows != 1 || versions != 2 {
		t.Fatalf("expected 1 stock row and 2 versions, got %d and %d", rows, versions)
	}
}

func Test_shouldStopAtTheBrokenMigrationWhenAMigrationHasAnError(t *testing.T) {
	// GIVEN: a migration set whose second file contains invalid SQL
	db := open(t, 0)
	broken := fstest.MapFS{
		"001_ok.sql":     {Data: []byte(`CREATE TABLE a (id int)`)},
		"002_broken.sql": {Data: []byte(`CREATE TABLE`)},
	}

	// WHEN: the migrations run
	err := db.Migrate(context.Background(), broken)

	// THEN: startup fails naming the broken file, and the first migration stays applied
	if err == nil || !contains(err.Error(), "002_broken.sql") {
		t.Fatalf("expected error naming 002_broken.sql, got %v", err)
	}
	var versions int
	_ = db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM schema_migrations`).Scan(&versions)
	if versions != 1 {
		t.Fatalf("expected 1 applied version, got %d", versions)
	}
}

func Test_shouldRollBackAllChangesWhenTheBusinessStepFails(t *testing.T) {
	// GIVEN: a stock table with 10 units
	db := open(t, 0)
	ctx := context.Background()
	if err := db.Migrate(ctx, stockMigrations); err != nil {
		t.Fatal(err)
	}

	// WHEN: a reservation decrements stock and then fails
	err := db.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE stock SET units = units - 3`); err != nil {
			return err
		}
		return errors.New("payment provider exploded")
	})

	// THEN: the error is returned and the stock is unchanged
	var units int
	_ = db.Pool.QueryRow(ctx, `SELECT units FROM stock`).Scan(&units)
	if err == nil || units != 10 {
		t.Fatalf("expected rollback, got err=%v units=%d", err, units)
	}
}

func Test_shouldDelayEveryTransactionWhenTheDatabaseSlowdownKnobIsSet(t *testing.T) {
	// GIVEN: a database slowdown of 50 ms per transaction
	db := open(t, 50*time.Millisecond)

	// WHEN: an empty transaction runs
	start := time.Now()
	err := db.InTx(context.Background(), func(pgx.Tx) error { return nil })

	// THEN: it commits, but only after the injected delay
	if err != nil || time.Since(start) < 50*time.Millisecond {
		t.Fatalf("expected delayed commit, err=%v took=%v", err, time.Since(start))
	}
}

func Test_shouldAbortTheTransactionWhenTheServiceShutsDownDuringASlowdown(t *testing.T) {
	// GIVEN: a very slow database and a service that is shutting down
	db := open(t, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// WHEN: a transaction is attempted
	err := db.InTx(ctx, func(pgx.Tx) error { return nil })

	// THEN: it gives up immediately instead of blocking the shutdown
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func Test_shouldAbortTheTransactionWhenTheSlowdownOutlastsTheDeadline(t *testing.T) {
	// GIVEN: a slowdown longer than the caller is willing to wait
	db := open(t, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// WHEN: a transaction runs
	err := db.InTx(ctx, func(pgx.Tx) error { return nil })

	// THEN: the transaction is rolled back with a deadline error, which counts as "retry later"
	if !IsUnavailable(err) {
		t.Fatalf("expected an unavailable error, got %v", err)
	}
}

func Test_shouldGiveUpConnectingWhenTheDatabaseNeverComesUp(t *testing.T) {
	// GIVEN: a database address where nothing listens and a startup deadline
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	// WHEN: the service opens the database
	_, err := Open(ctx, Options{URL: "postgres://x:y@127.0.0.1:1/db?connect_timeout=1", RetryEvery: 50 * time.Millisecond,
		Log: testLogger(t), Metrics: metrics.NewSet(), MaxConns: 2})

	// THEN: it reports the database as unreachable once the deadline has passed
	if err == nil || !contains(err.Error(), "not reachable") {
		t.Fatalf("expected unreachable error, got %v", err)
	}
}

func Test_shouldRejectStartupWhenTheDatabaseURLIsMalformed(t *testing.T) {
	// GIVEN: a typo in DATABASE_URL
	// WHEN: the database is opened
	_, err := Open(context.Background(), Options{URL: "postgres://%zz"})

	// THEN: the error points at the URL
	if err == nil || !contains(err.Error(), "database url") {
		t.Fatalf("expected url error, got %v", err)
	}
}

func Test_shouldClassifyInfrastructureErrorsWhenDecidingWhetherToRetry(t *testing.T) {
	// GIVEN: errors seen in production
	cases := map[error]bool{
		nil: false,
		errors.New("dial tcp: connect: connection refused"):     true,
		fmt.Errorf("wrapped: %w", context.DeadlineExceeded):     true,
		errors.New("ERROR: deadlock detected (SQLSTATE 40P01)"): true,
		errors.New(`ERROR: relation "x" does not exist`):        false,
	}
	for err, want := range cases {
		// WHEN: the runner classifies the error
		got := IsUnavailable(err)

		// THEN: only infrastructure problems are retried
		if got != want {
			t.Fatalf("IsUnavailable(%v) = %v, want %v", err, got, want)
		}
	}
}

func Test_shouldFailMigrationsWhenTheMigrationSourceCannotBeRead(t *testing.T) {
	// GIVEN: a migration source with an unreadable file
	db := open(t, 0)
	src := brokenFS{fstest.MapFS{"001_a.sql": {Data: []byte("SELECT 1")}}}

	// WHEN: migrations run
	err := db.Migrate(context.Background(), src)

	// THEN: startup fails
	if err == nil {
		t.Fatal("expected read error")
	}
}

func Test_shouldFailMigrationsWhenTheDatabaseIsGone(t *testing.T) {
	// GIVEN: a database pool that was already closed
	db := open(t, 0)
	db.Close()

	// WHEN: migrations or a transaction run
	errMigrate := db.Migrate(context.Background(), stockMigrations)
	errTx := db.InTx(context.Background(), func(pgx.Tx) error { return nil })

	// THEN: both fail instead of pretending success
	if errMigrate == nil || errTx == nil {
		t.Fatalf("expected errors, got %v and %v", errMigrate, errTx)
	}
}

func Test_shouldFailMigrationsWhenTheGlobPatternIsInvalid(t *testing.T) {
	// GIVEN: a migration source whose glob fails
	db := open(t, 0)

	// WHEN: migrations run
	err := db.Migrate(context.Background(), globErrFS{})

	// THEN: the error is reported
	if err == nil {
		t.Fatal("expected glob error")
	}
}

func Test_shouldCommitWithoutDelayWhenNoSlowdownIsConfigured(t *testing.T) {
	// GIVEN: a service that does not wire the Database Slowdown knob
	db, err := Open(context.Background(), Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// WHEN: a transaction runs
	start := time.Now()
	err = db.InTx(context.Background(), func(pgx.Tx) error { return nil })

	// THEN: it commits right away
	if err != nil || time.Since(start) > time.Second {
		t.Fatalf("err=%v took=%v", err, time.Since(start))
	}
}
