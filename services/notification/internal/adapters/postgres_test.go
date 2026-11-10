package adapters

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
	"kafkalab/services/notification/internal/app"
	"kafkalab/services/notification/internal/domain"
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

func count(t *testing.T, db *pg.DB) int {
	t.Helper()
	var n int
	if err := db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// seed logs n notifications sent at the given instant.
func seed(t *testing.T, db *pg.DB, n int, at time.Time) {
	t.Helper()
	if _, err := db.Pool.Exec(context.Background(), `INSERT INTO notifications(order_id, template, channel, sent_at)
		SELECT 'o-' || i, 'order-confirmed', 'EMAIL', $2 FROM generate_series(1, $1) AS i`, n, at); err != nil {
		t.Fatal(err)
	}
}

var sentAt = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func Test_shouldKeepALogEntryWhenANotificationIsSent(t *testing.T) {
	// GIVEN: an empty notification log
	db := openDB(t)
	ctx := context.Background()
	n := app.Notification{OrderID: "o-1", Template: domain.TemplateOrderShipped, Channel: domain.ChannelPush, SentAt: sentAt}

	// WHEN: a shipping notification is recorded
	err := db.InTx(ctx, func(tx pgx.Tx) error { return Postgres{}.Record(ctx, tx, n) })

	// THEN: the log shows who got which message on which channel and when
	var got app.Notification
	var channel string
	_ = db.Pool.QueryRow(ctx, `SELECT order_id, template, channel, sent_at FROM notifications`).Scan(&got.OrderID, &got.Template, &channel, &got.SentAt)
	got.Channel = domain.Channel(channel)
	if err != nil || got.OrderID != n.OrderID || got.Template != n.Template || got.Channel != n.Channel || !got.SentAt.Equal(sentAt) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func Test_shouldForgetOnlyOldNotificationsWhenTheLogIsPruned(t *testing.T) {
	// GIVEN: 5,001 notifications from two hours ago and 3 from a minute ago
	db := openDB(t)
	seed(t, db, 5001, sentAt.Add(-2*time.Hour))
	seed(t, db, 3, sentAt.Add(-time.Minute))
	j := &Janitor{Pool: db.Pool}

	// WHEN: everything older than one hour is pruned
	n, err := j.Prune(context.Background(), sentAt.Add(-time.Hour))

	// THEN: the whole old backlog is gone (in several batches) and the recent entries stay
	if err != nil || n != 5001 || count(t, db) != 3 {
		t.Fatalf("pruned %d, %v, left %d", n, err, count(t, db))
	}
}

func Test_shouldPruneTheLogPeriodicallyWhenTheJanitorRuns(t *testing.T) {
	// GIVEN: a running janitor keeping one hour, and notifications from yesterday
	db := openDB(t)
	seed(t, db, 10, time.Now().Add(-24*time.Hour))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { (&Janitor{Pool: db.Pool, Every: 5 * time.Millisecond}).Run(ctx); close(done) }()

	// WHEN: the janitor had time to run and the service stops
	deadline := time.Now().Add(5 * time.Second)
	for count(t, db) > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	// THEN: yesterday's notifications are gone
	if count(t, db) != 0 {
		t.Fatalf("%d entries left", count(t, db))
	}
}

func Test_shouldKeepRunningWhenTheDatabaseIsUnreachableDuringPruning(t *testing.T) {
	// GIVEN: a janitor whose database is gone
	db := openDB(t)
	db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { (&Janitor{Pool: db.Pool, Every: time.Millisecond}).Run(ctx); close(done) }()

	// WHEN: it tries to prune a few times and the service then stops
	time.Sleep(20 * time.Millisecond)
	cancel()

	// THEN: it stops cleanly instead of crashing the service
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("janitor did not stop")
	}
}

func Test_shouldTakeTheGatewayLatencyWhenANotificationIsDelivered(t *testing.T) {
	// GIVEN: a provider that answers after 20 ms
	p := SimulatedProvider{Latency: 20 * time.Millisecond}

	// WHEN: a notification is sent
	start := time.Now()
	err := p.Send(context.Background(), app.Notification{})

	// THEN: it succeeds after the latency
	if err != nil || time.Since(start) < 20*time.Millisecond {
		t.Fatalf("err=%v after %v", err, time.Since(start))
	}
}

func Test_shouldAbortTheDeliveryWhenTheServiceIsStopping(t *testing.T) {
	// GIVEN: a slow provider and a service that is shutting down
	p := SimulatedProvider{Latency: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// WHEN: a notification is sent
	err := p.Send(ctx, app.Notification{})

	// THEN: the send is aborted (the batch is redelivered later)
	if err == nil {
		t.Fatal("expected cancellation")
	}
}

func Test_shouldDeliverImmediatelyWhenNoLatencyIsConfigured(t *testing.T) {
	// GIVEN: a provider without simulated latency
	p := SimulatedProvider{}

	// WHEN: a notification is sent
	err := p.Send(context.Background(), app.Notification{})

	// THEN: it succeeds
	if err != nil {
		t.Fatal(err)
	}
}
