package adapters

import (
	"context"
	"testing"
	"time"

	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
)

func Test_shouldElectExactlyOneGaugeLeaderWhenTwoPodsCompete(t *testing.T) {
	// GIVEN: two shipping pods sharing one database
	db := openDB(t)
	ctx := context.Background()
	a, b := &Leader{Pool: db.Pool, Key: GaugeLeaderKey}, &Leader{Pool: db.Pool, Key: GaugeLeaderKey}
	defer b.Close()

	// WHEN: both ask repeatedly, then the leader shuts down
	firstA, firstB, againA := a.IsLeader(ctx), b.IsLeader(ctx), a.IsLeader(ctx)
	a.Close()
	takeover := b.IsLeader(ctx)

	// THEN: the first pod leads and keeps leading, the second follows until it takes over
	if !firstA || firstB || !againA || !takeover {
		t.Fatalf("a=%v b=%v a again=%v takeover=%v", firstA, firstB, againA, takeover)
	}
}

func Test_shouldLetAnotherPodTakeOverWhenTheLeadersDatabaseConnectionDies(t *testing.T) {
	// GIVEN: a leader pod and a follower pod
	db := openDB(t)
	ctx := context.Background()
	a, b := &Leader{Pool: db.Pool, Key: GaugeLeaderKey}, &Leader{Pool: db.Pool, Key: GaugeLeaderKey}
	defer a.Close()
	defer b.Close()
	if !a.IsLeader(ctx) || b.IsLeader(ctx) {
		t.Fatal("expected a to lead")
	}

	// WHEN: the leader's session is killed on the server (network cut, PostgreSQL restart)
	if _, err := db.Pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory' AND objid::bigint = $1
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		GaugeLeaderKey&0xffffffff); err != nil {
		t.Fatal(err)
	}
	takeover := b.IsLeader(ctx)
	for i := 0; i < 50 && !takeover; i++ { // the server frees the lock once the backend has exited
		time.Sleep(100 * time.Millisecond)
		takeover = b.IsLeader(ctx)
	}
	aStill := a.IsLeader(ctx)

	// THEN: the follower takes over and the old leader notices it lost the lock
	if !takeover || aStill {
		t.Fatalf("takeover=%v a still=%v", takeover, aStill)
	}
}

func Test_shouldNotClaimLeadershipWhenTheDatabaseIsUnreachable(t *testing.T) {
	// GIVEN: a pod whose database pool is gone
	db, err := pg.Open(context.Background(), pg.Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	l := &Leader{Pool: db.Pool, Key: GaugeLeaderKey}

	// WHEN: it asks for the leadership
	got := l.IsLeader(context.Background())

	// THEN: it stays a follower (exports 0) instead of failing
	if got {
		t.Fatal("must not lead without a database")
	}
}
