package adapters

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// GaugeLeaderKey is the advisory lock that elects the pod exporting lab_join_open_orders.
// Advisory locks are database-wide, so the key is specific to shipping.
const GaugeLeaderKey int64 = 0x5_3417_0001

// Leader holds a session-level PostgreSQL advisory lock on one dedicated pooled connection.
// Whoever holds the lock is the leader; the lock vanishes with the connection when the pod
// dies, so another pod takes over on its next attempt.
type Leader struct {
	Pool *pgxpool.Pool
	Key  int64

	mu   sync.Mutex
	conn *pgxpool.Conn
}

// IsLeader keeps or tries to take the lock and reports whether this pod holds it.
func (l *Leader) IsLeader(ctx context.Context) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn != nil {
		if err := l.conn.Ping(ctx); err == nil {
			return true
		}
		// The connection (and with it the lock) is gone: drop it and compete again.
		l.drop()
	}
	conn, err := l.Pool.Acquire(ctx)
	if err != nil {
		return false
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, l.Key).Scan(&got); err != nil || !got {
		conn.Release()
		return false
	}
	l.conn = conn
	return true
}

// Close gives up the leadership at once, so another pod can take over on its next attempt.
func (l *Leader) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return
	}
	if _, err := l.conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, l.Key); err != nil { // coverage-ignore: connection lost during shutdown; closing it frees the lock as well
		l.drop()
		return
	}
	l.conn.Release()
	l.conn = nil
}

// drop closes the dedicated connection of a lost session. Releasing it into the pool instead
// could hand a connection that still holds the lock to unrelated work.
func (l *Leader) drop() {
	_ = l.conn.Conn().Close(context.Background())
	l.conn.Release()
	l.conn = nil
}
