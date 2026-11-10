package kafka

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"
)

// ErrInjectedReset is returned when the Network Chaos scenario drops a connection.
var ErrInjectedReset = errors.New("connection reset (injected by Network Chaos)")

// Faults describes client-side network faults: added latency per write and the probability
// that a write (or a new connection) fails as if the peer had reset it.
type Faults struct {
	Latency         time.Duration
	DropProbability float64
}

// FaultDialer wraps a dialer and injects the faults returned by current. Faults are read on every
// operation, so a scenario can start and stop without rebuilding the Kafka client.
type FaultDialer struct {
	current func() Faults
	dial    func(ctx context.Context, network, address string) (net.Conn, error)
	random  func() float64
	sleep   func(time.Duration)
	drops   atomic.Int64
}

// NewFaultDialer returns a dialer that applies current() to every connection.
func NewFaultDialer(current func() Faults) *FaultDialer {
	d := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &FaultDialer{current: current, dial: d.DialContext, random: rand.Float64, sleep: time.Sleep}
}

// Drops returns how many operations were failed on purpose (for metrics and tests).
func (f *FaultDialer) Drops() int64 { return f.drops.Load() }

// DialContext matches kgo.Dialer's function signature.
func (f *FaultDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if fl := f.current(); fl.DropProbability > 0 && f.random() < fl.DropProbability {
		f.drops.Add(1)
		return nil, ErrInjectedReset
	}
	c, err := f.dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &faultConn{Conn: c, f: f}, nil
}

type faultConn struct {
	net.Conn
	f *FaultDialer
}

func (c *faultConn) Write(b []byte) (int, error) {
	fl := c.f.current()
	if fl.DropProbability > 0 && c.f.random() < fl.DropProbability {
		c.f.drops.Add(1)
		_ = c.Conn.Close()
		return 0, ErrInjectedReset
	}
	if fl.Latency > 0 {
		c.f.sleep(fl.Latency)
	}
	return c.Conn.Write(b)
}
