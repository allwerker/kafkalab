// Package kafka builds franz-go clients for lab services: client metrics, Tuning Lab settings
// and the fault-injecting dialer used by the Network Chaos scenario.
package kafka

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Hooks exports client-side Kafka metrics (docs/contracts/metrics.md, "franz-go client").
// Client metrics show what brokers cannot see: time spent waiting on the network,
// connection churn during broker failures and how full the client buffers are.
type Hooks struct {
	set     *metrics.Set
	mu      sync.Mutex
	sources []func() *kgo.Client
}

// NewHooks returns hooks that register their metrics in set.
func NewHooks(set *metrics.Set) *Hooks { return &Hooks{set: set} }

func node(meta kgo.BrokerMetadata) string { return strconv.Itoa(int(meta.NodeID)) }

func (h *Hooks) counter(name string, labels ...string) *metrics.Counter {
	return h.set.GetOrCreateCounter(withLabels(name, labels...))
}

func withLabels(name string, kv ...string) string {
	if len(kv) == 0 {
		return name
	}
	s := name + "{"
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%s=%q", kv[i], kv[i+1])
	}
	return s + "}"
}

// OnBrokerConnect implements kgo.HookBrokerConnect.
func (h *Hooks) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		h.counter("kafka_client_connect_errors_total", "node", node(meta)).Inc()
		return
	}
	h.counter("kafka_client_connects_total", "node", node(meta)).Inc()
}

// OnBrokerDisconnect implements kgo.HookBrokerDisconnect.
func (h *Hooks) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	h.counter("kafka_client_disconnects_total", "node", node(meta)).Inc()
}

// OnBrokerWrite implements kgo.HookBrokerWrite.
func (h *Hooks) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, n int, _, timeToWrite time.Duration, _ error) {
	h.counter("kafka_client_write_bytes_total", "node", node(meta)).Add(n)
	h.set.GetOrCreateHistogram(withLabels("kafka_client_request_duration_seconds", "node", node(meta))).Update(timeToWrite.Seconds())
}

// OnBrokerRead implements kgo.HookBrokerRead.
func (h *Hooks) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, n int, readWait, timeToRead time.Duration, _ error) {
	h.counter("kafka_client_read_bytes_total", "node", node(meta)).Add(n)
	h.set.GetOrCreateHistogram(withLabels("kafka_client_request_duration_seconds", "node", node(meta))).Update((readWait + timeToRead).Seconds())
}

// OnBrokerThrottle implements kgo.HookBrokerThrottle.
func (h *Hooks) OnBrokerThrottle(meta kgo.BrokerMetadata, interval time.Duration, _ bool) {
	h.set.GetOrCreateFloatCounter(withLabels("kafka_client_throttle_seconds_total", "node", node(meta))).Add(interval.Seconds())
}

// OnProduceBatchWritten implements kgo.HookProduceBatchWritten.
func (h *Hooks) OnProduceBatchWritten(_ kgo.BrokerMetadata, topic string, _ int32, m kgo.ProduceBatchMetrics) {
	h.counter("kafka_client_produce_batches_total", "topic", topic).Inc()
	h.counter("kafka_client_produce_records_total", "topic", topic).Add(m.NumRecords)
	h.counter("kafka_client_produce_bytes_total", "topic", topic).Add(m.CompressedBytes)
	h.set.GetOrCreateHistogram(withLabels("kafka_client_produce_batch_records", "topic", topic)).Update(float64(m.NumRecords))
}

// OnFetchBatchRead implements kgo.HookFetchBatchRead.
func (h *Hooks) OnFetchBatchRead(_ kgo.BrokerMetadata, topic string, _ int32, m kgo.FetchBatchMetrics) {
	h.counter("kafka_client_fetch_records_total", "topic", topic).Add(m.NumRecords)
	h.counter("kafka_client_fetch_bytes_total", "topic", topic).Add(m.CompressedBytes)
}

// WatchBuffers adds a client source to the produce and fetch buffer gauges. Several sources
// (e.g. a consumer runner and a separate producer) are summed. current may return nil while
// a client is being rebuilt.
func (h *Hooks) WatchBuffers(current func() *kgo.Client) {
	h.mu.Lock()
	h.sources = append(h.sources, current)
	first := len(h.sources) == 1
	h.mu.Unlock()
	if !first {
		return
	}
	h.set.GetOrCreateGauge("kafka_client_buffered_produce_records", func() float64 {
		return h.sum(func(c *kgo.Client) int64 { return c.BufferedProduceRecords() })
	})
	h.set.GetOrCreateGauge("kafka_client_buffered_fetch_records", func() float64 {
		return h.sum(func(c *kgo.Client) int64 { return c.BufferedFetchRecords() })
	})
}

func (h *Hooks) sum(f func(*kgo.Client) int64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var n int64
	for _, src := range h.sources {
		if c := src(); c != nil {
			n += f(c)
		}
	}
	return float64(n)
}

// Label formats a metric name with label pairs, e.g. Label("x_total", "topic", "t").
func Label(name string, kv ...string) string { return withLabels(name, kv...) }

var (
	_ kgo.HookBrokerConnect       = (*Hooks)(nil)
	_ kgo.HookBrokerDisconnect    = (*Hooks)(nil)
	_ kgo.HookBrokerWrite         = (*Hooks)(nil)
	_ kgo.HookBrokerRead          = (*Hooks)(nil)
	_ kgo.HookBrokerThrottle      = (*Hooks)(nil)
	_ kgo.HookProduceBatchWritten = (*Hooks)(nil)
	_ kgo.HookFetchBatchRead      = (*Hooks)(nil)
)
