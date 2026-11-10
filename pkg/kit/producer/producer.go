// Package producer is the producing side for services without input topics (order-generator).
// It owns the Kafka client and rebuilds it when the Tuning Lab changes producer settings.
package producer

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
)

// Knobs provides the control-plane state (implemented by *control.Client).
type Knobs interface {
	Knobs() events.Knobs
}

// Config configures a Producer.
type Config struct {
	Service   string
	Brokers   []string
	Control   Knobs
	Metrics   *metrics.Set
	Log       *slog.Logger
	Hooks     []kgo.Hook
	Dialer    func(ctx context.Context, network, host string) (net.Conn, error)
	ExtraOpts []kgo.Opt
}

// Producer produces records asynchronously with a client matching the current tuning.
type Producer struct {
	cfg      Config
	mu       sync.RWMutex
	cl       *kgo.Client
	tuning   events.ClientTuning
	rebuilds *metrics.Counter
}

// New creates the first client.
func New(cfg Config) (*Producer, error) {
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewSet()
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	p := &Producer{cfg: cfg, rebuilds: cfg.Metrics.GetOrCreateCounter("lab_client_rebuilds_total")}
	t := cfg.Control.Knobs().Client
	cl, err := p.newClient(t)
	if err != nil { // coverage-ignore: only invalid static options fail here
		return nil, err
	}
	p.cl, p.tuning = cl, t
	return p, nil
}

func (p *Producer) newClient(t events.ClientTuning) (*kgo.Client, error) {
	tuning, _, lossy := kafka.TuningOptions(t)
	if lossy {
		p.cfg.Log.Warn("Tuning Lab lowered acks: idempotent writes are off, orders may be lost or duplicated")
	}
	opts := append(kafka.BaseOptions(p.cfg.Brokers, p.cfg.Service, p.cfg.Hooks...), tuning...)
	if p.cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(p.cfg.Dialer))
	}
	return kgo.NewClient(append(opts, p.cfg.ExtraOpts...)...)
}

// Client returns the current client (for buffer gauges).
func (p *Producer) Client() *kgo.Client {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cl
}

// Refresh rebuilds the client if the tuning changed: the old client is flushed first, so no
// buffered order is lost by a Tuning Lab change.
func (p *Producer) Refresh(ctx context.Context) error {
	t := p.cfg.Control.Knobs().Client
	p.mu.RLock()
	same := kafka.SameTuning(t, p.tuning)
	p.mu.RUnlock()
	if same {
		return nil
	}
	cl, err := p.newClient(t)
	if err != nil { // coverage-ignore: only invalid static options fail here
		return err
	}
	p.mu.Lock()
	old := p.cl
	p.cl, p.tuning = cl, t
	p.mu.Unlock()
	if err := old.Flush(ctx); err != nil {
		p.cfg.Log.WarnContext(ctx, "flush before client rebuild failed", "error", err)
	}
	old.Close()
	p.rebuilds.Inc()
	p.cfg.Log.InfoContext(ctx, "producer rebuilt with new tuning")
	return nil
}

// Produce sends rec asynchronously; promise is called with the delivery result.
func (p *Producer) Produce(ctx context.Context, rec *kgo.Record, promise func(*kgo.Record, error)) {
	p.Client().Produce(ctx, rec, promise)
}

// Close flushes and closes the client.
func (p *Producer) Close(ctx context.Context) {
	cl := p.Client()
	_ = cl.Flush(ctx)
	cl.Close()
}
