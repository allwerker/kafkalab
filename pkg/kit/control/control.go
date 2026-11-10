// Package control receives the controlboard's desired state from the compacted
// platform.control topic. Each record is the FULL knob set for one service (never a delta),
// so the newest record per key is all a service needs, also after a restart.
package control

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
)

// Decoder decodes control records (implemented by *serde.Serde).
type Decoder interface {
	Decode([]byte) (any, error)
}

// Options configures Start.
type Options struct {
	Brokers []string
	// Service whose knobs are tracked; empty tracks every service (controlboard).
	Service string
	Decoder Decoder
	Metrics *metrics.Set
	Log     *slog.Logger
	Now     func() time.Time
	// ExtraOpts are appended to the client options (tests).
	ExtraOpts []kgo.Opt
}

// Client holds the latest snapshot per service and notifies about changes.
type Client struct {
	o       Options
	cl      *kgo.Client
	mu      sync.RWMutex
	snaps   map[string]events.ControlSnapshot
	changed chan struct{}
	timers  map[string]*time.Timer
	done    chan struct{}
}

// Start connects with a dedicated client (no consumer group, never fault-injected, never
// rebuilt by the Tuning Lab) and returns once the topic has been read up to its end, so a
// service never starts processing with stale knobs.
func Start(ctx context.Context, o Options) (*Client, error) {
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Metrics == nil {
		o.Metrics = metrics.NewSet()
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	opts := append(kafka.BaseOptions(o.Brokers, o.Service+"-control"),
		kgo.ConsumeTopics(contracts.TopicPlatformControl),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	cl, err := kgo.NewClient(append(opts, o.ExtraOpts...)...)
	if err != nil { // coverage-ignore: only invalid static options fail here
		return nil, err
	}
	c := &Client{o: o, cl: cl, snaps: map[string]events.ControlSnapshot{}, changed: make(chan struct{}),
		timers: map[string]*time.Timer{}, done: make(chan struct{})}
	o.Metrics.GetOrCreateGauge("lab_control_generation", func() float64 {
		s, _ := c.Snapshot()
		return float64(s.Generation)
	})
	if err := c.catchUp(ctx); err != nil {
		cl.Close()
		return nil, err
	}
	go c.loop()
	return c, nil
}

func (c *Client) catchUp(ctx context.Context) error {
	ends, err := kadm.NewClient(c.cl).ListEndOffsets(ctx, contracts.TopicPlatformControl)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return fmt.Errorf("control topic end offsets: %w", err)
	}
	pending := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			pending[o.Partition] = o.Offset
		}
	})
	for len(pending) > 0 {
		fs := c.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return fmt.Errorf("control topic catch-up: %w", ctx.Err())
		}
		fs.EachRecord(func(r *kgo.Record) {
			c.apply(r)
			if end, ok := pending[r.Partition]; ok && r.Offset+1 >= end {
				delete(pending, r.Partition)
			}
		})
	}
	return nil
}

func (c *Client) loop() {
	defer close(c.done)
	for {
		fs := c.cl.PollFetches(context.Background())
		if fs.IsClientClosed() {
			return
		}
		fs.EachRecord(c.apply)
	}
}

func (c *Client) apply(r *kgo.Record) {
	key := string(r.Key)
	if c.o.Service != "" && key != c.o.Service {
		return
	}
	v, err := c.o.Decoder.Decode(r.Value)
	snap, ok := v.(*events.ControlSnapshot)
	if err != nil || !ok {
		c.o.Log.Warn("ignoring undecodable control record", "offset", r.Offset, "error", err)
		return
	}
	c.mu.Lock()
	if prev, seen := c.snaps[key]; seen && prev.Generation >= snap.Generation {
		c.mu.Unlock()
		return
	}
	c.snaps[key] = *snap
	if t := c.timers[key]; t != nil {
		t.Stop()
	}
	if snap.ExpiresAt != nil {
		// Wake listeners when the snapshot expires so knobs fall back to baseline on their own.
		c.timers[key] = time.AfterFunc(max(snap.ExpiresAt.Sub(c.o.Now()), 0), c.notify)
	}
	c.mu.Unlock()
	c.o.Log.Info("control snapshot applied", "service", key, "generation", snap.Generation, "experiments", snap.Experiments)
	c.exportKnobs()
	c.notify()
}

func (c *Client) notify() {
	c.mu.Lock()
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
}

// Changed returns a channel that is closed on the next change (new snapshot or expiry).
func (c *Client) Changed() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.changed
}

// Snapshot returns the tracked service's latest snapshot.
func (c *Client) Snapshot() (events.ControlSnapshot, bool) {
	return c.SnapshotOf(c.o.Service)
}

// SnapshotOf returns the latest snapshot of a service.
func (c *Client) SnapshotOf(service string) (events.ControlSnapshot, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.snaps[service]
	return s, ok
}

// Knobs returns the knobs in force now for the tracked service: baseline (zero value) when no
// snapshot exists or it has expired.
func (c *Client) Knobs() events.Knobs { return c.KnobsOf(c.o.Service) }

// KnobsOf returns the knobs in force now for a service.
func (c *Client) KnobsOf(service string) events.Knobs {
	s, ok := c.SnapshotOf(service)
	if !ok || (s.ExpiresAt != nil && !c.o.Now().Before(*s.ExpiresAt)) {
		return events.Knobs{}
	}
	return s.Knobs
}

func (c *Client) exportKnobs() {
	if c.o.Service == "" {
		return
	}
	k := c.Knobs()
	for name, v := range map[string]float64{
		"processingLatencyMs":        float64(k.ProcessingLatencyMs),
		"processingErrorProbability": k.ProcessingErrorProbability,
		"dbLatencyMs":                float64(k.DbLatencyMs),
		"netLatencyMs":               float64(k.NetLatencyMs),
		"netDropProbability":         k.NetDropProbability,
		"paused":                     b2f(k.Paused),
		"providerDown":               b2f(k.ProviderDown),
		"warehouseOutage":            b2f(k.WarehouseOutage),
		"paddingBytes":               float64(k.PaddingBytes),
		"duplicateProbability":       k.DuplicateProbability,
		"poisonProbability":          k.PoisonProbability,
	} {
		c.o.Metrics.GetOrCreateGauge(kafka.Label("lab_control_knob", "knob", name), nil).Set(v)
	}
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Close stops the client.
func (c *Client) Close() {
	c.cl.Close()
	<-c.done
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, t := range c.timers {
		t.Stop()
	}
}

// Encoder encodes control snapshots (implemented by *serde.Serde).
type Encoder interface {
	Encode(any) ([]byte, error)
}

// Publish writes a snapshot for snap.Service and waits for the broker acknowledgement.
// Only the controlboard publishes; the record key is the service name.
func Publish(ctx context.Context, cl *kgo.Client, enc Encoder, snap *events.ControlSnapshot) error {
	if snap.Service == "" {
		return errors.New("control snapshot without service")
	}
	b, err := enc.Encode(snap)
	if err != nil {
		return err
	}
	return cl.ProduceSync(ctx, &kgo.Record{Topic: contracts.TopicPlatformControl, Key: []byte(snap.Service), Value: b}).FirstErr()
}
