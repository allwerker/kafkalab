// Package feed is the board's live view of the business: a consumer without group that reads
// every business topic from its end, turns records into LiveEvents, measures per-topic rates,
// samples events for the WebSocket and keeps the recent orders for GET /api/orders/{id}.
package feed

import (
	"context"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/grafana"
	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/serde"
)

// Defaults of Options.
const (
	DefaultOrders      = 5000 // orders kept for the order timeline
	DefaultSampleSize  = 40   // items per "events" message
	maxEventsPerOrder  = 50
	rateWindowSeconds  = 3 // rates average the last 3 complete seconds
	rateBuckets        = rateWindowSeconds + 2
	traceparentVersion = "00"
)

// Topics are the topics the feed reads: every business topic plus the dead-letter topic.
var Topics = append(slices.Clone(contracts.BusinessTopics), contracts.TopicOrdersDLQ)

// Decoder decodes record values (implemented by *serde.Serde).
type Decoder interface {
	Decode([]byte) (any, error)
}

// Options configures New.
type Options struct {
	Decoder    Decoder
	Grafana    grafana.Links
	Orders     int // ring size, default DefaultOrders
	SampleSize int // default DefaultSampleSize
	Now        func() time.Time
}

// Feed aggregates the live business events. All methods are safe for concurrent use.
type Feed struct {
	o   Options
	rng *rand.Rand

	mu     sync.Mutex
	rates  map[string]*window
	seen   int
	sample []api.LiveEvent
	orders map[string]*order
	ring   []string // order ids, oldest at next
	next   int
}

type order struct {
	traceID string
	events  []api.LiveEvent
}

// New returns an empty feed.
func New(o Options) *Feed {
	if o.Orders <= 0 {
		o.Orders = DefaultOrders
	}
	if o.SampleSize <= 0 {
		o.SampleSize = DefaultSampleSize
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	f := &Feed{o: o, rng: rand.New(rand.NewPCG(1, 2)), rates: map[string]*window{}, orders: map[string]*order{},
		ring: make([]string, 0, o.Orders)}
	for _, t := range Topics {
		f.rates[t] = &window{}
	}
	return f
}

// ClientOptions returns the options of the feed's consumer: no group, every feed topic,
// starting at the end (the board shows what happens now, not history).
func ClientOptions(brokers []string, hooks ...kgo.Hook) []kgo.Opt {
	return append(kafka.BaseOptions(brokers, contracts.ServiceControlboard+"-feed", hooks...),
		kgo.ConsumeTopics(Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
}

// Run polls cl until ctx ends or the client is closed.
func (f *Feed) Run(ctx context.Context, cl *kgo.Client) error {
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fs.IsClientClosed() {
			return nil
		}
		fs.EachRecord(f.Ingest)
	}
}

// Ingest processes one record.
func (f *Feed) Ingest(r *kgo.Record) {
	ev, ok := f.toLive(r)
	now := f.o.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	if w := f.rates[r.Topic]; w != nil {
		w.add(now.Unix())
	}
	if !ok {
		return
	}
	f.seen++
	if len(f.sample) < f.o.SampleSize {
		f.sample = append(f.sample, ev)
	} else if j := f.rng.IntN(f.seen); j < f.o.SampleSize {
		f.sample[j] = ev // reservoir sampling: every event of the interval is equally likely
	}
	f.remember(ev, traceID(r))
}

func (f *Feed) toLive(r *kgo.Record) (api.LiveEvent, bool) {
	ev := api.LiveEvent{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, OrderID: string(r.Key), At: r.Timestamp.UTC()}
	if r.Topic == contracts.TopicOrdersDLQ {
		ev.Kind = api.EventDLQ
		ev.Reason = header(r, contracts.HeaderDLQReason)
		return ev, true
	}
	v, err := f.o.Decoder.Decode(r.Value)
	if err != nil {
		return ev, false // poison record: counted in the rate, dead-lettered by the consumers
	}
	switch e := v.(type) {
	case *events.OrderCreated:
		ev.Kind, ev.ValueCents, ev.Region = api.EventCreated, e.TotalCents, string(e.Region)
	case *events.PaymentCompleted:
		ev.Kind, ev.ValueCents = api.EventPaid, e.AmountCents
	case *events.PaymentFailed:
		ev.Kind, ev.ValueCents, ev.Reason = api.EventPaymentFailed, e.AmountCents, string(e.Reason)
	case *events.PaymentRefunded:
		ev.Kind, ev.ValueCents = api.EventRefunded, e.AmountCents
	case *events.InventoryReserved:
		ev.Kind = api.EventReserved
	case *events.InventoryRejected:
		ev.Kind, ev.Reason = api.EventRejected, string(e.Reason)
	case *events.InventoryReleased:
		ev.Kind = api.EventReleased
	case *events.OrderShipped:
		ev.Kind, ev.Carrier = api.EventShipped, e.Carrier
	case *events.OrderCancelled:
		ev.Kind, ev.Reason = api.EventCancelled, string(e.Reason)
	case *events.NotificationSent:
		ev.Kind = api.EventNotified
	default:
		return ev, false // a platform event on a business topic: not part of the live view
	}
	m, _ := serde.Meta(v)
	ev.OrderID, ev.At = m.OrderID, m.OccurredAt.UTC()
	return ev, true
}

// remember adds ev to its order in the ring, evicting the oldest order when full.
func (f *Feed) remember(ev api.LiveEvent, trace string) {
	o, ok := f.orders[ev.OrderID]
	if !ok {
		if len(f.ring) < f.o.Orders {
			f.ring = append(f.ring, ev.OrderID)
		} else {
			delete(f.orders, f.ring[f.next])
			f.ring[f.next] = ev.OrderID
			f.next = (f.next + 1) % f.o.Orders
		}
		o = &order{}
		f.orders[ev.OrderID] = o
	}
	if o.traceID == "" {
		o.traceID = trace
	}
	if len(o.events) < maxEventsPerOrder {
		o.events = append(o.events, ev)
	}
}

// Rates returns records per second of every feed topic, averaged over the last complete seconds.
func (f *Feed) Rates() map[string]float64 {
	now := f.o.Now().Unix()
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]float64, len(f.rates))
	for t, w := range f.rates {
		out[t] = w.rate(now)
	}
	return out
}

// Drain returns the sample of events since the previous call; Scale is the number of real
// events each item stands for.
func (f *Feed) Drain() api.Events {
	f.mu.Lock()
	items, seen := f.sample, f.seen
	f.sample, f.seen = nil, 0
	f.mu.Unlock()
	if len(items) == 0 {
		return api.Events{Scale: 1, Items: []api.LiveEvent{}}
	}
	slices.SortStableFunc(items, func(a, b api.LiveEvent) int { return a.At.Compare(b.At) })
	return api.Events{Scale: float64(seen) / float64(len(items)), Items: items}
}

// Order returns the timeline of a recent order.
func (f *Feed) Order(id string) (api.OrderTimeline, bool) {
	f.mu.Lock()
	o, ok := f.orders[id]
	var evs []api.LiveEvent
	var trace string
	if ok {
		evs, trace = slices.Clone(o.events), o.traceID
	}
	f.mu.Unlock()
	if !ok {
		return api.OrderTimeline{}, false
	}
	slices.SortStableFunc(evs, func(a, b api.LiveEvent) int { return a.At.Compare(b.At) })
	t := api.OrderTimeline{OrderID: id, Events: evs, LogsURL: f.o.Grafana.OrderLogs(id)}
	if trace != "" {
		t.TraceID, t.TraceURL = trace, f.o.Grafana.Trace(trace)
	}
	return t, true
}

// window counts events per second in a few rotating buckets.
type window struct {
	secs   [rateBuckets]int64
	counts [rateBuckets]float64
}

func (w *window) add(sec int64) {
	i := sec % rateBuckets
	if w.secs[i] != sec {
		w.secs[i], w.counts[i] = sec, 0
	}
	w.counts[i]++
}

func (w *window) rate(now int64) float64 {
	sum := 0.0
	for i, s := range w.secs {
		if s < now && s >= now-rateWindowSeconds {
			sum += w.counts[i]
		}
	}
	return sum / rateWindowSeconds
}

func header(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// traceID extracts the trace id from a W3C traceparent header ("00-<trace>-<span>-<flags>").
func traceID(r *kgo.Record) string {
	parts := strings.Split(header(r, "traceparent"), "-")
	if len(parts) != 4 || parts[0] != traceparentVersion || len(parts[1]) != 32 || strings.Trim(parts[1], "0") == "" {
		return ""
	}
	return parts[1]
}
