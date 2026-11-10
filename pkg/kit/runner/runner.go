// Package runner is the consumer loop every lab service uses. It implements the delivery
// contract of docs/contracts/events.md: one PostgreSQL transaction per partition batch,
// idempotent processing via processed_events, synchronous produce, offsets committed only
// after the outputs were acknowledged, endless retries for infrastructure errors and the
// dead-letter topic for records that can never be processed.
package runner

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/trace"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/serde"
)

// Migrations creates the processed_events table. Services apply it before their own migrations.
//
//go:embed migrations/*.sql
var migrations embed.FS

// Migrations returns the kit's migrations (apply with pg.DB.Migrate).
func Migrations() embed.FS { return migrations }

// Input is one decoded record handed to a Handler.
type Input struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     any // pointer to a generated event type
	Meta      events.EventMeta
}

// Output is an event the handler wants published.
type Output struct {
	Topic string
	Key   string
	Value any // pointer to a generated event type
}

// Handler holds a service's business logic. It runs inside the batch transaction; state
// changes made through tx commit together with the processed-event marker.
type Handler interface {
	Handle(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error)

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, tx pgx.Tx, in Input) ([]Output, error) {
	return f(ctx, tx, in)
}

// Codec encodes and decodes events (implemented by *serde.Serde).
type Codec interface {
	Encode(any) ([]byte, error)
	Decode([]byte) (any, error)
}

// Knobs provides the control-plane state (implemented by *control.Client).
type Knobs interface {
	Knobs() events.Knobs
	Changed() <-chan struct{}
}

// Config configures a Runner.
type Config struct {
	Service string
	Group   string // consumer group; defaults to Service
	Topics  []string
	Brokers []string
	DB      *pg.DB
	Codec   Codec
	Control Knobs
	Handler Handler
	Metrics *metrics.Set
	Log     *slog.Logger
	Hooks   []kgo.Hook
	Tracer  *kotel.Tracer
	// Dialer replaces the network dialer (fault injection for Network Chaos).
	Dialer func(ctx context.Context, network, host string) (net.Conn, error)
	// ExtraOpts are appended to the client options (tests).
	ExtraOpts []kgo.Opt
	// DedupWindow is how long processed event ids are kept (default 1 h).
	DedupWindow time.Duration
	// JanitorEvery is how often old processed event ids are purged (default 1 min).
	JanitorEvery time.Duration

	Backoff func(attempt int) time.Duration
	Random  func() float64
	Sleep   func(ctx context.Context, d time.Duration) error
	Now     func() time.Time
}

// Runner consumes, processes and publishes. Create it with New.
type Runner struct {
	cfg     Config
	current atomic.Pointer[kgo.Client]
	mu      sync.Mutex
	owned   map[string]map[int32]bool
	ready   atomic.Bool
	m       runnerMetrics
}

type runnerMetrics struct {
	set      *metrics.Set
	paused   *metrics.Gauge
	rebuilds *metrics.Counter
}

var errRebuild = errors.New("client settings changed")

// New returns a runner with defaults applied.
func New(cfg Config) *Runner {
	if cfg.Group == "" {
		cfg.Group = cfg.Service
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.NewSet()
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.DedupWindow == 0 {
		cfg.DedupWindow = time.Hour
	}
	if cfg.JanitorEvery == 0 {
		cfg.JanitorEvery = time.Minute
	}
	if cfg.Backoff == nil {
		cfg.Backoff = func(attempt int) time.Duration {
			return min(time.Duration(attempt)*250*time.Millisecond, 5*time.Second)
		}
	}
	if cfg.Random == nil {
		cfg.Random = rand.Float64
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepCtx
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	r := &Runner{cfg: cfg, owned: map[string]map[int32]bool{}}
	r.m = runnerMetrics{
		set:      cfg.Metrics,
		paused:   cfg.Metrics.GetOrCreateGauge("lab_consumer_paused", nil),
		rebuilds: cfg.Metrics.GetOrCreateCounter("lab_client_rebuilds_total"),
	}
	return r
}

// Client returns the current Kafka client (nil while it is being rebuilt).
func (r *Runner) Client() *kgo.Client { return r.current.Load() }

// Ready reports whether the runner has a client and is consuming.
func (r *Runner) Ready() bool { return r.ready.Load() }

// Run consumes until ctx ends. A Tuning Lab change rebuilds the client in place:
// outstanding work is committed, the group is left and rejoined with the new settings.
func (r *Runner) Run(ctx context.Context) error {
	go r.janitor(ctx)
	first := true
	for {
		tuning := r.cfg.Control.Knobs().Client
		cl, maxPoll, err := r.newClient(tuning)
		if err != nil { // coverage-ignore: only invalid static options fail here
			return err
		}
		if !first {
			r.m.rebuilds.Inc()
			r.cfg.Log.InfoContext(ctx, "kafka client rebuilt with new tuning")
		}
		first = false
		r.current.Store(cl)
		r.ready.Store(true)
		err = r.consume(ctx, cl, tuning, maxPoll)
		r.ready.Store(false)
		r.current.Store(nil)
		r.closeClient(cl)
		if !errors.Is(err, errRebuild) {
			return err
		}
	}
}

func (r *Runner) newClient(t events.ClientTuning) (*kgo.Client, int, error) {
	tuningOpts, maxPoll, lossy := kafka.TuningOptions(t)
	if lossy {
		r.cfg.Log.Warn("Tuning Lab lowered acks: idempotent writes are off, outputs may be lost or duplicated")
	}
	opts := append(kafka.BaseOptions(r.cfg.Brokers, r.cfg.Service, r.cfg.Hooks...),
		kgo.ConsumerGroup(r.cfg.Group),
		kgo.ConsumeTopics(r.cfg.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(2*time.Second),
		kgo.OnPartitionsAssigned(r.onAssigned),
		kgo.OnPartitionsRevoked(r.onRevoked("revoked")),
		kgo.OnPartitionsLost(r.onRevoked("lost")),
	)
	if r.cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(r.cfg.Dialer))
	}
	opts = append(append(opts, tuningOpts...), r.cfg.ExtraOpts...)
	cl, err := kgo.NewClient(opts...)
	return cl, maxPoll, err
}

func (r *Runner) closeClient(cl *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.CommitMarkedOffsets(ctx); err != nil { // coverage-ignore: log only; uncommitted work is redelivered and deduplicated
		r.cfg.Log.Warn("final offset commit failed", "error", err)
	}
	cl.Close()
}

func (r *Runner) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for topic, parts := range assigned {
		if r.owned[topic] == nil {
			r.owned[topic] = map[int32]bool{}
		}
		for _, p := range parts {
			r.owned[topic][p] = true
		}
		r.exportOwned(topic)
	}
	r.m.set.GetOrCreateCounter(kafka.Label("lab_rebalances_total", "kind", "assigned")).Inc()
}

func (r *Runner) onRevoked(kind string) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
		r.mu.Lock()
		for topic, parts := range revoked {
			for _, p := range parts {
				delete(r.owned[topic], p)
			}
			r.exportOwned(topic)
		}
		r.mu.Unlock()
		r.m.set.GetOrCreateCounter(kafka.Label("lab_rebalances_total", "kind", kind)).Inc()
		if kind == "revoked" {
			if err := cl.CommitMarkedOffsets(ctx); err != nil { // coverage-ignore: log only; uncommitted work is redelivered and deduplicated
				r.cfg.Log.Warn("commit on revoke failed", "error", err)
			}
		}
	}
}

func (r *Runner) exportOwned(topic string) {
	r.m.set.GetOrCreateGauge(kafka.Label("lab_partitions_assigned", "topic", topic), nil).Set(float64(len(r.owned[topic])))
}

func (r *Runner) owns(topic string, partition int32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.owned[topic][partition]
}

func (r *Runner) consume(ctx context.Context, cl *kgo.Client, tuning events.ClientTuning, maxPoll int) error {
	paused := false
	for {
		changed := r.cfg.Control.Changed()
		knobs := r.cfg.Control.Knobs()
		if !kafka.SameTuning(knobs.Client, tuning) {
			return errRebuild
		}
		if knobs.Paused != paused {
			paused = knobs.Paused
			if paused {
				cl.PauseFetchTopics(r.cfg.Topics...)
				r.m.paused.Set(1)
				r.cfg.Log.WarnContext(ctx, "consumption paused by control plane (process stays healthy)")
			} else {
				cl.ResumeFetchTopics(r.cfg.Topics...)
				r.m.paused.Set(0)
				r.cfg.Log.InfoContext(ctx, "consumption resumed")
			}
		}
		if paused {
			select {
			case <-ctx.Done():
				return nil
			case <-changed:
			}
			continue
		}
		pctx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-changed:
				cancel()
			case <-pctx.Done():
			}
		}()
		fs := cl.PollRecords(pctx, maxPoll)
		cancel()
		if fs.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		fs.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) { // coverage-ignore: log only; the client retries fetches itself
				r.cfg.Log.WarnContext(ctx, "fetch error", "topic", topic, "partition", p, "error", err)
			}
		})
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) > 0 {
				r.processPartition(ctx, cl, p.Topic, p.Partition, p.Records)
			}
		})
	}
}

type outRecord struct {
	Topic   string            `json:"t"`
	Key     string            `json:"k"`
	Value   []byte            `json:"v"`
	Headers map[string]string `json:"h,omitempty"`
	ctx     context.Context
}

type batchStats struct {
	ok, duplicate, dlq int
	dlqReasons         map[string]int
}

func (r *Runner) processPartition(ctx context.Context, cl *kgo.Client, topic string, partition int32, recs []*kgo.Record) {
	start := r.cfg.Now()
	r.m.set.GetOrCreateCounter(kafka.Label("lab_records_consumed_total", "topic", topic)).Add(len(recs))
	for attempt := 1; ctx.Err() == nil; attempt++ {
		if !r.owns(topic, partition) {
			r.cfg.Log.InfoContext(ctx, "partition moved to another pod, dropping batch", "topic", topic, "partition", partition)
			return
		}
		hooks := &commitHooks{}
		outs, stats, err := r.processBatch(context.WithValue(ctx, commitKey{}, hooks), topic, recs)
		reason := "db"
		if err == nil {
			reason = "produce"
			err = r.produce(ctx, cl, outs)
		} else if errors.Is(err, ErrInjected) {
			reason = "transient"
		} else if errors.Is(err, errEncode) {
			reason = "registry"
		}
		if err == nil {
			hooks.run()
			cl.MarkCommitRecords(recs[len(recs)-1])
			r.record(topic, stats, start)
			return
		}
		if ctx.Err() != nil {
			return
		}
		r.m.set.GetOrCreateCounter(kafka.Label("lab_processing_retries_total", "topic", topic, "reason", reason)).Inc()
		if attempt == 1 || attempt%20 == 0 {
			r.cfg.Log.WarnContext(ctx, "batch failed, retrying", "topic", topic, "partition", partition, "attempt", attempt, "reason", reason, "error", err)
		}
		_ = r.cfg.Sleep(ctx, r.cfg.Backoff(attempt))
	}
}

func (r *Runner) record(topic string, s batchStats, start time.Time) {
	set := r.m.set
	set.GetOrCreateCounter(kafka.Label("lab_records_processed_total", "topic", topic, "result", "ok")).Add(s.ok)
	set.GetOrCreateCounter(kafka.Label("lab_records_processed_total", "topic", topic, "result", "duplicate")).Add(s.duplicate)
	set.GetOrCreateCounter(kafka.Label("lab_records_processed_total", "topic", topic, "result", "dlq")).Add(s.dlq)
	for reason, n := range s.dlqReasons {
		set.GetOrCreateCounter(kafka.Label("lab_dlq_records_total", "topic", topic, "reason", reason)).Add(n)
	}
	set.GetOrCreateHistogram(kafka.Label("lab_batch_duration_seconds", "topic", topic)).Update(r.cfg.Now().Sub(start).Seconds())
}

func (r *Runner) processBatch(ctx context.Context, topic string, recs []*kgo.Record) ([]outRecord, batchStats, error) {
	var outs []outRecord
	stats := batchStats{dlqReasons: map[string]int{}}
	err := r.cfg.DB.InTx(ctx, func(tx pgx.Tx) error {
		outs, stats = nil, batchStats{dlqReasons: map[string]int{}}
		for _, rec := range recs {
			o, result, err := r.processRecord(ctx, tx, topic, rec)
			if err != nil {
				return err
			}
			switch result {
			case "duplicate":
				stats.duplicate++
			case "ok":
				stats.ok++
			default:
				stats.dlq++
				stats.dlqReasons[result]++
			}
			outs = append(outs, o...)
		}
		return nil
	})
	return outs, stats, err
}

var errEncode = errors.New("encode output")

// processRecord returns the outputs and the result: "ok", "duplicate" or a DLQ reason.
func (r *Runner) processRecord(ctx context.Context, tx pgx.Tx, topic string, rec *kgo.Record) ([]outRecord, string, error) {
	v, err := r.cfg.Codec.Decode(rec.Value)
	meta, isEvent := serde.Meta(v)
	reason := "undecodable"
	if err == nil && !isEvent {
		err = fmt.Errorf("%T is not a business event", v)
	} else if err == nil && !expectedType(topic, v) {
		reason, err = "unexpected-type", fmt.Errorf("%T does not belong on %s", v, topic)
	}
	if err != nil {
		id := fmt.Sprintf("dlq:%s:%d:%d", topic, rec.Partition, rec.Offset)
		return r.once(ctx, tx, id, topic, reason, func() ([]outRecord, string, error) {
			return []outRecord{r.dlqRecord(rec, reason, err)}, reason, nil
		})
	}
	return r.once(ctx, tx, meta.EventID, topic, "ok", func() ([]outRecord, string, error) {
		pctx := ctx
		if r.cfg.Tracer != nil {
			var span trace.Span
			pctx, span = r.cfg.Tracer.WithProcessSpan(rec)
			defer span.End()
		}
		knobs := r.cfg.Control.Knobs()
		if d := time.Duration(knobs.ProcessingLatencyMs) * time.Millisecond; d > 0 || knobs.ProcessingJitterMs > 0 {
			d += time.Duration(r.cfg.Random()*float64(knobs.ProcessingJitterMs)) * time.Millisecond
			if err := r.cfg.Sleep(ctx, d); err != nil {
				return nil, "", err
			}
		}
		if p := knobs.ProcessingErrorProbability; p > 0 && r.cfg.Random() < p {
			return nil, "", ErrInjected
		}
		r.m.set.GetOrCreateHistogram(kafka.Label("lab_event_age_seconds", "topic", topic)).
			Update(max(r.cfg.Now().Sub(meta.OccurredAt).Seconds(), 0))
		start := r.cfg.Now()
		sp, err := tx.Begin(ctx)
		if err != nil { // coverage-ignore: connection loss mid-transaction; the batch is retried
			return nil, "", err
		}
		outs, err := r.cfg.Handler.Handle(pctx, sp, Input{
			Topic: topic, Partition: rec.Partition, Offset: rec.Offset, Key: string(rec.Key), Value: v, Meta: meta,
		})
		r.m.set.GetOrCreateHistogram(kafka.Label("lab_processing_duration_seconds", "topic", topic)).Update(r.cfg.Now().Sub(start).Seconds())
		if IsPermanent(err) {
			_ = sp.Rollback(ctx)
			return []outRecord{r.dlqRecord(rec, "rejected", err)}, "rejected", nil
		}
		if err != nil {
			return nil, "", err
		}
		if err := sp.Commit(ctx); err != nil { // coverage-ignore: connection loss mid-transaction; the batch is retried
			return nil, "", err
		}
		encoded := make([]outRecord, 0, len(outs))
		for _, o := range outs {
			b, err := r.cfg.Codec.Encode(o.Value)
			if err != nil {
				return nil, "", fmt.Errorf("%w %s: %v", errEncode, o.Topic, err)
			}
			encoded = append(encoded, outRecord{Topic: o.Topic, Key: o.Key, Value: b, ctx: pctx})
		}
		return encoded, "ok", nil
	})
}

// once runs fn only the first time id is seen. A repeated id returns the stored outputs, so a
// redelivered input re-publishes exactly what the first delivery produced.
func (r *Runner) once(ctx context.Context, tx pgx.Tx, id, topic, firstResult string, fn func() ([]outRecord, string, error)) ([]outRecord, string, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO processed_events(event_id, topic) VALUES ($1, $2) ON CONFLICT DO NOTHING`, id, topic)
	if err != nil { // coverage-ignore: connection loss mid-transaction; the batch is retried
		return nil, "", err
	}
	if tag.RowsAffected() == 0 {
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT outputs FROM processed_events WHERE event_id = $1`, id).Scan(&raw); err != nil { // coverage-ignore: connection loss mid-transaction
			return nil, "", err
		}
		var stored []outRecord
		if err := json.Unmarshal(raw, &stored); err != nil { // coverage-ignore: the column is only written by this function
			return nil, "", err
		}
		for i := range stored {
			stored[i].ctx = ctx
		}
		return stored, "duplicate", nil
	}
	outs, result, err := fn()
	if err != nil {
		return nil, "", err
	}
	raw, _ := json.Marshal(outs)
	if _, err := tx.Exec(ctx, `UPDATE processed_events SET outputs = $2 WHERE event_id = $1`, id, raw); err != nil { // coverage-ignore: connection loss mid-transaction
		return nil, "", err
	}
	return outs, result, nil
}

func (r *Runner) dlqRecord(rec *kgo.Record, reason string, cause error) outRecord {
	return outRecord{
		Topic: contracts.TopicOrdersDLQ, Key: string(rec.Key), Value: rec.Value,
		Headers: map[string]string{
			contracts.HeaderDLQSourceTopic:     rec.Topic,
			contracts.HeaderDLQSourcePartition: strconv.Itoa(int(rec.Partition)),
			contracts.HeaderDLQSourceOffset:    strconv.FormatInt(rec.Offset, 10),
			contracts.HeaderDLQReason:          reason,
			contracts.HeaderDLQError:           cause.Error(),
			contracts.HeaderDLQConsumer:        r.cfg.Service,
		},
	}
}

func (r *Runner) produce(ctx context.Context, cl *kgo.Client, outs []outRecord) error {
	if len(outs) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(outs))
	for i, o := range outs {
		rec := &kgo.Record{Topic: o.Topic, Key: []byte(o.Key), Value: o.Value, Context: o.ctx,
			Headers: []kgo.RecordHeader{{Key: contracts.HeaderProducer, Value: []byte(r.cfg.Service)}}}
		if rec.Context == nil {
			rec.Context = ctx
		}
		for k, v := range o.Headers {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		recs[i] = rec
	}
	results := cl.ProduceSync(ctx, recs...)
	if err := results.FirstErr(); err != nil {
		return err
	}
	for _, o := range outs {
		r.m.set.GetOrCreateCounter(kafka.Label("lab_records_produced_total", "topic", o.Topic)).Inc()
	}
	return nil
}

// janitor forgets processed event ids older than the dedup window, in small batches.
func (r *Runner) janitor(ctx context.Context) {
	for sleepCtx(ctx, r.cfg.JanitorEvery) == nil {
		r.Purge(ctx)
	}
}

// Purge deletes processed event ids older than the dedup window and returns how many.
func (r *Runner) Purge(ctx context.Context) int64 {
	var total int64
	for {
		tag, err := r.cfg.DB.Pool.Exec(ctx, `DELETE FROM processed_events WHERE event_id IN (
			SELECT event_id FROM processed_events WHERE processed_at < $1 LIMIT 5000)`, r.cfg.Now().Add(-r.cfg.DedupWindow))
		if err != nil {
			r.cfg.Log.WarnContext(ctx, "purging processed events failed", "error", err)
			return total
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < 5000 {
			return total
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// expectedType reports whether v is the value type the contract defines for topic.
// Topics outside the contract accept any business event.
func expectedType(topic string, v any) bool {
	name, ok := contracts.ValueTypes[topic]
	if !ok {
		return true
	}
	return reflect.TypeOf(serde.Prototypes[name]()) == reflect.TypeOf(v)
}
