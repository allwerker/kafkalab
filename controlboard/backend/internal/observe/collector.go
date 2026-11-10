package observe

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

// Queries are the fixed MetricsQL expressions the board evaluates every refresh. Fallbacks
// (`or`) keep the board useful before the analytics service or KMinion are installed.
var Queries = struct {
	OrdersPerSec, RevenuePerMin, PaymentSuccessRate, ReservationSuccessRate, E2EP95Ms, InFlight, DlqPerMin, TotalLag string
	BrokerUp, BrokerLeaders, BrokerController, BrokerUnderReplicated                                                 string
	SpecReplicas, ReadyReplicas, AppliedGeneration                                                                   string
}{
	OrdersPerSec:  `sum(rate(lab_orders_created_total[1m])) or sum(rate(lab_generator_orders_total{kind!~"poison|duplicate"}[1m]))`,
	RevenuePerMin: `sum(rate(lab_revenue_cents_total[1m])) * 60 / 100`,
	PaymentSuccessRate: `(sum(rate(lab_payments_total{result="completed"}[1m])) / sum(rate(lab_payments_total[1m])))` +
		` or (sum(rate(lab_records_produced_total{topic="payments.completed"}[1m])) / sum(rate(lab_records_produced_total{topic=~"payments.completed|payments.failed"}[1m])))`,
	ReservationSuccessRate: `(sum(rate(lab_reservations_total{result="reserved"}[1m])) / sum(rate(lab_reservations_total[1m])))` +
		` or (sum(rate(lab_records_produced_total{topic="inventory.reserved"}[1m])) / sum(rate(lab_records_produced_total{topic=~"inventory.reserved|inventory.rejected"}[1m])))`,
	E2EP95Ms: `histogram_quantile(0.95, sum(rate(lab_order_completion_seconds_bucket{outcome="shipped"}[1m])) by (vmrange)) * 1000`,
	InFlight: `clamp_min((sum(lab_orders_created_total) or vector(0)) - (sum(lab_orders_shipped_total) or vector(0))` +
		` - (sum(lab_orders_cancelled_total) or vector(0)), 0)`,
	DlqPerMin: `(sum(rate(lab_dlq_total[1m])) or sum(rate(lab_dlq_records_total[1m]))) * 60`,
	TotalLag:  `sum(kminion_kafka_consumer_group_topic_lag{group=~"payment|inventory|shipping|notification|analytics"})`,

	BrokerUp:              `max by (kafka_broker, pod) (up{namespace="kafka", pod=~"kafka-lab-.*"})`,
	BrokerLeaders:         `max by (kafka_broker, pod) (kafka_server_replicamanager_leadercount)`,
	BrokerController:      `max by (kafka_broker, pod) (kafka_controller_kafkacontroller_activecontrollercount)`,
	BrokerUnderReplicated: `max by (kafka_broker, pod) (kafka_server_replicamanager_underreplicatedpartitions)`,

	SpecReplicas:      `max by (deployment) (kube_deployment_spec_replicas{namespace="commerce"})`,
	ReadyReplicas:     `max by (deployment) (kube_deployment_status_replicas_ready{namespace="commerce"})`,
	AppliedGeneration: `min by (service) (lab_control_generation{namespace="commerce"})`,
}

// ServiceObs is what kube-state-metrics and the service pods report for one service.
type ServiceObs struct {
	Known             bool // kube-state-metrics knows the deployment
	Replicas          int
	ReadyReplicas     int
	AppliedGeneration int64
}

// Snapshot is everything observed in one refresh.
type Snapshot struct {
	At       time.Time
	Kpis     api.Kpis
	Brokers  []api.BrokerState
	Services map[string]ServiceObs
	Lag      Lag
}

// LagSource returns consumer lag (AdminLag in production).
type LagSource interface {
	Lag(ctx context.Context) (Lag, error)
}

// Options configures NewCollector.
type Options struct {
	VM      VM
	Lag     LagSource
	Brokers int // number of Kafka nodes (ids 0..Brokers-1)
	Log     *slog.Logger
	Now     func() time.Time
}

// Collector refreshes the snapshot periodically. A failing source keeps its previous values.
type Collector struct {
	o    Options
	mu   sync.RWMutex
	snap Snapshot
}

// NewCollector returns a collector whose snapshot lists every broker as down until the first refresh.
func NewCollector(o Options) *Collector {
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	c := &Collector{o: o}
	c.snap = Snapshot{Brokers: brokers(o.Brokers), Services: map[string]ServiceObs{},
		Lag: Lag{ByGroup: map[string]map[string][]int64{}, Partitions: map[string]int{}}}
	return c
}

func brokers(n int) []api.BrokerState {
	b := make([]api.BrokerState, n)
	for i := range b {
		b[i].ID = i
	}
	return b
}

// Run refreshes every interval until ctx ends.
func (c *Collector) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		c.Refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Refresh queries VictoriaMetrics and the lag source once.
func (c *Collector) Refresh(ctx context.Context) {
	c.mu.RLock()
	next := c.snap
	c.mu.RUnlock()
	next.At = c.o.Now().UTC()
	if l, err := c.o.Lag.Lag(ctx); err != nil {
		c.o.Log.WarnContext(ctx, "consumer lag unavailable", "error", err)
	} else {
		next.Lag = l
	}
	if err := c.metrics(ctx, &next); err != nil {
		c.o.Log.WarnContext(ctx, "victoriametrics query failed", "error", err)
	}
	if next.Kpis.TotalLag < 0 {
		next.Kpis.TotalLag = float64(next.Lag.Total) // KMinion not installed: use the admin API
	}
	c.mu.Lock()
	c.snap = next
	c.mu.Unlock()
}

// metrics fills the VictoriaMetrics part of s; on any error s keeps the previous values.
// A missing KMinion leaves s.Kpis.TotalLag at -1.
func (c *Collector) metrics(ctx context.Context, s *Snapshot) error {
	q := Queries
	var k api.Kpis
	for _, f := range []struct {
		expr string
		dst  *float64
	}{
		{q.OrdersPerSec, &k.OrdersPerSec}, {q.RevenuePerMin, &k.RevenuePerMin},
		{q.PaymentSuccessRate, &k.PaymentSuccessRate}, {q.ReservationSuccessRate, &k.ReservationSuccessRate},
		{q.E2EP95Ms, &k.E2EP95Ms}, {q.InFlight, &k.InFlight}, {q.DlqPerMin, &k.DlqPerMin},
	} {
		v, _, err := c.o.VM.scalar(ctx, f.expr)
		if err != nil {
			return err
		}
		*f.dst = v
	}
	lag, ok, err := c.o.VM.scalar(ctx, q.TotalLag)
	if err != nil {
		return err
	}
	k.TotalLag = -1
	if ok {
		k.TotalLag = lag
	}

	brokerSeries := map[string]map[int]float64{}
	for _, expr := range []string{q.BrokerUp, q.BrokerLeaders, q.BrokerController, q.BrokerUnderReplicated} {
		samples, err := c.o.VM.Query(ctx, expr)
		if err != nil {
			return err
		}
		brokerSeries[expr] = map[int]float64{}
		for _, smp := range samples {
			if id, ok := brokerID(smp.Labels); ok {
				brokerSeries[expr][id] = smp.Value
			}
		}
	}
	bs := brokers(c.o.Brokers)
	for i := range bs {
		bs[i].Up = brokerSeries[q.BrokerUp][i] == 1
		bs[i].LeaderCount = int(brokerSeries[q.BrokerLeaders][i])
		bs[i].IsController = bs[i].Up && brokerSeries[q.BrokerController][i] == 1
		bs[i].UnderReplicated = int(brokerSeries[q.BrokerUnderReplicated][i])
	}

	byLabel := map[string]map[string]float64{}
	for _, f := range []struct{ expr, label string }{
		{q.SpecReplicas, "deployment"}, {q.ReadyReplicas, "deployment"}, {q.AppliedGeneration, "service"},
	} {
		samples, err := c.o.VM.Query(ctx, f.expr)
		if err != nil {
			return err
		}
		byLabel[f.expr] = map[string]float64{}
		for _, smp := range samples {
			byLabel[f.expr][smp.Labels[f.label]] = smp.Value
		}
	}
	svcs := map[string]ServiceObs{}
	for name, spec := range byLabel[q.SpecReplicas] {
		svcs[name] = ServiceObs{Known: true, Replicas: int(spec), ReadyReplicas: int(byLabel[q.ReadyReplicas][name]),
			AppliedGeneration: int64(byLabel[q.AppliedGeneration][name])}
	}
	s.Kpis, s.Brokers, s.Services = k, bs, svcs
	return nil
}

// brokerID reads the node id from kafka_broker, else from the pod name suffix (kafka-lab-dual-role-N).
func brokerID(l map[string]string) (int, bool) {
	if id, err := strconv.Atoi(l["kafka_broker"]); err == nil {
		return id, true
	}
	pod := l["pod"]
	id, err := strconv.Atoi(pod[strings.LastIndex(pod, "-")+1:])
	return id, err == nil
}

// Snapshot returns a copy of the latest snapshot.
func (c *Collector) Snapshot() Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	s := c.snap
	s.Brokers = slices.Clone(s.Brokers)
	s.Services = maps.Clone(s.Services)
	s.Lag.ByGroup = maps.Clone(s.Lag.ByGroup)
	s.Lag.Partitions = maps.Clone(s.Lag.Partitions)
	return s
}
