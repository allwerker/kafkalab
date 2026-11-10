package observe

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/testkit"
)

type series struct {
	labels map[string]string
	value  string
}

// fakeVM answers instant queries from a table; unknown queries return no series.
type fakeVM struct {
	mu     sync.Mutex
	result map[string][]series
	failOn string // this query fails with a VictoriaMetrics error
	broken bool   // every answer is not JSON
}

func (f *fakeVM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query().Get("query")
	if f.broken {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	if q == f.failOn {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": "cannot parse"})
		return
	}
	res := []map[string]any{}
	for _, s := range f.result[q] {
		res = append(res, map[string]any{"metric": s.labels, "value": []any{1794342792, s.value}})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "vector", "result": res}})
}

func (f *fakeVM) set(fn func(*fakeVM)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

func one(v string) []series { return []series{{labels: map[string]string{}, value: v}} }

func broker(id, v string) series {
	return series{labels: map[string]string{"kafka_broker": id, "pod": "kafka-lab-dual-role-" + id}, value: v}
}

// healthyLab is a three-node cluster with broker 1 as controller and two commerce services.
func healthyLab() *fakeVM {
	q := Queries
	return &fakeVM{result: map[string][]series{
		q.OrdersPerSec: one("50"), q.RevenuePerMin: one("2670"), q.PaymentSuccessRate: one("0.95"),
		q.ReservationSuccessRate: one("0.98"), q.E2EP95Ms: one("420"), q.InFlight: one("37"), q.DlqPerMin: one("NaN"),
		q.BrokerUp:              {broker("0", "1"), broker("1", "1"), {labels: map[string]string{"pod": "kafka-lab-dual-role-2"}, value: "1"}},
		q.BrokerLeaders:         {broker("0", "20"), broker("1", "20"), broker("2", "23"), {labels: map[string]string{"pod": "strimzi"}, value: "9"}},
		q.BrokerController:      {broker("0", "0"), broker("1", "1"), broker("2", "0")},
		q.BrokerUnderReplicated: {broker("0", "0"), broker("1", "0"), broker("2", "4")},
		q.SpecReplicas:          {{labels: map[string]string{"deployment": "payment"}, value: "2"}, {labels: map[string]string{"deployment": "order-generator"}, value: "1"}},
		q.ReadyReplicas:         {{labels: map[string]string{"deployment": "payment"}, value: "1"}, {labels: map[string]string{"deployment": "order-generator"}, value: "1"}},
		q.AppliedGeneration:     {{labels: map[string]string{"service": "payment"}, value: "7"}},
	}}
}

type fakeLag struct {
	mu  sync.Mutex
	lag Lag
	err error
}

func (f *fakeLag) Lag(context.Context) (Lag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lag, f.err
}

func collector(t *testing.T, vm http.Handler, lag LagSource) *Collector {
	t.Helper()
	srv := httptest.NewServer(vm)
	t.Cleanup(srv.Close)
	return NewCollector(Options{VM: VM{Base: srv.URL + "/", HTTP: srv.Client()}, Lag: lag, Brokers: 3})
}

var ctx = context.Background()

func Test_shouldReportKpisBrokersAndServicesWhenVictoriaMetricsAnswers(t *testing.T) {
	// GIVEN: a healthy lab without KMinion, payment lagging 12 records according to Kafka
	lag := &fakeLag{lag: Lag{Total: 12, ByGroup: map[string]map[string][]int64{"payment": {"orders.created": {12, 0, 0}}}, Partitions: map[string]int{"orders.created": 3}}}
	c := collector(t, healthyLab(), lag)
	before := c.Snapshot()

	// WHEN: the board refreshes
	c.Refresh(ctx)
	s := c.Snapshot()

	// THEN: before the first refresh every broker is down; afterwards the KPIs, brokers and services are known
	if len(before.Brokers) != 3 || before.Brokers[0].Up {
		t.Fatalf("before %+v", before.Brokers)
	}
	k := s.Kpis
	if k.OrdersPerSec != 50 || k.RevenuePerMin != 2670 || k.PaymentSuccessRate != 0.95 || k.ReservationSuccessRate != 0.98 || k.E2EP95Ms != 420 || k.InFlight != 37 {
		t.Fatalf("kpis %+v", k)
	}
	// AND: an undefined DLQ rate counts as zero and the lag comes from the admin API
	if k.DlqPerMin != 0 || k.TotalLag != 12 || s.Lag.ByGroup["payment"]["orders.created"][0] != 12 {
		t.Fatalf("dlq %v lag %v", k.DlqPerMin, k.TotalLag)
	}
	b := s.Brokers
	if !b[2].Up || b[2].LeaderCount != 23 || b[2].UnderReplicated != 4 || !b[1].IsController || b[0].IsController {
		t.Fatalf("brokers %+v", b)
	}
	p := s.Services["payment"]
	if !p.Known || p.Replicas != 2 || p.ReadyReplicas != 1 || p.AppliedGeneration != 7 || s.Services["order-generator"].AppliedGeneration != 0 {
		t.Fatalf("services %+v", s.Services)
	}
}

func Test_shouldPreferKMinionLagWhenItIsInstalled(t *testing.T) {
	// GIVEN: KMinion reports 99 records of lag while the admin API sees 12
	vm := healthyLab()
	vm.result[Queries.TotalLag] = one("99")
	c := collector(t, vm, &fakeLag{lag: Lag{Total: 12}})

	// WHEN: the board refreshes
	c.Refresh(ctx)

	// THEN: the KPI shows KMinion's number
	if got := c.Snapshot().Kpis.TotalLag; got != 99 {
		t.Fatalf("got %v", got)
	}
}

func Test_shouldKeepThePreviousPictureWhenAQueryFails(t *testing.T) {
	// GIVEN: one good refresh, after which each kind of query starts failing in turn
	q := Queries
	for _, failing := range []string{q.OrdersPerSec, q.TotalLag, q.BrokerUp, q.SpecReplicas} {
		vm := healthyLab()
		c := collector(t, vm, &fakeLag{})
		c.Refresh(ctx)

		// WHEN: the query fails during the next refresh while orders double
		vm.set(func(f *fakeVM) { f.failOn = failing; f.result[q.OrdersPerSec] = one("100") })
		c.Refresh(ctx)

		// THEN: the board still shows the last good values instead of zeros
		s := c.Snapshot()
		if s.Kpis.OrdersPerSec != 50 || !s.Brokers[0].Up || !s.Services["payment"].Known {
			t.Fatalf("failing %q: %+v", failing, s.Kpis)
		}
	}
}

func Test_shouldKeepTheLastLagWhenKafkaCannotBeAsked(t *testing.T) {
	// GIVEN: a first refresh with 5 records of lag
	lag := &fakeLag{lag: Lag{Total: 5}}
	c := collector(t, healthyLab(), lag)
	c.Refresh(ctx)

	// WHEN: the admin API fails during the next refresh
	lag.mu.Lock()
	lag.err, lag.lag = errors.New("no brokers"), Lag{}
	lag.mu.Unlock()
	c.Refresh(ctx)

	// THEN: the lag stays at 5
	if got := c.Snapshot().Kpis.TotalLag; got != 5 {
		t.Fatalf("got %v", got)
	}
}

func Test_shouldReportAnErrorWhenVictoriaMetricsIsUnreachableOrAnswersGarbage(t *testing.T) {
	// GIVEN: an invalid address, a closed port and a proxy answering with an HTML error page
	garbage := httptest.NewServer(&fakeVM{broken: true})
	defer garbage.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	// WHEN: a query is sent to each of them
	_, invalid := VM{Base: "http://[::1", HTTP: http.DefaultClient}.Query(ctx, "up")
	_, refused := VM{Base: closed.URL, HTTP: http.DefaultClient}.Query(ctx, "up")
	_, html := VM{Base: garbage.URL, HTTP: http.DefaultClient}.Query(ctx, "up")

	// THEN: every query fails instead of returning empty data
	if invalid == nil || refused == nil || html == nil {
		t.Fatalf("%v / %v / %v", invalid, refused, html)
	}
}

func Test_shouldRefreshPeriodicallyWhenTheCollectorRuns(t *testing.T) {
	// GIVEN: a running collector
	c := collector(t, healthyLab(), &fakeLag{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx, 10*time.Millisecond); close(done) }()

	// WHEN: a few intervals pass and the board shuts down
	deadline := time.Now().Add(5 * time.Second)
	for c.Snapshot().Kpis.OrdersPerSec != 50 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done

	// THEN: the snapshot was refreshed
	if c.Snapshot().At.IsZero() {
		t.Fatal("never refreshed")
	}
}

func Test_shouldComputePartitionLagFromCommittedAndEndOffsetsWhenGroupsHaveCommitted(t *testing.T) {
	// GIVEN: five orders on partition 0 of orders.created, payment committed after the second one
	cluster := testkit.NewCluster(t)
	cl, _ := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	defer cl.Close()
	for range 5 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: contracts.TopicOrdersCreated, Partition: 0, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	adm := kadm.NewClient(cl)
	offsets := kadm.Offsets{}
	offsets.AddOffset(contracts.TopicOrdersCreated, 0, 2, -1)
	if _, err := adm.CommitOffsets(ctx, "payment", offsets); err != nil {
		t.Fatal(err)
	}

	// WHEN: the board computes the lag
	l, err := AdminLag{Adm: adm}.Lag(ctx)

	// THEN: payment lags 3 on partition 0, inventory has not committed and shows no lag
	if err != nil {
		t.Fatal(err)
	}
	if got := l.ByGroup["payment"][contracts.TopicOrdersCreated]; len(got) != 3 || got[0] != 3 || l.Total != 3 {
		t.Fatalf("payment %v total %d", got, l.Total)
	}
	if l.ByGroup["inventory"][contracts.TopicOrdersCreated][0] != 0 || l.Partitions[contracts.TopicOrdersDLQ] != 3 || len(l.ByGroup) != 5 {
		t.Fatalf("lag %+v", l)
	}
}

func Test_shouldFailTheLagWhenKafkaIsUnreachableOrRefusesTheGroupOffsets(t *testing.T) {
	// GIVEN: a cluster that refuses to show group offsets, and a closed client
	cluster := testkit.NewCluster(t)
	cluster.ControlKey(int16(kmsg.OffsetFetch), func(r kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		req := r.(*kmsg.OffsetFetchRequest)
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		for _, g := range req.Groups {
			resp.Groups = append(resp.Groups, kmsg.OffsetFetchResponseGroup{Group: g.Group, ErrorCode: kerr.GroupAuthorizationFailed.Code})
		}
		return resp, nil, true
	})
	cl, _ := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...))
	defer cl.Close()
	closed, _ := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...))
	closed.Close()

	// WHEN: the board asks for the lag through both
	_, refused := AdminLag{Adm: kadm.NewClient(cl)}.Lag(ctx)
	_, unreachable := AdminLag{Adm: kadm.NewClient(closed)}.Lag(ctx)

	// THEN: both fail
	if refused == nil || unreachable == nil {
		t.Fatalf("%v / %v", refused, unreachable)
	}
}
