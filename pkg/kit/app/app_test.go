package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/config"
	"kafkalab/pkg/kit/runner"
	"kafkalab/pkg/kit/testkit"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}
func (s *syncBuffer) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func get(t *testing.T, addr net.Addr, path string) (int, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr.String() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func baseEnv(extra map[string]string) config.Lookup {
	m := map[string]string{"HTTP_ADDR": "127.0.0.1:0"}
	for k, v := range extra {
		m[k] = v
	}
	return config.MapEnv(m)
}

func Test_shouldServeHealthReadinessAndMetricsWhenTheServiceRuns(t *testing.T) {
	// GIVEN: a service that becomes ready once its work loop has started
	var out syncBuffer
	ready := make(chan struct{})
	release := make(chan struct{})
	var addr net.Addr
	done := make(chan error, 1)
	go func() {
		done <- RunContext(context.Background(), "payment", Options{Env: baseEnv(nil), Out: &out, Listening: func(a net.Addr) { addr = a }},
			func(ctx context.Context, a *App) error {
				code, _ := get(t, a.Addr(), "/readyz")
				if code != http.StatusServiceUnavailable {
					t.Errorf("expected not ready before registration, got %d", code)
				}
				a.AddReadiness(func() bool { return true })
				a.Metrics.GetOrCreateCounter("lab_test_total").Inc()
				close(ready)
				<-release
				return nil
			})
	}()
	<-ready

	// WHEN: Kubernetes probes the pod and vmagent scrapes it
	healthCode, _ := get(t, addr, "/healthz")
	readyCode, _ := get(t, addr, "/readyz")
	_, metricsBody := get(t, addr, "/metrics")
	close(release)

	// THEN: it is alive and ready, exposes its own and the Go runtime metrics, and logs as JSON
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if healthCode != 200 || readyCode != 200 || !strings.Contains(metricsBody, "lab_test_total 1") || !strings.Contains(metricsBody, "go_goroutines") {
		t.Fatalf("health=%d ready=%d metrics:\n%s", healthCode, readyCode, metricsBody)
	}
	if !strings.Contains(out.String(), `"msg":"service starting"`) || !strings.Contains(out.String(), `"service":"payment"`) {
		t.Fatalf("unexpected logs %s", out.String())
	}
}

func Test_shouldReportNotReadyWhenAnyComponentIsNotReady(t *testing.T) {
	// GIVEN: a service with a ready database but a consumer that has not joined its group yet
	a := &App{}
	a.AddReadiness(func() bool { return true })
	a.AddReadiness(func() bool { return false })

	// WHEN / THEN: the pod is reported as not ready, so it gets no traffic
	if a.IsReady() {
		t.Fatal("expected not ready")
	}
}

func Test_shouldExitCleanlyWhenTheServiceIsStoppedBySignal(t *testing.T) {
	// GIVEN: a service whose work loop ends because the pod is terminated
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// WHEN: it runs
	err := RunContext(ctx, "shipping", Options{Env: baseEnv(nil)}, func(ctx context.Context, _ *App) error {
		return ctx.Err()
	})

	// THEN: the cancellation is a normal shutdown, not an error
	if err != nil {
		t.Fatal(err)
	}
}

func Test_shouldFailStartupWhenTheConfigurationIsInvalid(t *testing.T) {
	// GIVEN: broken settings: a malformed number, a malformed trace URL, a port already in use
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	for name, env := range map[string]config.Lookup{
		"config":  baseEnv(map[string]string{"TRACE_ROOT_RATE": "x"}),
		"tracing": baseEnv(map[string]string{"OTLP_TRACES_URL": "::nope"}),
		"listen":  config.MapEnv(map[string]string{"HTTP_ADDR": ln.Addr().String()}),
	} {
		// WHEN: the service starts
		err := RunContext(context.Background(), "inventory", Options{Env: env, Out: io.Discard, RootTraces: true}, func(context.Context, *App) error {
			t.Fatalf("%s: main must not run", name)
			return nil
		})

		// THEN: it refuses to start
		if err == nil {
			t.Fatalf("%s: expected an error", name)
		}
	}
}

func Test_shouldReturnTheWorkLoopErrorWhenItFails(t *testing.T) {
	// GIVEN: a work loop that hits an unrecoverable problem
	boom := errors.New("schema registry unreachable for good")

	// WHEN: the service runs
	err := RunContext(context.Background(), "analytics", Options{Env: baseEnv(nil), Out: io.Discard}, func(context.Context, *App) error { return boom })

	// THEN: the error is returned so the process exits non-zero and Kubernetes restarts it
	if !errors.Is(err, boom) {
		t.Fatalf("expected %v, got %v", boom, err)
	}
}

func Test_shouldWireAConsumingServiceEndToEndWhenAllDependenciesAreAvailable(t *testing.T) {
	// GIVEN: Kafka, the schema registry and PostgreSQL, and a payment-like handler
	cluster := testkit.NewCluster(t)
	reg := testkit.NewRegistry(t, true)
	env := baseEnv(map[string]string{
		"KAFKA_BROKERS": strings.Join(cluster.Brokers, ","), "SCHEMA_REGISTRY_URL": reg.URL, "DATABASE_URL": testkit.PostgresDSN(t),
	})
	migrations := fstest.MapFS{"001_payments.sql": {Data: []byte(`CREATE TABLE seen (order_id text PRIMARY KEY)`)}}
	processed := make(chan string, 1)

	// WHEN: the service starts, and an order is published
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunContext(ctx, "payment", Options{Env: env, Out: io.Discard}, func(ctx context.Context, a *App) error {
			s, err := a.Serde(ctx, contracts.TopicOrdersCreated, contracts.TopicPlatformControl)
			if err != nil {
				return err
			}
			ctl, err := a.Control(ctx, s)
			if err != nil {
				return err
			}
			defer ctl.Close()
			db, err := a.Database(ctx, ctl, migrations)
			if err != nil {
				return err
			}
			defer db.Close()
			p, err := a.Producer(ctl)
			if err != nil {
				return err
			}
			b, _ := s.Encode(&events.OrderCreated{Meta: events.EventMeta{EventID: "e1", OrderID: "o1", OccurredAt: time.Now(), OrderCreatedAt: time.Now()},
				Region: events.RegionEast, PaymentMethod: events.PaymentMethodWallet})
			if err := p.Client().ProduceSync(ctx, testRecord(b)).FirstErr(); err != nil {
				return err
			}
			p.Close(ctx)
			r := a.Runner(runner.Config{Topics: []string{contracts.TopicOrdersCreated}, DB: db, Codec: s, Control: ctl,
				Handler: runner.HandlerFunc(func(ctx context.Context, tx pgx.Tx, in runner.Input) ([]runner.Output, error) {
					_, err := tx.Exec(ctx, `INSERT INTO seen VALUES ($1)`, in.Meta.OrderID)
					processed <- in.Meta.OrderID
					return nil, err
				})})
			return r.Run(ctx)
		})
	}()

	// THEN: the order is processed through the wired runner and the service shuts down cleanly
	select {
	case id := <-processed:
		if id != "o1" {
			t.Fatalf("processed %s", id)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("order was not processed")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func Test_shouldFailStartupWhenAMigrationIsBroken(t *testing.T) {
	// GIVEN: a service shipping an invalid migration
	env := baseEnv(map[string]string{"DATABASE_URL": testkit.PostgresDSN(t)})
	broken := fstest.MapFS{"001_broken.sql": {Data: []byte(`CREATE TABLE`)}}

	// WHEN: it opens its database
	err := RunContext(context.Background(), "inventory", Options{Env: env, Out: io.Discard}, func(ctx context.Context, a *App) error {
		_, err := a.Database(ctx, staticKnobs{}, broken)
		return err
	})

	// THEN: startup fails with the migration name
	if err == nil || !strings.Contains(err.Error(), "001_broken.sql") {
		t.Fatalf("expected migration error, got %v", err)
	}
}

func Test_shouldFailStartupWhenTheDatabaseURLIsInvalid(t *testing.T) {
	// GIVEN: a malformed DATABASE_URL
	env := baseEnv(map[string]string{"DATABASE_URL": "postgres://%zz"})

	// WHEN: the service opens its database
	err := RunContext(context.Background(), "inventory", Options{Env: env, Out: io.Discard}, func(ctx context.Context, a *App) error {
		_, err := a.Database(ctx, staticKnobs{})
		return err
	})

	// THEN: startup fails
	if err == nil {
		t.Fatal("expected an error")
	}
}

type staticKnobs struct{}

func (staticKnobs) Knobs() events.Knobs      { return events.Knobs{DbLatencyMs: 1} }
func (staticKnobs) Changed() <-chan struct{} { return nil }
