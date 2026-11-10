// Package app is the service skeleton: configuration, JSON logs, tracing, the HTTP server for
// /metrics, /healthz and /readyz, graceful shutdown, and helpers that wire the kit together.
// A service's main.go is a single app.Run call.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"

	"kafkalab/pkg/kit/config"
	"kafkalab/pkg/kit/control"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/obs"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/producer"
	"kafkalab/pkg/kit/runner"
	"kafkalab/pkg/kit/serde"
)

// App carries everything a service needs at runtime.
type App struct {
	Cfg     config.Config
	Env     *config.Reader // service-specific settings
	Log     *slog.Logger
	Metrics *metrics.Set
	Mux     *http.ServeMux
	Hooks   *kafka.Hooks
	Tracer  *kotel.Tracer

	kotelHooks []kgo.Hook
	mu         sync.Mutex
	readiness  []func() bool
	addr       net.Addr
}

// Options configures RunContext (tests); Run uses the process environment.
type Options struct {
	Env config.Lookup
	Out io.Writer
	// RootTraces makes this service start traces (order-generator); others only continue them.
	RootTraces bool
	// Listening is called with the HTTP address once the server accepts connections.
	Listening func(net.Addr)
}

// Run starts the service and exits the process when it ends.
func Run(service string, o Options, main func(ctx context.Context, a *App) error) { // coverage-ignore: process entry point (signals, os.Exit); covered end-to-end
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := RunContext(ctx, service, o, main); err != nil {
		fmt.Fprintln(os.Stderr, service+":", err)
		os.Exit(1)
	}
}

// RunContext runs main until ctx ends or main returns.
func RunContext(ctx context.Context, service string, o Options, main func(ctx context.Context, a *App) error) error {
	if o.Out == nil {
		o.Out = os.Stdout
	}
	cfg, err := config.Load(service, o.Env)
	if err != nil {
		return err
	}
	log := obs.NewLogger(o.Out, cfg.ServiceName, cfg.LogLevel)
	rate := 0.0
	if o.RootTraces {
		rate = cfg.TraceRootRate
	}
	shutdownTracing, err := obs.InitTracing(ctx, obs.TracingOptions{Service: cfg.ServiceName, Endpoint: cfg.OTLPTracesURL, RootRate: rate})
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()

	a := &App{Cfg: cfg, Env: config.NewReader(o.Env), Log: log, Metrics: metrics.NewSet(), Mux: http.NewServeMux()}
	a.Hooks = kafka.NewHooks(a.Metrics)
	a.Tracer = kotel.NewTracer(kotel.TracerProvider(otel.GetTracerProvider()), kotel.TracerPropagator(otel.GetTextMapPropagator()))
	a.kotelHooks = kotel.NewKotel(kotel.WithTracer(a.Tracer)).Hooks()
	metrics.RegisterSet(a.Metrics)
	defer metrics.UnregisterSet(a.Metrics, true)
	a.routes()

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.HTTPAddr, err)
	}
	a.addr = ln.Addr()
	srv := &http.Server{Handler: a.Mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(c)
	}()
	if o.Listening != nil {
		o.Listening(a.addr)
	}

	log.InfoContext(ctx, "service starting", "http", a.addr.String(), "brokers", cfg.KafkaBrokers)
	err = main(ctx, a)
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	log.InfoContext(context.WithoutCancel(ctx), "service stopped", "error", err)
	return err
}

func (a *App) routes() {
	a.Mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		metrics.WritePrometheus(w, true)
	})
	a.Mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	a.Mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.IsReady() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
}

// Addr returns the HTTP server address.
func (a *App) Addr() net.Addr { return a.addr }

// AddReadiness registers a readiness condition; /readyz is green when all of them are.
func (a *App) AddReadiness(ready func() bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.readiness = append(a.readiness, ready)
}

// IsReady reports whether every readiness condition holds (false if none was registered).
func (a *App) IsReady() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.readiness) == 0 {
		return false
	}
	for _, r := range a.readiness {
		if !r() {
			return false
		}
	}
	return true
}

// KafkaHooks returns the metric and tracing hooks for Kafka clients.
func (a *App) KafkaHooks() []kgo.Hook { return append([]kgo.Hook{a.Hooks}, a.kotelHooks...) }

// Serde resolves the schemas of the given topics from the registry (waits for them).
func (a *App) Serde(ctx context.Context, topics ...string) (*serde.Serde, error) {
	cl, err := sr.NewClient(sr.URLs(a.Cfg.SchemaRegistryURL))
	if err != nil { // coverage-ignore: sr.NewClient only fails for invalid static options
		return nil, err
	}
	return serde.New(ctx, serde.Options{Registry: cl, Topics: topics, Log: a.Log})
}

// Control starts the control-plane client for this service.
func (a *App) Control(ctx context.Context, dec control.Decoder) (*control.Client, error) {
	return control.Start(ctx, control.Options{Brokers: a.Cfg.KafkaBrokers, Service: a.Cfg.ServiceName, Decoder: dec, Metrics: a.Metrics, Log: a.Log})
}

// Database opens PostgreSQL (waiting until it is reachable), applies the kit migrations and then
// the service migrations. Transactions honour the Database Slowdown knob of ctl.
func (a *App) Database(ctx context.Context, ctl runner.Knobs, migrations ...fs.FS) (*pg.DB, error) {
	db, err := pg.Open(ctx, pg.Options{URL: a.Cfg.DatabaseURL, Metrics: a.Metrics, Log: a.Log, MaxConns: 8,
		Latency: func() time.Duration { return time.Duration(ctl.Knobs().DbLatencyMs) * time.Millisecond }})
	if err != nil {
		return nil, err
	}
	kit, _ := fs.Sub(runner.Migrations(), "migrations")
	for _, m := range append([]fs.FS{kit}, migrations...) {
		if err := db.Migrate(ctx, m); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

// FaultDialer returns the Network Chaos dialer driven by ctl's knobs.
func (a *App) FaultDialer(ctl runner.Knobs) *kafka.FaultDialer {
	return kafka.NewFaultDialer(func() kafka.Faults {
		k := ctl.Knobs()
		return kafka.Faults{Latency: time.Duration(k.NetLatencyMs) * time.Millisecond, DropProbability: k.NetDropProbability}
	})
}

// Runner builds a consumer runner with the app's brokers, metrics, logs, tracing and fault
// dialer filled in, and registers it for readiness and buffer metrics.
func (a *App) Runner(cfg runner.Config) *runner.Runner {
	cfg.Service = a.Cfg.ServiceName
	cfg.Brokers = a.Cfg.KafkaBrokers
	cfg.Metrics = a.Metrics
	cfg.Log = a.Log
	cfg.Hooks = a.KafkaHooks()
	cfg.Tracer = a.Tracer
	if cfg.Dialer == nil {
		cfg.Dialer = a.FaultDialer(cfg.Control).DialContext
	}
	r := runner.New(cfg)
	a.AddReadiness(r.Ready)
	a.Hooks.WatchBuffers(r.Client)
	return r
}

// Producer builds the producing client for services without input topics.
func (a *App) Producer(ctl runner.Knobs) (*producer.Producer, error) {
	p, err := producer.New(producer.Config{Service: a.Cfg.ServiceName, Brokers: a.Cfg.KafkaBrokers, Control: ctl,
		Metrics: a.Metrics, Log: a.Log, Hooks: a.KafkaHooks(), Dialer: a.FaultDialer(ctl).DialContext})
	if err != nil { // coverage-ignore: only invalid static options fail here
		return nil, err
	}
	a.Hooks.WatchBuffers(p.Client)
	return p, nil
}
