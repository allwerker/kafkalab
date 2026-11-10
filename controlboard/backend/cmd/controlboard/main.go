// controlboard serves the Kafka Lab board: REST API, WebSocket live feed, alerts and the SPA.
package main

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/controlboard/backend/internal/alerts"
	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/api/fakeexp"
	"kafkalab/controlboard/backend/internal/board"
	"kafkalab/controlboard/backend/internal/experiments"
	"kafkalab/controlboard/backend/internal/feed"
	"kafkalab/controlboard/backend/internal/grafana"
	"kafkalab/controlboard/backend/internal/k8s"
	"kafkalab/controlboard/backend/internal/links"
	"kafkalab/controlboard/backend/internal/observe"
	"kafkalab/controlboard/backend/web"
	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	"kafkalab/pkg/kit/kafka"
	"kafkalab/pkg/kit/pg"
)

func main() {
	app.Run(contracts.ServiceControlboard, app.Options{}, run)
}

func run(ctx context.Context, a *app.App) error {
	env := a.Env
	vmURL := env.String("VM_URL", "http://vmsingle-vmks.monitoring:8428")
	vmalertURL := env.String("VMALERT_URL", "http://vmalert-vmks.monitoring:8080")
	alertmanagerURL := env.String("ALERTMANAGER_URL", "http://vmalertmanager-vmks.monitoring:9093")
	boardURL := env.String("BOARD_URL", "http://board.localhost:8080")
	gl := grafana.Links{Base: env.String("GRAFANA_URL", "http://grafana.localhost:8080")}
	brokers := env.Int("BROKER_COUNT", 3)
	grafanaAPI := env.String("GRAFANA_API_URL", "http://grafana.monitoring:80")
	grafanaUser := env.String("GRAFANA_USER", "admin")
	grafanaPassword := env.String("GRAFANA_PASSWORD", "admin")
	chaosNetImage := env.String("CHAOS_NET_IMAGE", "")
	origins := env.List("WS_ORIGINS", "localhost:5173,127.0.0.1:5173")
	linkCfg, err := links.Load(env.String("LINKS_FILE", ""))
	if err != nil {
		return err
	}
	if err := env.Err(); err != nil {
		return err
	}
	hc := &http.Client{Timeout: 3 * time.Second}

	codec, err := a.Serde(ctx, append(append([]string{}, contracts.BusinessTopics...), contracts.TopicPlatformControl, contracts.TopicPlatformAudit)...)
	if err != nil {
		return err
	}
	var store alerts.Store = &alerts.Memory{}
	var db *pg.DB
	if a.Cfg.DatabaseURL != "" {
		db, err = pg.Open(ctx, pg.Options{URL: a.Cfg.DatabaseURL, MaxConns: 4, Metrics: a.Metrics, Log: a.Log})
		if err != nil {
			return err
		}
		defer db.Close()
		if err := db.Migrate(ctx, alerts.Migrations()); err != nil {
			return err
		}
		store = alerts.PG{DB: db}
	} else {
		a.Log.WarnContext(ctx, "DATABASE_URL not set: alert history is kept in memory only")
	}

	feedClient, err := kgo.NewClient(feed.ClientOptions(a.Cfg.KafkaBrokers, a.Hooks)...)
	if err != nil {
		return err
	}
	defer feedClient.Close()
	a.Hooks.WatchBuffers(func() *kgo.Client { return feedClient })
	adminClient, err := kgo.NewClient(kafka.BaseOptions(a.Cfg.KafkaBrokers, contracts.ServiceControlboard+"-admin")...)
	if err != nil {
		return err
	}
	defer adminClient.Close()

	live := feed.New(feed.Options{Decoder: codec, Grafana: gl})
	collector := observe.NewCollector(observe.Options{VM: observe.VM{Base: vmURL, HTTP: hc},
		Lag: observe.AdminLag{Adm: kadm.NewClient(adminClient)}, Brokers: brokers, Log: a.Log})
	checker := links.NewChecker(linkCfg, hc)

	var b *board.Board
	am := alerts.New(alerts.Options{Store: store, Grafana: gl, Metrics: a.Metrics, Log: a.Log,
		OnChange:   func(x api.Alert) { b.AlertChanged(x) },
		OnTimeline: func(e api.TimelineEntry) { b.AddTimeline(e) }})
	if err := am.Load(ctx); err != nil {
		a.Log.WarnContext(ctx, "alert history not loaded", "error", err)
	}
	b = board.New(board.Options{Feed: live, Observed: collector, Alerts: am, Links: checker, Origins: origins,
		ParseWebhook: func(body []byte) ([]api.Alert, error) { return alerts.ParseWebhook(bytes.NewReader(body), gl) },
		FireTest: func(ctx context.Context) error {
			return alerts.FireTest(ctx, hc, alertmanagerURL, boardURL, time.Now())
		},
		Metrics: a.Metrics, Log: a.Log})
	if db == nil {
		a.Log.WarnContext(ctx, "DATABASE_URL not set: using the in-memory fake experiment engine")
		b.SetExperiments(fakeexp.New(nil))
	} else {
		engineClient, err := kgo.NewClient(kafka.BaseOptions(a.Cfg.KafkaBrokers, contracts.ServiceControlboard+"-engine")...)
		if err != nil {
			return err
		}
		defer engineClient.Close()
		var kube *k8s.Client
		if kc, err := k8s.InCluster(k8s.ServiceAccountDir, os.Getenv); err == nil {
			kube = k8s.New(kc)
		} else {
			a.Log.WarnContext(ctx, "no Kubernetes API: Kubernetes and broker scenarios are unavailable", "error", err)
		}
		eng, err := experiments.New(experiments.Config{
			DB:            db,
			Publisher:     experiments.NewKafkaPublisher(engineClient, codec),
			Generations:   experiments.KafkaGenerations(a.Cfg.KafkaBrokers, codec),
			Observer:      b.Observer(),
			Kube:          kube,
			Admin:         experiments.NewKafkaAdmin(engineClient),
			Query:         experiments.VMQuery(vmURL),
			Annotator:     &experiments.GrafanaAnnotator{Base: grafanaAPI, User: grafanaUser, Password: grafanaPassword},
			Metrics:       a.Metrics,
			Log:           a.Log,
			GrafanaURL:    gl.Base,
			ChaosNetImage: chaosNetImage,
			TickEvery:     time.Second,
		})
		if err != nil {
			return err
		}
		defer eng.Close()
		b.SetExperiments(eng)
	}
	b.Register(a.Mux)
	a.Mux.Handle("/", web.Handler(web.Dist()))
	a.AddReadiness(func() bool { return true })
	b.AddTimeline(api.TimelineEntry{Kind: api.KindSystem, Severity: api.SeverityInfo, Title: "Controlboard started"})

	var wg sync.WaitGroup
	wg.Go(func() { _ = live.Run(ctx, feedClient) })
	wg.Go(func() { collector.Run(ctx, 2*time.Second) })
	wg.Go(func() { checker.Run(ctx, 15*time.Second) })
	wg.Go(func() { am.PollPending(ctx, alerts.VMAlert{Base: vmalertURL, HTTP: hc, Grafana: gl}, 5*time.Second) })
	wg.Go(func() { am.Run(ctx, time.Hour) })
	wg.Go(func() { b.Run(ctx) })
	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}
