// analytics turns every business event into business metrics.
package main

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/analytics/internal/adapters"
	analytics "kafkalab/services/analytics/internal/app"
)

func main() {
	app.Run(contracts.ServiceAnalytics, app.Options{}, func(ctx context.Context, a *app.App) error {
		codec, err := a.Serde(ctx, append(append([]string{}, contracts.BusinessTopics...), contracts.TopicPlatformControl)...)
		if err != nil {
			return err
		}
		ctl, err := a.Control(ctx, codec)
		if err != nil {
			return err
		}
		defer ctl.Close()
		db, err := a.Database(ctx, ctl, adapters.Migrations())
		if err != nil {
			return err
		}
		defer db.Close()

		// orders.dlq holds raw bytes: counted by a plain group client, not by the runner.
		dlq := &adapters.DLQCounter{Brokers: a.Cfg.KafkaBrokers, Hooks: a.KafkaHooks(), Metrics: a.Metrics, Log: a.Log, ExtraOpts: boundedFetch}
		a.AddReadiness(dlq.Ready)
		janitor := &analytics.Janitor{DB: db, Repo: adapters.Postgres{}, Log: a.Log, Now: time.Now, Every: time.Minute, Batch: 5000}
		done := make(chan struct{}, 2)
		go func() { _ = dlq.Run(ctx); done <- struct{}{} }()
		go func() { _ = janitor.Run(ctx); done <- struct{}{} }()
		defer func() { <-done; <-done }() // both stopped before the database closes

		return a.Runner(runner.Config{
			Topics:  contracts.BusinessTopics,
			DB:      db,
			Codec:   codec,
			Control: ctl,
			// Bound the fetch buffer so a catch-up after an outage fits the pod (kit change request).
			ExtraOpts: boundedFetch,
			Handler:   &analytics.Handler{Repo: adapters.Postgres{}, Metrics: a.Metrics, Now: time.Now},
		}).Run(ctx)
	})
}

// boundedFetch keeps at most one fetch response of small partition slices in memory. The kit's
// BaseOptions allow 8 MiB per broker, which overflows a 64-96 MiB pod while it catches up on
// many partitions. Neither option is a Tuning Lab setting, so the knobs keep working.
var boundedFetch = []kgo.Opt{kgo.MaxConcurrentFetches(1), kgo.FetchMaxPartitionBytes(256 << 10)}
