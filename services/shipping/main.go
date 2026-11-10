// shipping joins payment and stock outcomes into one terminal decision per order.
package main

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/shipping/internal/adapters"
	shipping "kafkalab/services/shipping/internal/app"
)

func main() {
	app.Run(contracts.ServiceShipping, app.Options{}, func(ctx context.Context, a *app.App) error {
		every := a.Env.Duration("SWEEP_EVERY", 5*time.Second)
		deadline := a.Env.Duration("JOIN_DEADLINE", 300*time.Second)
		retain := a.Env.Duration("RETAIN_TERMINAL", 2*time.Hour)
		if err := a.Env.Err(); err != nil {
			return err
		}
		inputs := []string{contracts.TopicPaymentsCompleted, contracts.TopicPaymentsFailed,
			contracts.TopicInventoryReserved, contracts.TopicInventoryRejected}
		codec, err := a.Serde(ctx, append(inputs, contracts.TopicOrdersShipped, contracts.TopicOrdersCancelled, contracts.TopicPlatformControl)...)
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
		// Build the runner first: it registers the client buffer gauges for its consumer client
		// (the sweeper's producer would otherwise take them over).
		r := a.Runner(runner.Config{
			Topics:  inputs,
			DB:      db,
			Codec:   codec,
			Control: ctl,
			// Bound the fetch buffer so a catch-up after an outage fits the pod (kit change request).
			ExtraOpts: boundedFetch,
			Handler:   &shipping.Handler{Repo: adapters.Postgres{}, Control: ctl, Now: time.Now},
		})
		prod, err := a.Producer(ctl)
		if err != nil {
			return err
		}
		defer prod.Close(context.WithoutCancel(ctx))
		leader := &adapters.Leader{Pool: db.Pool, Key: adapters.GaugeLeaderKey}
		defer leader.Close()

		sweeper := &shipping.Sweeper{
			DB: db, Repo: adapters.Postgres{}, Control: ctl, Leader: leader,
			Publisher: adapters.KafkaPublisher{Producer: prod, Codec: codec, Metrics: a.Metrics},
			Metrics:   a.Metrics, Log: a.Log, Now: time.Now,
			Deadline: deadline, Every: every, Retain: retain, Batch: 500,
		}
		done := make(chan struct{})
		go func() { defer close(done); _ = sweeper.Run(ctx) }()
		defer func() { <-done }() // runs first: no sweep is in flight when producer and db close
		return r.Run(ctx)
	})
}

// boundedFetch keeps at most one fetch response of small partition slices in memory. The kit's
// BaseOptions allow 8 MiB per broker, which overflows a 64-96 MiB pod while it catches up on
// many partitions. Neither option is a Tuning Lab setting, so the knobs keep working.
var boundedFetch = []kgo.Opt{kgo.MaxConcurrentFetches(1), kgo.FetchMaxPartitionBytes(256 << 10)}
