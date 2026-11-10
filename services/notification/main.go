// notification tells customers about their orders: one notification per relevant event.
package main

import (
	"context"
	"time"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/notification/internal/adapters"
	notification "kafkalab/services/notification/internal/app"
)

func main() {
	app.Run(contracts.ServiceNotification, app.Options{}, func(ctx context.Context, a *app.App) error {
		latency := a.Env.Duration("SEND_LATENCY", time.Millisecond)
		keep := a.Env.Duration("NOTIFICATION_RETENTION", time.Hour)
		if err := a.Env.Err(); err != nil {
			return err
		}
		inputs := []string{contracts.TopicOrdersCreated, contracts.TopicOrdersShipped, contracts.TopicOrdersCancelled, contracts.TopicPaymentsRefunded}
		codec, err := a.Serde(ctx, append(inputs, contracts.TopicNotificationsSent, contracts.TopicPlatformControl)...)
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
		go (&adapters.Janitor{Pool: db.Pool, Keep: keep, Log: a.Log}).Run(ctx)
		return a.Runner(runner.Config{
			Topics:  inputs,
			DB:      db,
			Codec:   codec,
			Control: ctl,
			Handler: &notification.Handler{Repo: adapters.Postgres{}, Provider: adapters.SimulatedProvider{Latency: latency}, Now: time.Now},
		}).Run(ctx)
	})
}
