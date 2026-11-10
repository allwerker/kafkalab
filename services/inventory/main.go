// inventory reserves stock for orders, releases it for cancelled ones and restocks shelves.
package main

import (
	"context"
	"time"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/inventory/internal/adapters"
	inventory "kafkalab/services/inventory/internal/app"
)

func main() {
	app.Run(contracts.ServiceInventory, app.Options{}, func(ctx context.Context, a *app.App) error {
		restockEvery := a.Env.Duration("RESTOCK_EVERY", 10*time.Second)
		gaugeEvery := a.Env.Duration("STOCK_GAUGE_EVERY", 2*time.Second)
		if err := a.Env.Err(); err != nil {
			return err
		}
		codec, err := a.Serde(ctx, contracts.TopicOrdersCreated, contracts.TopicOrdersCancelled,
			contracts.TopicInventoryReserved, contracts.TopicInventoryRejected, contracts.TopicInventoryReleased, contracts.TopicPlatformControl)
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
		restock := &adapters.Restocker{Pool: db.Pool, Metrics: a.Metrics, Log: a.Log, Every: restockEvery, GaugeEvery: gaugeEvery}
		rctx, stopRestock := context.WithCancel(ctx)
		restocked := make(chan struct{})
		go func() { restock.Run(rctx); close(restocked) }()
		defer func() { stopRestock(); <-restocked }() // before db.Close: Run releases its session
		return a.Runner(runner.Config{
			Topics:  []string{contracts.TopicOrdersCreated, contracts.TopicOrdersCancelled},
			DB:      db,
			Codec:   codec,
			Control: ctl,
			Handler: &inventory.Handler{Repo: adapters.Postgres{}, Now: time.Now},
		}).Run(ctx)
	})
}
