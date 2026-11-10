// payment authorises orders and refunds cancelled ones.
package main

import (
	"context"
	"time"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/payment/internal/adapters"
	payment "kafkalab/services/payment/internal/app"
)

func main() {
	app.Run(contracts.ServicePayment, app.Options{}, func(ctx context.Context, a *app.App) error {
		codec, err := a.Serde(ctx, contracts.TopicOrdersCreated, contracts.TopicOrdersCancelled,
			contracts.TopicPaymentsCompleted, contracts.TopicPaymentsFailed, contracts.TopicPaymentsRefunded, contracts.TopicPlatformControl)
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
		return a.Runner(runner.Config{
			Topics:  []string{contracts.TopicOrdersCreated, contracts.TopicOrdersCancelled},
			DB:      db,
			Codec:   codec,
			Control: ctl,
			Handler: &payment.Handler{Repo: adapters.Postgres{}, Control: ctl, Now: time.Now},
		}).Run(ctx)
	})
}
