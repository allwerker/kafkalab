// order-generator emits synthetic customer orders.
package main

import (
	"context"
	"time"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/kit/app"
	gen "kafkalab/services/order-generator/internal/app"
	"kafkalab/services/order-generator/internal/domain"
)

func main() {
	app.Run(contracts.ServiceOrderGenerator, app.Options{RootTraces: true}, func(ctx context.Context, a *app.App) error {
		codec, err := a.Serde(ctx, contracts.TopicOrdersCreated, contracts.TopicPlatformControl)
		if err != nil {
			return err
		}
		ctl, err := a.Control(ctx, codec)
		if err != nil {
			return err
		}
		defer ctl.Close()
		p, err := a.Producer(ctl)
		if err != nil {
			return err
		}
		defer p.Close(context.WithoutCancel(ctx))
		a.AddReadiness(func() bool { return true })
		loop := &gen.Loop{Producer: p, Encoder: codec, Control: ctl, Gen: domain.NewGenerator(uint64(time.Now().UnixNano())),
			BaseRate: a.Env.Float("ORDERS_PER_SECOND", 50), Metrics: a.Metrics, Now: time.Now}
		return loop.Run(ctx)
	})
}
