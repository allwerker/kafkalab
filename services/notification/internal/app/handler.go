// Package app turns business events into customer notifications: it picks the template and
// channel, hands the message to the provider, logs it and publishes notifications.sent.
package app

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/notification/internal/domain"
)

// Notification is one message sent to a customer.
type Notification struct {
	OrderID  string
	Template string
	Channel  domain.Channel
	SentAt   time.Time
}

// Repository keeps the log of sent notifications inside the batch transaction.
type Repository interface {
	Record(ctx context.Context, tx pgx.Tx, n Notification) error
}

// Provider delivers a notification (e-mail gateway, push service, SMS gateway).
type Provider interface {
	Send(ctx context.Context, n Notification) error
}

// Handler handles orders.created, orders.shipped, orders.cancelled and payments.refunded.
type Handler struct {
	Repo     Repository
	Provider Provider
	Now      func() time.Time
}

// Handle implements runner.Handler: exactly one notification per input event.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, in runner.Input) ([]runner.Output, error) {
	var template string
	switch in.Value.(type) {
	case *events.OrderCreated:
		template = domain.TemplateOrderConfirmed
	case *events.OrderShipped:
		template = domain.TemplateOrderShipped
	case *events.OrderCancelled:
		template = domain.TemplateOrderCancelled
	case *events.PaymentRefunded:
		template = domain.TemplatePaymentRefunded
	default:
		return nil, runner.Permanent(fmt.Errorf("notification does not handle %T", in.Value))
	}
	now := h.Now().UTC()
	n := Notification{OrderID: in.Meta.OrderID, Template: template, Channel: domain.ChannelFor(in.Meta.OrderID), SentAt: now}
	if err := h.Provider.Send(ctx, n); err != nil {
		return nil, err
	}
	if err := h.Repo.Record(ctx, tx, n); err != nil {
		return nil, err
	}
	return []runner.Output{{Topic: contracts.TopicNotificationsSent, Key: n.OrderID, Value: &events.NotificationSent{
		Meta: events.EventMeta{
			EventID:        contracts.DeriveEventID(in.Meta.EventID, "notification"),
			CausationID:    in.Meta.EventID,
			OrderID:        n.OrderID,
			OccurredAt:     now,
			OrderCreatedAt: in.Meta.OrderCreatedAt,
			Producer:       contracts.ServiceNotification,
		},
		Channel:  events.NotificationChannel(n.Channel),
		Template: template,
	}}}, nil
}
