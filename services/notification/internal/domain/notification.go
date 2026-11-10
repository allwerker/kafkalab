// Package domain holds the notification rules: which template a business event triggers and
// which channel reaches the customer. Pure Go, no infrastructure; see docs/contracts/events.md.
package domain

import "hash/fnv"

// Templates, one per triggering event.
const (
	TemplateOrderConfirmed  = "order-confirmed"  // orders.created
	TemplateOrderShipped    = "order-shipped"    // orders.shipped
	TemplateOrderCancelled  = "order-cancelled"  // orders.cancelled
	TemplatePaymentRefunded = "payment-refunded" // payments.refunded
)

// Channel is how a customer is notified (mirror events.NotificationChannel).
type Channel string

const (
	ChannelEmail Channel = "EMAIL"
	ChannelPush  Channel = "PUSH"
	ChannelSMS   Channel = "SMS"
)

// ChannelFor returns the channel the customer of an order prefers: 60% e-mail, 25% push,
// 15% SMS. It depends only on the order id, so every notification about one order (and every
// redelivery) uses the same channel.
func ChannelFor(orderID string) Channel {
	h := fnv.New32a()
	_, _ = h.Write([]byte(orderID))
	switch bucket := h.Sum32() % 100; {
	case bucket < 60:
		return ChannelEmail
	case bucket < 85:
		return ChannelPush
	default:
		return ChannelSMS
	}
}
