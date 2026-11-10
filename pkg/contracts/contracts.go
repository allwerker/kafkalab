// Package contracts holds the frozen cross-service names: topics, consumer groups, services,
// headers and the deterministic event-id rule. See docs/contracts/.
package contracts

import (
	"crypto/sha256"
	"fmt"
)

// Topic names (deploy/topics.yaml is the deployment source; a test keeps both in sync).
const (
	TopicOrdersCreated     = "orders.created"
	TopicPaymentsCompleted = "payments.completed"
	TopicPaymentsFailed    = "payments.failed"
	TopicPaymentsRefunded  = "payments.refunded"
	TopicInventoryReserved = "inventory.reserved"
	TopicInventoryRejected = "inventory.rejected"
	TopicInventoryReleased = "inventory.released"
	TopicOrdersShipped     = "orders.shipped"
	TopicOrdersCancelled   = "orders.cancelled"
	TopicNotificationsSent = "notifications.sent"
	TopicOrdersDLQ         = "orders.dlq"
	TopicPlatformControl   = "platform.control"
	TopicPlatformAudit     = "platform.audit"
)

// Service names. They double as consumer group ids, PostgreSQL roles/schemas and control keys.
const (
	ServiceOrderGenerator = "order-generator"
	ServicePayment        = "payment"
	ServiceInventory      = "inventory"
	ServiceShipping       = "shipping"
	ServiceNotification   = "notification"
	ServiceAnalytics      = "analytics"
	ServiceControlboard   = "controlboard"
)

// Services lists every business service in pipeline order.
var Services = []string{
	ServiceOrderGenerator, ServicePayment, ServiceInventory,
	ServiceShipping, ServiceNotification, ServiceAnalytics,
}

// BusinessTopics lists every topic that carries Avro business events, in pipeline order.
var BusinessTopics = []string{
	TopicOrdersCreated, TopicPaymentsCompleted, TopicPaymentsFailed, TopicPaymentsRefunded,
	TopicInventoryReserved, TopicInventoryRejected, TopicInventoryReleased,
	TopicOrdersShipped, TopicOrdersCancelled, TopicNotificationsSent,
}

// ValueTypes maps each Avro topic to the name of its value schema (schemas/gen/<Name>.avsc).
var ValueTypes = map[string]string{
	TopicOrdersCreated:     "OrderCreated",
	TopicPaymentsCompleted: "PaymentCompleted",
	TopicPaymentsFailed:    "PaymentFailed",
	TopicPaymentsRefunded:  "PaymentRefunded",
	TopicInventoryReserved: "InventoryReserved",
	TopicInventoryRejected: "InventoryRejected",
	TopicInventoryReleased: "InventoryReleased",
	TopicOrdersShipped:     "OrderShipped",
	TopicOrdersCancelled:   "OrderCancelled",
	TopicNotificationsSent: "NotificationSent",
	TopicPlatformControl:   "ControlSnapshot",
	TopicPlatformAudit:     "AuditEvent",
}

// Subject returns the schema registry subject of a topic's values (TopicNameStrategy).
func Subject(topic string) string { return topic + "-value" }

// Kafka record headers.
const (
	HeaderProducer           = "lab-producer"
	HeaderDLQSourceTopic     = "lab-dlq-source-topic"
	HeaderDLQSourcePartition = "lab-dlq-source-partition"
	HeaderDLQSourceOffset    = "lab-dlq-source-offset"
	HeaderDLQReason          = "lab-dlq-reason"
	HeaderDLQError           = "lab-dlq-error"
	HeaderDLQConsumer        = "lab-dlq-consumer"
)

// DeriveEventID returns the deterministic id of the event that `cause` produces as `outcome`.
// The same input always yields the same id, so redelivered inputs produce detectable duplicates.
// The result is formatted as a UUID (version 8, RFC 9562 custom).
func DeriveEventID(cause, outcome string) string {
	sum := sha256.Sum256([]byte(cause + "|" + outcome))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x80 // version 8
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 9562 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
