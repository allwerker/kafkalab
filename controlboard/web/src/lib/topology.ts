/**
 * Static facts about the Kafka Lab business topology (docs/contracts/events.md, deploy/topics.yaml,
 * docs/contracts/platform.md). Shared by the UI, the live view model and the mock backend.
 */

export const SERVICE_NAMES = [
  "order-generator",
  "payment",
  "inventory",
  "shipping",
  "notification",
  "analytics",
] as const;
export type ServiceName = (typeof SERVICE_NAMES)[number];

export const SERVICE_LABELS: Record<ServiceName, string> = {
  "order-generator": "Order Generator",
  payment: "Payment",
  inventory: "Inventory",
  shipping: "Shipping",
  notification: "Notification",
  analytics: "Analytics",
};

/** Consumer groups that report lag (service name = group name). */
export const CONSUMER_GROUPS = ["payment", "inventory", "shipping", "notification", "analytics"] as const;
export type ConsumerGroup = (typeof CONSUMER_GROUPS)[number];

export const TOPIC_PARTITIONS: Record<string, number> = {
  "orders.created": 6,
  "payments.completed": 6,
  "payments.failed": 3,
  "payments.refunded": 3,
  "inventory.reserved": 6,
  "inventory.rejected": 3,
  "inventory.released": 3,
  "orders.shipped": 6,
  "orders.cancelled": 3,
  "notifications.sent": 6,
  "orders.dlq": 3,
};

export const BUSINESS_TOPICS = Object.keys(TOPIC_PARTITIONS);

/** Which topics each consumer group reads (events.md, "Who consumes what"). */
export const CONSUMES: Record<ConsumerGroup, string[]> = {
  payment: ["orders.created", "orders.cancelled"],
  inventory: ["orders.created", "orders.cancelled"],
  shipping: ["payments.completed", "payments.failed", "inventory.reserved", "inventory.rejected"],
  notification: ["orders.created", "orders.shipped", "orders.cancelled", "payments.refunded"],
  analytics: BUSINESS_TOPICS,
};

/** Producer of each topic. */
export const PRODUCER: Record<string, ServiceName> = {
  "orders.created": "order-generator",
  "payments.completed": "payment",
  "payments.failed": "payment",
  "payments.refunded": "payment",
  "inventory.reserved": "inventory",
  "inventory.rejected": "inventory",
  "inventory.released": "inventory",
  "orders.shipped": "shipping",
  "orders.cancelled": "shipping",
  "notifications.sent": "notification",
  "orders.dlq": "payment",
};

export const GRAFANA_BASE = "http://grafana.localhost:8080";

export const DASHBOARDS: { uid: string; title: string; folder: "Kafka (bundle)" | "Kafka Lab" }[] = [
  { uid: "lab-home", title: "Lab Home", folder: "Kafka Lab" },
  { uid: "lab-business", title: "Business KPIs", folder: "Kafka Lab" },
  { uid: "lab-services", title: "Services", folder: "Kafka Lab" },
  { uid: "lab-chaos", title: "Chaos Experiments", folder: "Kafka Lab" },
  { uid: "lab-footprint", title: "Footprint", folder: "Kafka Lab" },
  { uid: "lab-monitoring", title: "Monitoring Health", folder: "Kafka Lab" },
  { uid: "lab-postgres", title: "PostgreSQL", folder: "Kafka Lab" },
  { uid: "lab-logs", title: "Logs", folder: "Kafka Lab" },
  { uid: "kafka-overview", title: "Kafka Overview", folder: "Kafka (bundle)" },
  { uid: "kafka-broker", title: "Brokers", folder: "Kafka (bundle)" },
  { uid: "kafka-kraft", title: "KRaft Controller", folder: "Kafka (bundle)" },
  { uid: "kafka-topics", title: "Topics", folder: "Kafka (bundle)" },
  { uid: "kafka-lag", title: "Consumer Lag", folder: "Kafka (bundle)" },
  { uid: "kafka-e2e", title: "End-to-End Latency", folder: "Kafka (bundle)" },
  { uid: "kafka-jvm", title: "JVM", folder: "Kafka (bundle)" },
  { uid: "kafka-pods", title: "Pods", folder: "Kafka (bundle)" },
];

/** Deep link into a Grafana dashboard, relative to the Grafana base URL (from /api/links when known). */
export function dashboardUrl(uid: string, base: string = GRAFANA_BASE, vars: Record<string, string> = {}): string {
  const query = Object.entries(vars)
    .map(([k, v]) => `var-${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");
  return `${base.replace(/\/$/, "")}/d/${uid}${query ? `?${query}` : ""}`;
}
