import type { Link, TuningSetting } from "../src/api/types.ts";

export const LINKS: Link[] = [
  { id: "grafana", title: "Grafana", url: "http://grafana.localhost:8080", description: "Dashboards for Kafka, services and business KPIs", icon: "gauge", health: "up" },
  { id: "akhq", title: "AKHQ", url: "http://akhq.localhost:8080", description: "Browse topics, consumer groups and messages", icon: "layers", health: "up" },
  { id: "vmui", title: "VictoriaMetrics", url: "http://vm.localhost:8080/vmui", description: "Ad-hoc MetricsQL queries", icon: "database", health: "up" },
  { id: "vmagent", title: "vmagent targets", url: "http://vmagent.localhost:8080/targets", description: "Scrape targets and their health", icon: "radar", health: "up" },
  { id: "vmalert", title: "vmalert", url: "http://vmalert.localhost:8080", description: "Alerting and recording rules", icon: "bell-ring", health: "up" },
  { id: "alertmanager", title: "Alertmanager", url: "http://alertmanager.localhost:8080", description: "Silences, routing and notifications", icon: "siren", health: "up" },
  { id: "logs", title: "VictoriaLogs", url: "http://logs.localhost:8080/select/vmui", description: "Structured logs of every pod", icon: "scroll-text", health: "up" },
  { id: "traces", title: "VictoriaTraces", url: "http://traces.localhost:8080", description: "Distributed traces (Jaeger UI API)", icon: "waypoints", health: "degraded" },
];

export const TUNING_SETTINGS: TuningSetting[] = [
  { key: "acks", label: "Producer acks", type: "select", options: ["all", "leader", "none"], default: "all", lossy: ["leader", "none"], help: "How many replicas must confirm a write. Anything but 'all' disables idempotence and can lose records on broker failure." },
  { key: "lingerMs", label: "linger.ms", type: "number", min: 0, max: 1000, step: 5, default: 5, help: "How long the producer waits to fill a batch. Higher = bigger batches, more latency." },
  { key: "batchMaxBytes", label: "Max batch bytes", type: "number", min: 16384, max: 16777216, step: 16384, default: 1048576, help: "Upper bound of one record batch per partition." },
  { key: "compression", label: "Compression", type: "select", options: ["none", "gzip", "snappy", "lz4", "zstd"], default: "snappy", help: "Codec for record batches. zstd = smallest, lz4 = fastest." },
  { key: "fetchMaxBytes", label: "Fetch max bytes", type: "number", min: 1048576, max: 104857600, step: 1048576, default: 52428800, help: "Max bytes a single fetch response may return." },
  { key: "fetchMaxWaitMs", label: "Fetch max wait", type: "number", min: 0, max: 10000, step: 50, default: 5000, help: "How long the broker may hold a fetch waiting for fetch.min.bytes." },
  { key: "fetchMinBytes", label: "Fetch min bytes", type: "number", min: 1, max: 1048576, step: 1024, default: 1, help: "Broker waits until this many bytes are available (or the max wait passed)." },
  { key: "maxPollRecords", label: "Max poll records", type: "number", min: 1, max: 10000, step: 10, default: 500, help: "Records handed to the handler per poll. Small values reduce throughput." },
];

export function tuningDefaults(): Record<string, number | string> {
  return Object.fromEntries(TUNING_SETTINGS.map((s) => [s.key, s.default]));
}
