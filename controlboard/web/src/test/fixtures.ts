/** Test data factories shaped exactly like the controlboard API contract. */
import type { Alert, BrokerState, Drill, Experiment, Kpis, Link, ScenarioDef, ServiceState, TimelineEntry, TuningResponse } from "../api/types";

export const T0 = Date.parse("2026-01-15T10:00:00.000Z");
export const iso = (offsetSec = 0) => new Date(T0 + offsetSec * 1000).toISOString();

export function service(over: Partial<ServiceState> = {}): ServiceState {
  return { name: "payment", replicas: 2, readyReplicas: 2, paused: false, desiredGeneration: 3, appliedGeneration: 3, health: "up", knobs: {}, ...over };
}

export function broker(over: Partial<BrokerState> = {}): BrokerState {
  return { id: 0, up: true, isController: false, leaderCount: 17, underReplicated: 0, ...over };
}

export function kpis(over: Partial<Kpis> = {}): Kpis {
  return { ordersPerSec: 50, revenuePerMin: 172000, paymentSuccessRate: 0.95, reservationSuccessRate: 0.97, e2eP95Ms: 1400, inFlight: 60, dlqPerMin: 0, totalLag: 0, ...over };
}

export function alert(over: Partial<Alert> = {}): Alert {
  return {
    fingerprint: "fp-1",
    name: "KafkaConsumerLagHigh",
    status: "firing",
    severity: "warning",
    component: "consumer",
    summary: "Consumer group payment lags 3,200 records",
    description: "The group consumes slower than records arrive.",
    startsAt: iso(-30),
    labels: { alertname: "KafkaConsumerLagHigh" },
    dashboardUrl: "http://grafana.localhost:8080/d/kafka-lag",
    runbookUrl: "https://runbooks/lag.md",
    generatorUrl: "http://vmalert.localhost:8080/rule",
    ...over,
  };
}

export function experiment(over: Partial<Experiment> = {}): Experiment {
  return {
    id: "exp-0001",
    scenario: "black-friday",
    name: "Black Friday",
    status: "running",
    params: { ordersPerSecond: 1000 },
    createdAt: iso(-10),
    startsAt: iso(-10),
    endsAt: iso(110),
    autoRecover: true,
    affected: ["order-generator"],
    observations: [],
    ...over,
  };
}

export function scenario(over: Partial<ScenarioDef> = {}): ScenarioDef {
  return {
    id: "black-friday",
    name: "Black Friday",
    description: "Orders jump to 1,000/s.",
    category: "traffic",
    severity: "high",
    destructive: false,
    defaultDurationSec: 180,
    params: [{ key: "ordersPerSecond", label: "Orders per second", type: "number", min: 100, max: 2000, step: 50, unit: "orders/s", default: 1000 }],
    expectedSymptoms: ["consumer lag grows"],
    dashboards: [{ title: "Business KPIs", url: "http://grafana.localhost:8080/d/lab-business" }],
    ...over,
  };
}

export const brokerCrash = (): ScenarioDef =>
  scenario({
    id: "broker-crash",
    name: "Broker Crash",
    description: "Kill one broker hard.",
    category: "broker",
    severity: "critical",
    destructive: true,
    defaultDurationSec: 60,
    params: [{ key: "broker", label: "Broker", type: "broker", options: ["0", "1", "2"], default: 1 }],
    expectedSymptoms: ["under-replicated partitions"],
  });

export function timeline(over: Partial<TimelineEntry> = {}): TimelineEntry {
  return { id: "tl-1", at: iso(0), kind: "experiment", severity: "warning", title: "Black Friday started", ...over };
}

export function link(over: Partial<Link> = {}): Link {
  return { id: "grafana", title: "Grafana", url: "http://grafana.localhost:8080", description: "Dashboards", icon: "gauge", health: "up", ...over };
}

export function drill(over: Partial<Drill> = {}): Drill {
  return {
    id: "drill-001",
    status: "running",
    difficulty: "medium",
    startedAt: iso(-42),
    hintsUsed: 0,
    hints: [],
    guesses: [],
    candidates: [
      { id: "hot-partition", name: "Hot Partition" },
      { id: "flash-sale", name: "Flash Sale" },
    ],
    ...over,
  };
}

export function tuning(): TuningResponse {
  return {
    settings: [
      { key: "acks", label: "Producer acks", type: "select", options: ["all", "leader", "none"], default: "all", lossy: ["leader", "none"], help: "Replicas that must confirm." },
      { key: "lingerMs", label: "linger.ms", type: "number", min: 0, max: 1000, step: 5, default: 5, help: "Batch wait." },
    ],
    services: {
      payment: { acks: "all", lingerMs: 5 },
      shipping: { acks: "all", lingerMs: 20 },
    },
  };
}
