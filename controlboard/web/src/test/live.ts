/** Builders for live-view test data (business-shaped ticks and events). */
import type { BrokerState, EventsBatch, Kpis, LiveEvent, ServiceState, Tick } from "../api/types";
import { CONSUMES, SERVICE_NAMES, TOPIC_PARTITIONS } from "../lib/topology";

export const BASE_KPIS: Kpis = {
  ordersPerSec: 50,
  revenuePerMin: 180000,
  paymentSuccessRate: 0.95,
  reservationSuccessRate: 0.97,
  e2eP95Ms: 1400,
  inFlight: 60,
  dlqPerMin: 0,
  totalLag: 0,
};

export function service(name: string, patch: Partial<ServiceState> = {}): ServiceState {
  return { name, replicas: 2, readyReplicas: 2, paused: false, desiredGeneration: 1, appliedGeneration: 1, health: "up", knobs: {}, ...patch };
}

export function broker(id: number, patch: Partial<BrokerState> = {}): BrokerState {
  return { id, up: true, isController: id === 0, leaderCount: 17, underReplicated: 0, ...patch };
}

export function zeroLag(): Tick["lag"] {
  const lag: Tick["lag"] = {};
  for (const [g, topics] of Object.entries(CONSUMES)) {
    lag[g] = {};
    for (const t of topics) lag[g]![t] = new Array(TOPIC_PARTITIONS[t]).fill(0);
  }
  return lag;
}

export function tick(patch: Partial<Tick> = {}): Tick {
  return {
    kpis: { ...BASE_KPIS },
    services: SERVICE_NAMES.map((n) => service(n)),
    brokers: [0, 1, 2].map((id) => broker(id)),
    rates: { "orders.created": 50 },
    lag: zeroLag(),
    partitions: { ...TOPIC_PARTITIONS },
    ...patch,
  };
}

export function withLag(group: string, topic: string, partition: number, value: number, base: Tick["lag"] = zeroLag()): Tick["lag"] {
  base[group]![topic]![partition] = value;
  return base;
}

let offset = 0;
export function event(kind: LiveEvent["kind"], patch: Partial<LiveEvent> = {}): LiveEvent {
  return { topic: "orders.created", partition: 0, offset: ++offset, orderId: `order-${offset}`, kind, at: "2026-01-15T10:00:00Z", ...patch };
}

export function batch(items: LiveEvent[], scale = 1): EventsBatch {
  return { scale, items };
}
