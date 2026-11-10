/** World layout of the logistics hub: station positions, lane bundles, brokers, DLQ pit. */
import type { ServiceName } from "../../lib/topology";
import { curvedPath, dist, type Path, type Vec } from "./geometry";

export const WORLD = { width: 1600, height: 900 } as const;

export const STATION_POS: Record<ServiceName, Vec> = {
  "order-generator": { x: 150, y: 450 },
  payment: { x: 560, y: 215 },
  inventory: { x: 560, y: 685 },
  shipping: { x: 1080, y: 450 },
  notification: { x: 1420, y: 215 },
  analytics: { x: 1420, y: 685 },
};

export const STATION_RADIUS = 54;
export const BROKER_POS: Vec[] = [
  { x: 752, y: 450 },
  { x: 822, y: 450 },
  { x: 892, y: 450 },
];
export const BROKER_SIZE = { w: 50, h: 92 };
export const DLQ_POS: Vec = { x: 822, y: 805 };
export const TRUCK_DOCK: Vec = { x: 1160, y: 470 };
export const PARTITION_GAP = 5;

export interface LaneDef {
  topic: string;
  from: ServiceName;
  to: ServiceName;
  /** consumer group whose lag piles up in front of `to` */
  group: string;
  bend: number;
  shift: number;
  color: number;
}

export const COLORS = {
  cyan: 0x22d3ee,
  gold: 0xfbbf24,
  red: 0xf43f5e,
  green: 0x34d399,
  orange: 0xfb923c,
  magenta: 0xe879f9,
  grey: 0x94a3b8,
  blue: 0x60a5fa,
  violet: 0xa78bfa,
} as const;

export const LANE_DEFS: LaneDef[] = [
  { topic: "orders.created", from: "order-generator", to: "payment", group: "payment", bend: 0.1, shift: 0, color: COLORS.cyan },
  { topic: "orders.created", from: "order-generator", to: "inventory", group: "inventory", bend: -0.1, shift: 0, color: COLORS.cyan },
  { topic: "payments.completed", from: "payment", to: "shipping", group: "shipping", bend: -0.06, shift: -12, color: COLORS.gold },
  { topic: "payments.failed", from: "payment", to: "shipping", group: "shipping", bend: -0.06, shift: 14, color: COLORS.red },
  { topic: "inventory.reserved", from: "inventory", to: "shipping", group: "shipping", bend: 0.06, shift: 12, color: COLORS.green },
  { topic: "inventory.rejected", from: "inventory", to: "shipping", group: "shipping", bend: 0.06, shift: -14, color: COLORS.orange },
  { topic: "orders.shipped", from: "shipping", to: "notification", group: "notification", bend: 0.05, shift: -10, color: COLORS.magenta },
  { topic: "orders.cancelled", from: "shipping", to: "notification", group: "notification", bend: 0.05, shift: 12, color: COLORS.grey },
  { topic: "payments.refunded", from: "payment", to: "notification", group: "notification", bend: -0.1, shift: 0, color: COLORS.gold },
  { topic: "notifications.sent", from: "notification", to: "analytics", group: "analytics", bend: -0.08, shift: 0, color: COLORS.blue },
];

export interface Lane {
  id: string;
  topic: string;
  group: string;
  partition: number;
  from: ServiceName;
  to: ServiceName;
  color: number;
  path: Path;
}

export function laneId(topic: string, to: string, partition: number): string {
  return `${topic}>${to}#${partition}`;
}

/** Builds one lane per partition of every lane bundle (partition counts from tick.partitions). */
export function buildLanes(partitions: Record<string, number>): Lane[] {
  const lanes: Lane[] = [];
  for (const def of LANE_DEFS) {
    const n = Math.max(1, partitions[def.topic] ?? 1);
    const a = STATION_POS[def.from];
    const b = STATION_POS[def.to];
    const d = dist(a, b);
    const ux = (b.x - a.x) / d;
    const uy = (b.y - a.y) / d;
    const start = { x: a.x + ux * (STATION_RADIUS + 6), y: a.y + uy * (STATION_RADIUS + 6) };
    const end = { x: b.x - ux * (STATION_RADIUS + 12), y: b.y - uy * (STATION_RADIUS + 12) };
    for (let p = 0; p < n; p++) {
      const offset = def.shift + (p - (n - 1) / 2) * PARTITION_GAP;
      lanes.push({
        id: laneId(def.topic, def.to, p),
        topic: def.topic,
        group: def.group,
        partition: p,
        from: def.from,
        to: def.to,
        color: def.color,
        path: curvedPath(start, end, def.bend, offset),
      });
    }
  }
  return lanes;
}
