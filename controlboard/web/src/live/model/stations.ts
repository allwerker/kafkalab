import type { Category, Experiment, ServiceState } from "../../api/types";
import type { StationStatus, Weather, WeatherKind } from "./types";

/** Visual status of a station: down beats paused beats degraded. */
export function stationStatus(s: ServiceState | undefined): StationStatus {
  if (!s) return "unknown";
  if (s.replicas === 0 || s.readyReplicas === 0 || s.health === "down") return "down";
  if (s.paused) return "paused";
  if (s.health === "degraded") return "degraded";
  if (s.health === "unknown") return "unknown";
  return "ok";
}

/** Topics a station produces (its throughput in the live view). */
export const PRODUCES: Record<string, string[]> = {
  "order-generator": ["orders.created"],
  payment: ["payments.completed", "payments.failed", "payments.refunded"],
  inventory: ["inventory.reserved", "inventory.rejected", "inventory.released"],
  shipping: ["orders.shipped", "orders.cancelled"],
  notification: ["notifications.sent"],
};

export function stationRate(id: string, rates: Record<string, number>): number {
  const topics = PRODUCES[id] ?? Object.keys(rates);
  return topics.reduce((sum, t) => sum + (rates[t] ?? 0), 0);
}

const WEATHER_BY_CATEGORY: Record<Category, WeatherKind> = {
  traffic: "rush",
  network: "glitch",
  broker: "lightning",
  service: "smoke",
  kubernetes: "smoke",
  data: "matrix",
};

/** Chaos weather overlays for every running experiment. Hidden drill scenarios produce fog. */
export function weatherFor(experiments: Experiment[], categories: Record<string, Category>): Weather[] {
  return experiments
    .filter((e) => e.status === "running")
    .map((e) => {
      const category = categories[e.scenario];
      if (!category) return { kind: "fog" as const, targets: [], intensity: 0.6, experimentId: e.id };
      const kind = WEATHER_BY_CATEGORY[category];
      const targets = kind === "rush" ? ["order-generator"] : kind === "matrix" ? [] : e.affected;
      return { kind, targets, intensity: 1, experimentId: e.id };
    });
}
