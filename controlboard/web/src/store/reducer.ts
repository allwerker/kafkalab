/** Pure reducer: folds server messages into the board state. */
import type { Alert, BrokerState, Drill, Experiment, Kpis, Link, ScenarioDef, ServerMessage, ServiceState, SimulationStatus, Tick, TimelineEntry } from "../api/types";
import type { ConnectionStatus } from "../api/socket";

export const KPI_HISTORY = 90;
export const TIMELINE_CAP = 300;
export const ALERT_CAP = 200;

export interface KpiSample {
  at: number;
  kpis: Kpis;
}

export interface BoardData {
  connection: ConnectionStatus;
  retryInMs?: number;
  hasState: boolean;
  /** serverTime - local time at hello, ms */
  clockOffsetMs: number;
  simulation: SimulationStatus;
  chaosArmed: boolean;
  services: ServiceState[];
  brokers: BrokerState[];
  kpis: Kpis | null;
  kpiHistory: KpiSample[];
  lag: Tick["lag"];
  rates: Tick["rates"];
  partitions: Tick["partitions"];
  eventScale: number;
  experiments: Experiment[];
  alerts: Alert[];
  unreadAlerts: string[];
  links: Link[];
  drill?: Drill;
  timeline: TimelineEntry[];
  scenarios: ScenarioDef[];
}

export function initialBoard(): BoardData {
  return {
    connection: "connecting",
    hasState: false,
    clockOffsetMs: 0,
    simulation: "stopped",
    chaosArmed: false,
    services: [],
    brokers: [],
    kpis: null,
    kpiHistory: [],
    lag: {},
    rates: {},
    partitions: {},
    eventScale: 1,
    experiments: [],
    alerts: [],
    unreadAlerts: [],
    links: [],
    timeline: [],
    scenarios: [],
  };
}

function upsert<T>(list: T[], item: T, key: (t: T) => string): T[] {
  const k = key(item);
  const i = list.findIndex((x) => key(x) === k);
  if (i < 0) return [item, ...list];
  const next = [...list];
  next[i] = item;
  return next;
}

const byCreatedDesc = (a: Experiment, b: Experiment) => b.createdAt.localeCompare(a.createdAt);

export function upsertExperiment(list: Experiment[], e: Experiment): Experiment[] {
  return upsert(list, e, (x) => x.id).sort(byCreatedDesc);
}

export function upsertService(list: ServiceState[], s: ServiceState): ServiceState[] {
  const i = list.findIndex((x) => x.name === s.name);
  if (i < 0) return [...list, s];
  const next = [...list];
  next[i] = s;
  return next;
}

export function mergeTimeline(existing: TimelineEntry[], incoming: TimelineEntry[]): TimelineEntry[] {
  const seen = new Set(existing.map((t) => t.id));
  const merged = [...existing, ...incoming.filter((t) => !seen.has(t.id))];
  return merged.sort((a, b) => b.at.localeCompare(a.at) || b.id.localeCompare(a.id)).slice(0, TIMELINE_CAP);
}

function pushKpis(history: KpiSample[], at: number, kpis: Kpis): KpiSample[] {
  const next = [...history, { at, kpis }];
  return next.length > KPI_HISTORY ? next.slice(next.length - KPI_HISTORY) : next;
}

export function reduce(s: BoardData, msg: ServerMessage, now: number): BoardData {
  switch (msg.type) {
    case "hello":
      return { ...s, clockOffsetMs: Date.parse(msg.data.serverTime) - now };
    case "state": {
      const d = msg.data;
      const firing = new Set(d.alerts.filter((a) => a.status === "firing").map((a) => a.fingerprint));
      return {
        ...s,
        hasState: true,
        simulation: d.simulation,
        chaosArmed: d.chaosArmed,
        services: d.services,
        brokers: d.brokers,
        kpis: d.kpis,
        experiments: [...d.experiments].sort(byCreatedDesc),
        alerts: d.alerts,
        unreadAlerts: s.unreadAlerts.filter((f) => firing.has(f)),
        links: d.links,
        drill: d.drill,
      };
    }
    case "tick":
      return {
        ...s,
        kpis: msg.data.kpis,
        kpiHistory: pushKpis(s.kpiHistory, now, msg.data.kpis),
        services: msg.data.services,
        brokers: msg.data.brokers,
        lag: msg.data.lag,
        rates: msg.data.rates,
        partitions: msg.data.partitions,
      };
    case "events":
      return msg.data.scale === s.eventScale ? s : { ...s, eventScale: msg.data.scale };
    case "alert": {
      const a = msg.data;
      const prev = s.alerts.find((x) => x.fingerprint === a.fingerprint);
      const becameFiring = a.status === "firing" && prev?.status !== "firing";
      let unread = s.unreadAlerts;
      if (becameFiring && !unread.includes(a.fingerprint)) unread = [...unread, a.fingerprint];
      if (a.status === "resolved") unread = unread.filter((f) => f !== a.fingerprint);
      return { ...s, alerts: upsert(s.alerts, a, (x) => x.fingerprint).slice(0, ALERT_CAP), unreadAlerts: unread };
    }
    case "experiment":
      return { ...s, experiments: upsertExperiment(s.experiments, msg.data) };
    case "timeline":
      return { ...s, timeline: mergeTimeline(s.timeline, [msg.data]) };
    case "drill":
      return { ...s, drill: msg.data };
  }
}
