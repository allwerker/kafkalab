/** Pure domain helpers for the shell (alerts, experiments, services, scenarios). */
import type { Alert, Category, Experiment, ParamValue, ScenarioDef, ScenarioParam, ServiceState, TimelineEntry, TuningSetting } from "../api/types";

// ---------------------------------------------------------------- alerts

const ALERT_SEVERITY_RANK: Record<Alert["severity"], number> = { critical: 0, warning: 1, info: 2 };

export interface AlertGroups {
  firing: Alert[];
  pending: Alert[];
  resolved: Alert[];
}

/** Groups alerts by status; active groups by severity then newest first, resolved newest first. */
export function groupAlerts(alerts: Alert[]): AlertGroups {
  const bySeverity = (a: Alert, b: Alert) => ALERT_SEVERITY_RANK[a.severity] - ALERT_SEVERITY_RANK[b.severity] || b.startsAt.localeCompare(a.startsAt);
  const end = (a: Alert) => a.endsAt ?? a.startsAt;
  return {
    firing: alerts.filter((a) => a.status === "firing").sort(bySeverity),
    pending: alerts.filter((a) => a.status === "pending").sort(bySeverity),
    resolved: alerts.filter((a) => a.status === "resolved").sort((a, b) => end(b).localeCompare(end(a))),
  };
}

/** Alerts that are firing in `next` but were not firing in `prev` (for sound / notifications). */
export function newlyFiring(prev: Alert[], next: Alert[]): Alert[] {
  const was = new Set(prev.filter((a) => a.status === "firing").map((a) => a.fingerprint));
  return next.filter((a) => a.status === "firing" && !was.has(a.fingerprint));
}

// ---------------------------------------------------------------- experiments

export const ACTIVE_STATUSES: Experiment["status"][] = ["scheduled", "running", "recovering"];

export function isActive(e: Experiment): boolean {
  return ACTIVE_STATUSES.includes(e.status);
}

export interface Progress {
  /** 0..1 of the planned run */
  fraction: number;
  /** seconds until start (scheduled) or until end (running); 0 otherwise */
  remainingSec: number;
  overdue: boolean;
}

export function experimentProgress(e: Experiment, now: number): Progress {
  const start = Date.parse(e.startsAt);
  const end = Date.parse(e.endsAt);
  if (e.status === "scheduled") return { fraction: 0, remainingSec: Math.max(0, (start - now) / 1000), overdue: false };
  if (e.status !== "running") return { fraction: 1, remainingSec: 0, overdue: false };
  const fraction = Math.min(1, Math.max(0, (now - start) / Math.max(1, end - start)));
  return { fraction, remainingSec: Math.max(0, (end - now) / 1000), overdue: now > end };
}

export function confirmedCount(e: Experiment): number {
  return e.observations.filter((o) => o.confirmed).length;
}

// ---------------------------------------------------------------- services

export function isApplying(s: ServiceState): boolean {
  return s.appliedGeneration < s.desiredGeneration;
}

// ---------------------------------------------------------------- scenarios

export const CATEGORY_ORDER: Category[] = ["traffic", "service", "kubernetes", "broker", "network", "data"];

export const CATEGORY_LABEL: Record<Category, string> = {
  traffic: "Traffic",
  service: "Services",
  kubernetes: "Kubernetes",
  broker: "Kafka brokers",
  network: "Network",
  data: "Data",
};

export function groupScenarios(scenarios: ScenarioDef[]): { category: Category; scenarios: ScenarioDef[] }[] {
  return CATEGORY_ORDER.map((category) => ({ category, scenarios: scenarios.filter((s) => s.category === category) })).filter((g) => g.scenarios.length > 0);
}

export function categoryMap(scenarios: ScenarioDef[]): Record<string, Category> {
  return Object.fromEntries(scenarios.map((s) => [s.id, s.category]));
}

export function defaultParams(params: ScenarioParam[]): Record<string, ParamValue> {
  return Object.fromEntries(params.map((p) => [p.key, p.default]));
}

/** Coerces a raw form value into the param's type (numbers clamped to min/max). */
export function coerceParam(p: ScenarioParam, raw: string | boolean): ParamValue {
  if (p.type === "boolean") return raw === true || raw === "true";
  if (p.type === "number" || p.type === "broker") {
    const n = Number(raw);
    if (Number.isNaN(n)) return p.default;
    return Math.min(p.max ?? n, Math.max(p.min ?? n, n));
  }
  return String(raw);
}

/** Why a scenario cannot be started right now, or null. */
export function startBlocker(s: ScenarioDef, chaosArmed: boolean, confirmText: string): string | null {
  if (!s.destructive) return null;
  if (!chaosArmed) return "Arm chaos to run destructive scenarios";
  if (confirmText !== s.id) return `Type "${s.id}" to confirm`;
  return null;
}

// ---------------------------------------------------------------- tuning

export function isLossy(setting: TuningSetting, value: number | string | undefined): boolean {
  return value !== undefined && !!setting.lossy?.includes(String(value));
}

// ---------------------------------------------------------------- timeline

export const TIMELINE_KINDS: TimelineEntry["kind"][] = ["experiment", "config", "alert", "recovery", "simulation", "drill", "system"];

export function filterTimeline(entries: TimelineEntry[], kinds: Set<string>, experimentId?: string): TimelineEntry[] {
  return entries.filter((e) => kinds.has(e.kind) && (!experimentId || e.experimentId === experimentId));
}
