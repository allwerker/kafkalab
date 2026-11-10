/**
 * Deterministic commerce + Kafka simulation behind the mock controlboard backend.
 *
 * Two layers:
 *  - a fluid model of real volumes (orders/s, per-partition backlog per consumer group), and
 *  - a sampled set of individual orders that ride through FIFO queues which drain at the same
 *    fraction as the fluid backlog. Sampled orders produce the LiveEvents for the live view and
 *    the /api/orders/{id} timelines, so piles in the live view match the reported lag.
 *
 * Time is virtual: the caller advances it with step(); the server does so in real time, the
 * fixture recorder as fast as possible.
 */
import type {
  Alert,
  BrokerState,
  Drill,
  Experiment,
  ExperimentReport,
  Health,
  Kpis,
  LeaderboardEntry,
  LiveEvent,
  Observation,
  OrderTimeline,
  ParamValue,
  ScenarioDef,
  ServerMessage,
  ServiceState,
  State,
  Tick,
  TimelineEntry,
} from "../src/api/types.ts";
import { CONSUMER_GROUPS, CONSUMES, SERVICE_NAMES, TOPIC_PARTITIONS, dashboardUrl, type ConsumerGroup } from "../src/lib/topology.ts";
import { LINKS, TUNING_SETTINGS, tuningDefaults } from "./catalog.ts";
import { Rng } from "./rng.ts";
import { DRILL_POOL, HINTS, SCENARIOS, scenarioById } from "./scenarios.ts";

type KnobValue = number | boolean | string;
type Knobs = Record<string, KnobValue>;

export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
    message: string,
  ) {
    super(message);
  }
}

const KNOB_DEFAULTS: Knobs = {
  paused: false,
  processingLatencyMs: 0,
  processingJitterMs: 0,
  processingErrorProbability: 0,
  providerDown: false,
  dbLatencyMs: 0,
  netLatencyMs: 0,
  netDropProbability: 0,
  warehouseOutage: false,
  popularSkuProbability: 0,
  hotPartitionRatio: 0,
  paddingBytes: 0,
  duplicateProbability: 0,
  poisonProbability: 0,
};
const NULLABLE_KNOBS = new Set(["businessFailureProbability", "ordersPerSecond", "popularSku", "hotPartition"]);
export const KNOB_KEYS = new Set([...Object.keys(KNOB_DEFAULTS), ...NULLABLE_KNOBS]);

const DEFAULT_REPLICAS: Record<string, number> = {
  "order-generator": 1,
  payment: 2,
  inventory: 2,
  shipping: 2,
  notification: 1,
  analytics: 1,
};
const BASE_CAPACITY: Record<ConsumerGroup, number> = {
  payment: 220,
  inventory: 200,
  shipping: 520,
  notification: 320,
  analytics: 1400,
};
const BASE_RATE = 50;
const AVG_ORDER_EUR = 62.4;
const TOTAL_PARTITIONS = 50; // business topics + platform.control + platform.audit (topics.yaml)
const REGIONS = ["eu-west", "eu-central", "eu-north", "us-east"];
const CARRIERS = ["DHL", "UPS", "DPD", "GLS", "Hermes"];

interface Svc {
  name: string;
  replicas: number;
  ready: number;
  manual: Knobs;
  desiredGeneration: number;
  appliedGeneration: number;
  applyAt: number;
  lastKey: string;
  readyAt: number;
}

interface Effects {
  knobs: Record<string, Knobs>;
  down: Set<number>;
  crash: boolean;
  zeroReady: Set<string>;
  rebalancing: Set<string>;
}

interface Exp {
  e: Experiment;
  def: ScenarioDef;
  hidden: boolean;
  startsAtMs: number;
  endsAtMs: number;
  recoveringSince?: number;
  peakLag: number;
  failedOrders: number;
  lostRevenue: number;
  firstAlertAt?: number;
  alerts: Set<string>;
  restarts: number;
  confirmedAt: Map<string, string>;
  drillId?: string;
}

interface SOrder {
  id: string;
  p: number;
  valueCents: number;
  region: string;
  paid?: boolean;
  reserved?: boolean;
  terminal?: "shipped" | "cancelled";
}

interface QItem {
  o: SOrder;
  readyAt: number;
}

interface AlertInst {
  alert: Alert;
  since: number;
  resolvedAt?: number;
}

interface Rule {
  name: string;
  severity: Alert["severity"];
  component: string;
  dashboard: string;
  forSec: number;
  summary: (labels: Record<string, string>, v: number) => string;
  description: string;
  eval: (s: Sim) => { labels: Record<string, string>; value: number; active: boolean }[];
}

export interface SimOptions {
  seed?: number;
  startMs?: number;
  sampledOrdersPerSec?: number;
  maxItems?: number;
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

function round(n: number, d = 0): number {
  const f = 10 ** d;
  return Math.round(n * f) / f;
}

function hashStr(s: string): string {
  let h = 2166136261;
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619);
  return (h >>> 0).toString(16).padStart(8, "0");
}

export class Sim {
  rng: Rng;
  now: number;
  private stepCount = 0;
  simulation: "running" | "stopped" = "running";
  chaosArmed = false;
  svcs = new Map<string, Svc>();
  brokerUp = [true, true, true];
  brokerLeaders = [17, 17, 16];
  controller = 0;
  private disruptionUntil = 0;
  private catchUpUntil = [0, 0, 0];
  private brokerDownSince: (number | null)[] = [null, null, null];
  private writeBlocked = false;
  backlog = new Map<string, number[]>();
  private lastProduced = new Map<string, number[]>();
  private queues = new Map<string, QItem[]>();
  private carry = new Map<string, number>();
  private pPaid = 0.95;
  private pRes = 0.97;
  private cancelPaidShare = 0.5;
  private cancelResShare = 0.5;
  private sampleAcc = 0;
  eventScale = 1;
  kpis: Kpis = {
    ordersPerSec: BASE_RATE,
    revenuePerMin: BASE_RATE * 60 * AVG_ORDER_EUR * 0.92,
    paymentSuccessRate: 0.95,
    reservationSuccessRate: 0.97,
    e2eP95Ms: 1400,
    inFlight: 60,
    dlqPerMin: 0,
    totalLag: 0,
  };
  private acc = { created: 0, paid: 0, failed: 0, reserved: 0, rejected: 0, dlq: 0, procPayment: 0 };
  rates: Record<string, number> = {};
  private stepProduced: Record<string, number> = {};
  experiments: Exp[] = [];
  private expCounter = 0;
  alerts = new Map<string, AlertInst>();
  private testAlertUntil = 0;
  private testAlertAt = 0;
  timeline: TimelineEntry[] = [];
  private tlCounter = 0;
  drill?: Drill;
  private drillScenario?: string;
  private drillCounter = 0;
  private drillPlayer = "";
  private drillExpiresAt = 0;
  leaderboard: LeaderboardEntry[] = [];
  tuning = new Map<string, Record<string, number | string>>();
  private orders = new Map<string, { events: LiveEvent[]; traceId: string }>();
  private pendingEvents: LiveEvent[] = [];
  private outbox: ServerMessage[] = [];
  private offsets = new Map<string, number>();
  private restartCounter = new Map<string, number>();
  private crashState = new Map<string, number>();
  private sampledPerSec: number;
  private maxItems: number;
  private rules: Rule[];

  constructor(opts: SimOptions = {}) {
    this.rng = new Rng(opts.seed ?? 42);
    this.now = opts.startMs ?? Date.now();
    this.sampledPerSec = opts.sampledOrdersPerSec ?? 14;
    this.maxItems = opts.maxItems ?? 40;
    for (const name of SERVICE_NAMES) {
      const replicas = DEFAULT_REPLICAS[name] ?? 1;
      this.svcs.set(name, { name, replicas, ready: replicas, manual: {}, desiredGeneration: 1, appliedGeneration: 1, applyAt: 0, lastKey: "[]", readyAt: 0 });
      this.tuning.set(name, tuningDefaults());
    }
    for (const g of CONSUMER_GROUPS) for (const t of CONSUMES[g]) this.backlog.set(`${g}|${t}`, new Array(TOPIC_PARTITIONS[t]).fill(0));
    for (const t of Object.keys(TOPIC_PARTITIONS)) this.offsets.set(t, 0);
    this.rules = this.buildRules();
    this.addTimeline("simulation", "info", "Simulation started", "Baseline: 50 orders/s, 6 services, 3 brokers");
  }

  // ------------------------------------------------------------------ public API

  drain(): ServerMessage[] {
    const out = this.outbox;
    this.outbox = [];
    return out;
  }

  emit(msg: ServerMessage): void {
    this.outbox.push(msg);
  }

  private push<T extends ServerMessage["type"]>(type: T, data: Extract<ServerMessage, { type: T }>["data"]): void {
    this.outbox.push({ type, ts: iso(this.now), data } as ServerMessage);
  }

  state(): State {
    const s: State = {
      simulation: this.simulation,
      chaosArmed: this.chaosArmed,
      services: this.serviceStates(),
      brokers: this.brokerStates(),
      kpis: { ...this.kpis },
      experiments: this.experimentList(),
      alerts: this.alertList(),
      links: LINKS.map((l) => ({ ...l })),
    };
    if (this.drill) s.drill = this.drill;
    return s;
  }

  tick(): Tick {
    const lag: Tick["lag"] = {};
    for (const g of CONSUMER_GROUPS) {
      lag[g] = {};
      for (const t of CONSUMES[g]) lag[g]![t] = (this.backlog.get(`${g}|${t}`) ?? []).map((v) => Math.round(v));
    }
    return {
      kpis: { ...this.kpis },
      services: this.serviceStates(),
      brokers: this.brokerStates(),
      rates: { ...this.rates },
      lag,
      partitions: { ...TOPIC_PARTITIONS },
    };
  }

  hello() {
    return { type: "hello" as const, ts: iso(this.now), data: { version: 1, serverTime: iso(this.now) } };
  }

  scenarios(): ScenarioDef[] {
    return SCENARIOS;
  }

  timelineList(limit = 200): TimelineEntry[] {
    return this.timeline.slice(0, limit);
  }

  alertList(): Alert[] {
    return [...this.alerts.values()].map((a) => a.alert).sort((a, b) => b.startsAt.localeCompare(a.startsAt));
  }

  experimentList(): Experiment[] {
    return this.experiments.slice(-20 - this.activeExperiments().length).map((x) => x.e).reverse();
  }

  order(id: string): OrderTimeline {
    const o = this.orders.get(id);
    if (!o) throw new ApiError(404, "not_found", `order ${id} is not in the recent-orders ring`);
    return {
      orderId: id,
      events: o.events,
      traceId: o.traceId,
      traceUrl: `http://traces.localhost:8080/trace/${o.traceId}`,
      logsUrl: `http://logs.localhost:8080/select/vmui/#/?query=${encodeURIComponent(`orderId:"${id}"`)}`,
    };
  }

  /** Any recent order id (used by fixtures and tests). */
  anyOrderId(): string | undefined {
    let last: string | undefined;
    for (const [id, o] of this.orders) if (o.events.length >= 4) last = id;
    return last;
  }

  setSimulation(action: string): { simulation: "running" | "stopped" } {
    if (action === "start") {
      this.simulation = "running";
      this.addTimeline("simulation", "success", "Simulation started");
    } else if (action === "stop") {
      this.simulation = "stopped";
      this.addTimeline("simulation", "warning", "Simulation stopped", "Order generator idle; consumers drain their backlog");
    } else if (action === "reset") {
      this.panic(false);
      for (const b of this.backlog.values()) b.fill(0);
      for (const q of this.queues.values()) q.length = 0;
      this.simulation = "running";
      this.addTimeline("simulation", "info", "Simulation reset", "Backlogs cleared, baseline restored");
    } else {
      throw new ApiError(400, "invalid_action", `unknown simulation action "${action}"`);
    }
    return { simulation: this.simulation };
  }

  arm(armed: boolean): { armed: boolean } {
    if (this.chaosArmed !== armed) {
      this.chaosArmed = armed;
      this.addTimeline("config", armed ? "warning" : "info", armed ? "Chaos armed" : "Chaos disarmed", armed ? "Destructive scenarios can now be started" : undefined);
    }
    return { armed };
  }

  panic(record = true): { reverted: number } {
    let reverted = 0;
    for (const x of this.activeExperiments()) {
      this.finishExperiment(x, "aborted");
      reverted++;
    }
    for (const s of this.svcs.values()) {
      if (Object.keys(s.manual).length || s.replicas !== DEFAULT_REPLICAS[s.name]) reverted++;
      s.manual = {};
      s.replicas = DEFAULT_REPLICAS[s.name] ?? 1;
      this.tuning.set(s.name, tuningDefaults());
    }
    this.chaosArmed = false;
    if (record) this.addTimeline("system", "critical", "PANIC — everything reverted", `${reverted} change(s) reverted, chaos disarmed`);
    return { reverted };
  }

  putKnobs(name: string, body: Record<string, unknown>): ServiceState {
    const s = this.svc(name);
    const changes: string[] = [];
    for (const [k, v] of Object.entries(body)) {
      if (!KNOB_KEYS.has(k)) throw new ApiError(400, "unknown_knob", `unknown knob "${k}"`);
      if (v === null || v === KNOB_DEFAULTS[k]) delete s.manual[k];
      else if (typeof v === "number" || typeof v === "boolean" || typeof v === "string") s.manual[k] = v;
      else throw new ApiError(400, "invalid_value", `invalid value for "${k}"`);
      changes.push(`${k}=${String(v)}`);
    }
    this.addTimeline("config", "info", `${name}: knobs changed`, changes.join(", "));
    this.updateGenerations();
    return this.serviceState(s);
  }

  resetKnobs(name: string): ServiceState {
    const s = this.svc(name);
    s.manual = {};
    this.addTimeline("config", "info", `${name}: knobs reset to baseline`);
    this.updateGenerations();
    return this.serviceState(s);
  }

  scale(name: string, replicas: unknown): ServiceState {
    const s = this.svc(name);
    if (typeof replicas !== "number" || !Number.isInteger(replicas) || replicas < 0 || replicas > 6) throw new ApiError(400, "invalid_replicas", "replicas must be an integer 0..6");
    const prev = s.replicas;
    s.replicas = replicas;
    if (replicas < s.ready) s.ready = replicas;
    s.readyAt = this.now + 2000;
    this.addTimeline("config", replicas === 0 ? "warning" : "info", `${name} scaled ${prev} → ${replicas}`);
    return this.serviceState(s);
  }

  tuningResponse() {
    return { settings: TUNING_SETTINGS, services: Object.fromEntries([...this.tuning.entries()].map(([k, v]) => [k, { ...v }])) };
  }

  putTuning(name: string, body: Record<string, unknown>) {
    this.svc(name);
    const t = this.tuning.get(name)!;
    const changes: string[] = [];
    for (const [k, v] of Object.entries(body)) {
      const setting = TUNING_SETTINGS.find((s) => s.key === k);
      if (!setting) throw new ApiError(400, "unknown_setting", `unknown tuning key "${k}"`);
      if (v === null) t[k] = setting.default;
      else if (setting.type === "number" && typeof v === "number") t[k] = Math.max(setting.min ?? v, Math.min(setting.max ?? v, v));
      else if (setting.type === "select" && typeof v === "string" && setting.options?.includes(v)) t[k] = v;
      else throw new ApiError(400, "invalid_value", `invalid value for "${k}"`);
      changes.push(`${k}=${String(t[k])}`);
    }
    const lossy = TUNING_SETTINGS.some((s) => s.lossy?.includes(String(t[s.key])));
    this.addTimeline("config", lossy ? "warning" : "info", `${name}: client tuning changed`, changes.join(", ") + (lossy ? " — delivery guarantees weakened" : ""));
    const s = this.svcs.get(name)!;
    s.desiredGeneration++;
    s.applyAt = this.now + 1500;
    return this.tuningResponse();
  }

  startExperiment(body: Record<string, unknown>, opts: { drill?: boolean } = {}): Experiment {
    const scenarioId = String(body.scenario ?? "");
    const def = scenarioById(scenarioId);
    if (!def) throw new ApiError(404, "unknown_scenario", `unknown scenario "${scenarioId}"`);
    if (!opts.drill && def.destructive) {
      if (!this.chaosArmed) throw new ApiError(409, "chaos_disarmed", "Arm chaos before starting a destructive scenario.");
      if (body.confirm !== def.id) throw new ApiError(400, "confirmation_required", `Type "${def.id}" to confirm this destructive scenario.`);
    }
    if (def.category === "broker" && this.activeExperiments().some((x) => x.def.category === "broker")) {
      throw new ApiError(409, "broker_fault_active", "Another broker fault is already active. Stop it first.");
    }
    const params: Record<string, ParamValue> = {};
    const given = (body.params ?? {}) as Record<string, unknown>;
    for (const p of def.params) {
      const v = given[p.key];
      params[p.key] = typeof v === "number" || typeof v === "boolean" || typeof v === "string" ? v : p.default;
    }
    const durationSec = Math.max(5, Math.min(3600, Number(body.durationSec ?? def.defaultDurationSec) || def.defaultDurationSec));
    const delay = Math.max(0, Number(body.startDelaySec ?? 0) || 0);
    const startsAtMs = this.now + delay * 1000;
    const id = `exp-${String(++this.expCounter).padStart(4, "0")}`;
    const hidden = !!opts.drill;
    const e: Experiment = {
      id,
      scenario: hidden ? "hidden" : def.id,
      name: hidden ? `Incident Drill #${this.drillCounter}` : def.name,
      status: delay > 0 ? "scheduled" : "running",
      params: hidden ? {} : params,
      createdAt: iso(this.now),
      startsAt: iso(startsAtMs),
      endsAt: iso(startsAtMs + durationSec * 1000),
      autoRecover: body.autoRecover !== false,
      affected: hidden ? [] : this.affectedOf(def, params),
      observations: [],
    };
    const x: Exp = { e, def, hidden, startsAtMs, endsAtMs: startsAtMs + durationSec * 1000, peakLag: 0, failedOrders: 0, lostRevenue: 0, alerts: new Set(), restarts: 0, confirmedAt: new Map() };
    this.experiments.push(x);
    if (hidden) {
      this.addTimeline("drill", "warning", `${e.name} started`, "Something is wrong. Find out what.", id);
    } else if (delay > 0) {
      this.addTimeline("experiment", "info", `${def.name} scheduled`, `starts in ${delay}s, runs ${durationSec}s`, id);
    } else {
      this.addTimeline("experiment", def.severity === "critical" ? "critical" : "warning", `${def.name} started`, `${durationSec}s, ${e.autoRecover ? "auto-recover" : "manual recovery"}`, id);
    }
    this.updateObservations(x);
    this.push("experiment", e);
    this.updateGenerations();
    return e;
  }

  stopExperiment(id: string): Experiment {
    const x = this.experiments.find((e) => e.e.id === id);
    if (!x) throw new ApiError(404, "not_found", `experiment ${id} not found`);
    if (x.e.status === "running" || x.e.status === "scheduled" || x.e.status === "recovering") this.finishExperiment(x, "aborted");
    return x.e;
  }

  testAlert(): void {
    this.testAlertAt = this.now;
    this.testAlertUntil = this.now + 45000;
    this.addTimeline("alert", "info", "Test alert requested", "LabTestAlert → vmalert → Alertmanager → webhook");
  }

  startDrill(body: Record<string, unknown>): Drill {
    if (this.drill?.status === "running") throw new ApiError(409, "drill_active", "A drill is already running.");
    const difficulty = (["easy", "medium", "hard"] as const).find((d) => d === body.difficulty) ?? "easy";
    const player = String(body.player ?? "").trim().slice(0, 32) || "anonymous";
    const pool = DRILL_POOL[difficulty];
    const scenario = this.rng.pick(pool);
    this.drillCounter++;
    this.drillScenario = scenario;
    this.drillPlayer = player;
    const decoys = this.rng.shuffle(SCENARIOS.filter((s) => s.id !== scenario && !s.destructive).map((s) => s.id)).slice(0, 5);
    const candidates = this.rng.shuffle([scenario, ...decoys]).map((id) => ({ id, name: scenarioById(id)!.name }));
    const exp = this.startExperiment({ scenario, durationSec: 600, autoRecover: true, params: {} }, { drill: true });
    const x = this.experiments.find((e) => e.e.id === exp.id)!;
    this.drill = {
      id: `drill-${String(this.drillCounter).padStart(3, "0")}`,
      status: "running",
      difficulty,
      startedAt: iso(this.now),
      hintsUsed: 0,
      hints: [],
      guesses: [],
      candidates,
    };
    x.drillId = this.drill.id;
    this.drillExpiresAt = this.now + 600000;
    this.push("drill", this.drill);
    return this.drill;
  }

  hint(id: string): Drill {
    const d = this.requireDrill(id);
    const all = HINTS[this.drillScenario!] ?? [];
    if (d.hintsUsed < all.length) {
      d.hints = all.slice(0, d.hintsUsed + 1);
      d.hintsUsed++;
    }
    this.push("drill", d);
    return d;
  }

  guess(id: string, scenario: unknown): Drill {
    const d = this.requireDrill(id);
    const correct = scenario === this.drillScenario;
    d.guesses = [...d.guesses, { scenario: String(scenario), correct, at: iso(this.now) }];
    if (correct) {
      const elapsed = (this.now - Date.parse(d.startedAt)) / 1000;
      const base = { easy: 500, medium: 800, hard: 1200 }[d.difficulty];
      const wrong = d.guesses.filter((g) => !g.correct).length;
      d.score = Math.max(0, Math.round(base * Math.max(0.1, 1 - elapsed / 600) - 100 * d.hintsUsed - 75 * wrong));
      this.endDrill(d, "solved");
      this.leaderboard.push({ player: this.drillPlayer, score: d.score, scenario: this.drillScenario!, durationSec: Math.round(elapsed), at: iso(this.now) });
      this.leaderboard.sort((a, b) => b.score - a.score);
      this.leaderboard = this.leaderboard.slice(0, 20);
    }
    this.push("drill", d);
    return d;
  }

  reveal(id: string): Drill {
    const d = this.requireDrill(id);
    d.score = 0;
    this.endDrill(d, "revealed");
    this.push("drill", d);
    return d;
  }

  // ------------------------------------------------------------------ simulation step

  /** Advance the simulation by dtMs (the server calls this every 250 ms). */
  step(dtMs = 250): void {
    this.now += dtMs;
    this.stepCount++;
    const dt = dtMs / 1000;
    const fx = this.effects();
    this.updateBrokers(fx);
    this.updateServices(fx);
    this.flow(dt, fx);
    if (this.stepCount % 4 === 0) {
      this.secondly();
      this.push("tick", this.tick());
    }
    const items = this.pendingEvents.slice(0, this.maxItems);
    this.pendingEvents = [];
    this.push("events", { scale: this.eventScale, items });
    if (this.stepCount % 40 === 0) this.push("state", this.state());
  }

  // ------------------------------------------------------------------ internals

  private svc(name: string): Svc {
    const s = this.svcs.get(name);
    if (!s) throw new ApiError(404, "unknown_service", `unknown service "${name}"`);
    return s;
  }

  activeExperiments(): Exp[] {
    return this.experiments.filter((x) => x.e.status === "running" || x.e.status === "scheduled" || x.e.status === "recovering");
  }

  private runningExperiments(): Exp[] {
    return this.experiments.filter((x) => x.e.status === "running");
  }

  private affectedOf(def: ScenarioDef, p: Record<string, ParamValue>): string[] {
    switch (def.id) {
      case "black-friday":
        return ["order-generator", "payment", "inventory"];
      case "flash-sale":
      case "hot-partition":
      case "fat-messages":
      case "traffic-tsunami":
      case "poison-message":
      case "duplicate-delivery":
        return ["order-generator"];
      case "payment-provider-down":
        return ["payment"];
      case "warehouse-overload":
        return ["inventory"];
      case "notification-meltdown":
        return ["notification"];
      case "database-slowdown":
        return ["payment", "inventory", "shipping", "notification", "analytics"];
      case "recovery-storm":
        return ["payment", "inventory", "shipping"];
      case "regional-outage":
        return String(p.region).startsWith("region-a") ? ["payment", "inventory"] : ["shipping", "notification"];
      case "kafka-broker-failure":
      case "broker-crash":
        return [`broker-${p.broker}`];
      case "broker-rolling-restart":
        return ["broker-0", "broker-1", "broker-2"];
      case "broker-outage":
        return [0, 1, 2].filter((b) => b !== Number(p.survivor)).map((b) => `broker-${b}`);
      default:
        return [String(p.service)];
    }
  }

  private effects(): Effects {
    const fx: Effects = { knobs: {}, down: new Set(), crash: false, zeroReady: new Set(), rebalancing: new Set() };
    const k = (svc: string, patch: Knobs) => (fx.knobs[svc] = { ...fx.knobs[svc], ...patch });
    for (const x of this.runningExperiments()) {
      const p = x.e.scenario === "hidden" ? this.defaultParams(x.def) : x.e.params;
      const elapsed = (this.now - x.startsAtMs) / 1000;
      const dur = (x.endsAtMs - x.startsAtMs) / 1000;
      switch (x.def.id) {
        case "black-friday": {
          const burst = p.randomize ? 0.75 + 0.5 * Math.abs(Math.sin(elapsed / 7)) : 1;
          k("order-generator", { ordersPerSecond: round(Number(p.ordersPerSecond) * burst) });
          if (Number(p.paymentLatencyMs) > 0) k("payment", { processingLatencyMs: Number(p.paymentLatencyMs) });
          if (Number(p.inventoryLatencyMs) > 0) k("inventory", { processingLatencyMs: Number(p.inventoryLatencyMs) });
          break;
        }
        case "flash-sale":
          k("order-generator", { popularSku: String(p.sku), popularSkuProbability: Number(p.probability), ordersPerSecond: Number(p.ordersPerSecond) });
          break;
        case "traffic-tsunami":
          k("order-generator", { ordersPerSecond: round(BASE_RATE + (Number(p.peakOrdersPerSecond) - BASE_RATE) * Math.min(1, elapsed / dur)) });
          break;
        case "hot-partition":
          k("order-generator", { hotPartition: Number(p.partition), hotPartitionRatio: Number(p.ratio) });
          break;
        case "fat-messages":
          k("order-generator", { paddingBytes: Number(p.paddingBytes) });
          break;
        case "payment-provider-down":
          k("payment", { providerDown: true });
          break;
        case "warehouse-overload":
          k("inventory", { processingLatencyMs: round(15 * (Number(p.factor) - 1)) });
          break;
        case "notification-meltdown":
          k("notification", { processingLatencyMs: round(Math.max(0, (BASE_CAPACITY.notification / Number(p.eventsPerSecond) - 1) * 15)) });
          break;
        case "silent-failure":
          k(String(p.service), { paused: true });
          break;
        case "database-slowdown":
          for (const g of CONSUMER_GROUPS) k(g, { dbLatencyMs: Number(p.dbLatencyMs) });
          break;
        case "recovery-storm":
          if (elapsed < dur * Number(p.pauseShare)) for (const g of ["payment", "inventory", "shipping"]) k(g, { paused: true });
          break;
        case "consumer-crash-loop": {
          const interval = Number(p.intervalSec);
          if (elapsed % interval > interval - 4) fx.zeroReady.add(String(p.service));
          break;
        }
        case "regional-outage":
          for (const s of x.e.affected.length ? x.e.affected : this.affectedOf(x.def, p)) fx.zeroReady.add(s);
          break;
        case "rebalance-storm":
          if (elapsed % Number(p.everySec) < 3) fx.rebalancing.add(String(p.service));
          break;
        case "kafka-broker-failure":
          fx.down.add(Number(p.broker));
          break;
        case "broker-crash":
          fx.down.add(Number(p.broker));
          fx.crash = true;
          break;
        case "broker-rolling-restart": {
          const slot = Math.floor(elapsed / (dur / 3));
          const within = elapsed - slot * (dur / 3);
          if (slot < 3 && within < Number(p.pauseSec)) fx.down.add(slot);
          break;
        }
        case "broker-outage":
          for (const b of [0, 1, 2]) if (b !== Number(p.survivor)) fx.down.add(b);
          break;
        case "network-chaos":
          k(String(p.service), { netLatencyMs: Number(p.latencyMs), netDropProbability: Number(p.dropProbability) });
          break;
        case "poison-message":
          k("order-generator", { poisonProbability: Number(p.probability) });
          break;
        case "duplicate-delivery":
          k("order-generator", { duplicateProbability: Number(p.probability) });
          break;
      }
    }
    return fx;
  }

  private defaultParams(def: ScenarioDef): Record<string, ParamValue> {
    return Object.fromEntries(def.params.map((p) => [p.key, p.default]));
  }

  private knobsOf(name: string, fx: Effects): Knobs {
    return { ...(fx.knobs[name] ?? {}), ...this.svcs.get(name)!.manual };
  }

  private kn(name: string, key: string, fx: Effects): KnobValue | undefined {
    return this.knobsOf(name, fx)[key] ?? KNOB_DEFAULTS[key];
  }

  private lastFx: Effects = { knobs: {}, down: new Set(), crash: false, zeroReady: new Set(), rebalancing: new Set() };

  private updateGenerations(): void {
    const fx = this.effects();
    this.lastFx = fx;
    for (const s of this.svcs.values()) {
      const key = JSON.stringify(Object.entries(this.knobsOf(s.name, fx)).sort());
      if (key !== s.lastKey) {
        s.lastKey = key;
        s.desiredGeneration++;
        s.applyAt = this.now + 1200 + this.rng.int(0, 800);
      }
    }
  }

  private updateServices(fx: Effects): void {
    this.lastFx = fx;
    this.updateGenerations();
    for (const s of this.svcs.values()) {
      const zero = fx.zeroReady.has(s.name);
      const crashKey = s.name;
      if (zero) {
        if (!this.crashState.get(crashKey)) {
          this.crashState.set(crashKey, 1);
          this.restartCounter.set(crashKey, (this.restartCounter.get(crashKey) ?? 0) + 1);
          for (const x of this.runningExperiments()) if (x.e.affected.includes(s.name) || x.hidden) x.restarts++;
        }
        s.ready = 0;
        s.readyAt = this.now + 1500;
      } else {
        this.crashState.set(crashKey, 0);
        if (s.ready < s.replicas && this.now >= s.readyAt) {
          s.ready++;
          s.readyAt = this.now + 1500;
        }
        if (s.ready > s.replicas) s.ready = s.replicas;
      }
      if (s.appliedGeneration < s.desiredGeneration && this.now >= s.applyAt && s.ready > 0) s.appliedGeneration = s.desiredGeneration;
    }
  }

  private health(s: Svc, fx: Effects): Health {
    if (s.replicas === 0 || s.ready === 0) return "down";
    const k = this.knobsOf(s.name, fx);
    if (s.ready < s.replicas || k.providerDown || Number(k.netDropProbability ?? 0) > 0 || Number(k.processingErrorProbability ?? 0) > 0.05 || fx.rebalancing.has(s.name)) return "degraded";
    if (s.name !== "order-generator" && this.capacity(s.name as ConsumerGroup, fx) < BASE_CAPACITY[s.name as ConsumerGroup] * s.replicas * 0.3) return "degraded";
    return "up";
  }

  serviceState(s: Svc): ServiceState {
    const fx = this.lastFx;
    const knobs: Record<string, KnobValue> = {};
    for (const [k, v] of Object.entries(this.knobsOf(s.name, fx))) if (v !== KNOB_DEFAULTS[k]) knobs[k] = v;
    return {
      name: s.name,
      replicas: s.replicas,
      readyReplicas: s.ready,
      paused: this.kn(s.name, "paused", fx) === true,
      desiredGeneration: s.desiredGeneration,
      appliedGeneration: s.appliedGeneration,
      health: this.health(s, fx),
      knobs,
    };
  }

  serviceStates(): ServiceState[] {
    return [...this.svcs.values()].map((s) => this.serviceState(s));
  }

  brokerStates(): BrokerState[] {
    const anyDown = this.brokerUp.some((u) => !u);
    const catching = this.catchUpUntil.some((t) => t > this.now);
    return [0, 1, 2].map((id) => ({
      id,
      up: this.brokerUp[id]!,
      isController: this.controller === id && this.brokerUp[id]!,
      leaderCount: this.brokerUp[id] ? this.brokerLeaders[id]! : 0,
      underReplicated: this.brokerUp[id] && (anyDown || catching) ? this.brokerLeaders[id]! : 0,
    }));
  }

  private updateBrokers(fx: Effects): void {
    for (const id of [0, 1, 2]) {
      const shouldUp = !fx.down.has(id);
      if (this.brokerUp[id] && !shouldUp) {
        this.brokerUp[id] = false;
        this.brokerDownSince[id] = this.now;
        this.disruptionUntil = this.now + (fx.crash ? 9000 : 3000);
        this.addTimeline("experiment", "critical", `Broker ${id} ${fx.crash ? "crashed" : "stopped"}`, fx.crash ? "no controlled shutdown — clients wait for timeouts" : "controlled shutdown, leaders migrated");
      } else if (!this.brokerUp[id] && shouldUp) {
        this.brokerUp[id] = true;
        this.brokerDownSince[id] = null;
        this.catchUpUntil[id] = this.now + 8000;
        this.addTimeline("recovery", "success", `Broker ${id} is back`, "replicas catching up, preferred leaders return shortly");
      }
    }
    if (!this.brokerUp[this.controller]) {
      const next = [0, 1, 2].find((b) => this.brokerUp[b]);
      if (next !== undefined) {
        this.addTimeline("system", "warning", `KRaft controller moved ${this.controller} → ${next}`);
        this.controller = next;
      }
    }
    const up = [0, 1, 2].filter((b) => this.brokerUp[b]);
    // Preferred leaders return 10 s after the broker came back.
    const leaderHolders = up.filter((b) => this.catchUpUntil[b]! - 8000 + 10000 <= this.now);
    const holders = leaderHolders.length ? leaderHolders : up;
    const leaders = [0, 0, 0];
    for (let i = 0; i < TOTAL_PARTITIONS; i++) {
      const pref = i % 3;
      const target = holders.includes(pref) ? pref : holders[i % Math.max(1, holders.length)];
      if (target !== undefined) leaders[target]!++;
    }
    this.brokerLeaders = leaders;
    this.writeBlocked = up.length < 2;
  }

  capacity(g: ConsumerGroup, fx: Effects): number {
    const s = this.svcs.get(g)!;
    if (this.kn(g, "paused", fx) === true || fx.rebalancing.has(g) || s.ready === 0) return 0;
    if (this.writeBlocked && g !== "analytics") return 0;
    const lat = Number(this.kn(g, "processingLatencyMs", fx)) + Number(this.kn(g, "dbLatencyMs", fx)) * 0.6 + Number(this.kn(g, "netLatencyMs", fx)) * 0.5;
    let c = (BASE_CAPACITY[g] * s.ready) / (1 + lat / 15);
    c *= 1 - Math.min(0.9, Number(this.kn(g, "netDropProbability", fx)) * 2);
    c *= 1 - Math.min(0.9, Number(this.kn(g, "processingErrorProbability", fx)));
    if (Number(this.kn("order-generator", "paddingBytes", fx)) > 10000) c *= 0.45;
    const t = this.tuning.get(g)!;
    const mpr = Number(t.maxPollRecords);
    if (mpr < 100) c *= 0.35 + (0.65 * mpr) / 100;
    if (t.acks !== "all") c *= 1.12;
    if (g === "inventory") c *= 1 - 0.4 * Number(this.kn("order-generator", "popularSkuProbability", fx));
    if (this.now < this.disruptionUntil) c *= 0.2;
    return c;
  }

  private nextOffset(topic: string): number {
    const o = (this.offsets.get(topic) ?? 0) + 1;
    this.offsets.set(topic, o);
    return o;
  }

  private ev(o: SOrder, kind: LiveEvent["kind"], topic: string, extra: Partial<LiveEvent> = {}): void {
    const partitions = TOPIC_PARTITIONS[topic]!;
    const e: LiveEvent = { topic, partition: o.p % partitions, offset: this.nextOffset(topic) * this.eventScale, orderId: o.id, kind, at: iso(this.now - this.rng.int(0, 240)), ...extra };
    this.pendingEvents.push(e);
    let rec = this.orders.get(o.id);
    if (!rec) {
      rec = { events: [], traceId: this.rng.hex(32) };
      this.orders.set(o.id, rec);
      if (this.orders.size > 3000) this.orders.delete(this.orders.keys().next().value!);
    }
    rec.events.push(e);
  }

  private enqueue(group: ConsumerGroup, topic: string, o: SOrder, fx: Effects): void {
    const key = `${group}|${topic}|${o.p % TOPIC_PARTITIONS[topic]!}`;
    let q = this.queues.get(key);
    if (!q) this.queues.set(key, (q = []));
    const lat = Number(this.kn(group, "processingLatencyMs", fx)) + Number(this.kn(group, "dbLatencyMs", fx));
    q.push({ o, readyAt: this.now + 120 + this.rng.int(0, 400) + Math.min(lat, 3000) });
  }

  private flow(dt: number, fx: Effects): void {
    const gen = this.svcs.get("order-generator")!;
    let rate = 0;
    if (this.simulation === "running" && gen.ready > 0 && this.kn("order-generator", "paused", fx) !== true && !this.writeBlocked) {
      const target = Number(this.knobsOf("order-generator", fx).ordersPerSecond ?? BASE_RATE);
      rate = target * (0.92 + 0.16 * this.rng.next()) * (0.95 + 0.05 * Math.sin(this.now / 9000));
      if (this.now < this.disruptionUntil) rate *= 0.4;
    }
    const hotP = this.knobsOf("order-generator", fx).hotPartition;
    const hotRatio = Number(this.kn("order-generator", "hotPartitionRatio", fx));
    const weights = new Array(6).fill(1 / 6) as number[];
    if (typeof hotP === "number" && hotRatio > 0) {
      for (let p = 0; p < 6; p++) weights[p] = p === hotP ? hotRatio : (1 - hotRatio) / 5;
    }
    const dup = Number(this.kn("order-generator", "duplicateProbability", fx));
    const poison = Number(this.kn("order-generator", "poisonProbability", fx));
    const createdTotal = rate * dt * (1 + dup + poison);
    const produced = new Map<string, number[]>();
    const add = (topic: string, p: number, v: number) => {
      let arr = produced.get(topic);
      if (!arr) produced.set(topic, (arr = new Array(TOPIC_PARTITIONS[topic]).fill(0)));
      arr[p % arr.length]! += v;
    };
    for (let p = 0; p < 6; p++) add("orders.created", p, createdTotal * weights[p]!);
    this.acc.created += rate * dt;

    // Fluid processing per group using the previous step's production as input.
    const inputs = this.lastProduced;
    const pf = this.knobsOf("payment", fx);
    this.pPaid = pf.providerDown ? 0 : 1 - Number(pf.businessFailureProbability ?? 0.05);
    const pop = Number(this.kn("order-generator", "popularSkuProbability", fx));
    this.pRes = 0.97 * (1 - pop) + 0.35 * pop;
    const both = this.pPaid * this.pRes;
    this.cancelPaidShare = both < 1 ? (this.pPaid * (1 - this.pRes)) / (1 - both) : 0;
    this.cancelResShare = both < 1 ? (this.pRes * (1 - this.pPaid)) / (1 - both) : 0;
    const processedBy: Record<string, Map<string, number[]>> = {};
    const fractions = new Map<string, number>();
    for (const g of CONSUMER_GROUPS) {
      const cells: { t: string; p: number; arr: number[] }[] = [];
      for (const t of CONSUMES[g]) {
        const arr = this.backlog.get(`${g}|${t}`)!;
        const inp = inputs.get(t);
        for (let p = 0; p < arr.length; p++) {
          arr[p]! += inp ? inp[p]! : 0;
          cells.push({ t, p, arr });
        }
      }
      let cap = this.capacity(g, fx) * dt;
      const proc = new Map<string, number[]>();
      const fair = cap / cells.length;
      const take = cells.map((c) => Math.min(c.arr[c.p]!, fair));
      cap -= take.reduce((a, b) => a + b, 0);
      for (let i = 0; i < cells.length && cap > 0; i++) {
        const c = cells[i]!;
        const extra = Math.min(c.arr[c.p]! - take[i]!, fair * 0.5, cap);
        take[i]! += extra;
        cap -= extra;
      }
      cells.forEach((c, i) => {
        const before = c.arr[c.p]!;
        const t = take[i]!;
        c.arr[c.p] = Math.max(0, before - t);
        fractions.set(`${g}|${c.t}|${c.p}`, before > 0.5 ? t / before : 1);
        let a = proc.get(c.t);
        if (!a) proc.set(c.t, (a = new Array(c.arr.length).fill(0)));
        a[c.p] = t;
      });
      processedBy[g] = proc;
    }
    const sum = (a?: number[]) => (a ? a.reduce((x, y) => x + y, 0) : 0);
    // payment outputs
    const payCreated = processedBy.payment!.get("orders.created")!;
    payCreated.forEach((v, p) => {
      const clean = v / (1 + dup + poison);
      add("payments.completed", p, clean * this.pPaid);
      add("payments.failed", p, clean * (1 - this.pPaid));
      add("orders.dlq", p, (v * poison) / (1 + dup + poison));
      this.acc.paid += clean * this.pPaid;
      this.acc.failed += clean * (1 - this.pPaid);
      this.acc.dlq += (v * poison) / (1 + dup + poison);
    });
    processedBy.payment!.get("orders.cancelled")!.forEach((v, p) => add("payments.refunded", p, v * this.cancelPaidShare));
    processedBy.inventory!.get("orders.created")!.forEach((v, p) => {
      const clean = v / (1 + dup + poison);
      add("inventory.reserved", p, clean * this.pRes);
      add("inventory.rejected", p, clean * (1 - this.pRes));
      this.acc.reserved += clean * this.pRes;
      this.acc.rejected += clean * (1 - this.pRes);
    });
    processedBy.inventory!.get("orders.cancelled")!.forEach((v, p) => add("inventory.released", p, v * this.cancelResShare));
    for (const [t, arr] of processedBy.shipping!) {
      arr.forEach((v, p) => {
        const decisions = v / 2;
        add("orders.shipped", p + (t.endsWith("failed") || t.endsWith("rejected") ? 0 : 0), decisions * both);
        add("orders.cancelled", p, decisions * (1 - both));
      });
    }
    for (const arr of processedBy.notification!.values()) arr.forEach((v, p) => add("notifications.sent", p, v));
    this.lastProduced = produced;
    for (const [t, arr] of produced) this.stepProduced[t] = (this.stepProduced[t] ?? 0) + sum(arr);

    // Sampled orders.
    this.eventScale = Math.max(1, Math.round(rate / this.sampledPerSec));
    this.sampleAcc += (rate * dt * (1 + dup)) / this.eventScale;
    while (this.sampleAcc >= 1) {
      this.sampleAcc -= 1;
      const p = this.pickPartition(weights);
      const o: SOrder = { id: this.rng.uuid(), p, valueCents: this.rng.int(900, 18900), region: this.rng.pick(REGIONS) };
      this.ev(o, "created", "orders.created", { valueCents: o.valueCents, region: o.region });
      if (this.rng.chance(dup)) this.ev(o, "created", "orders.created", { valueCents: o.valueCents, region: o.region, reason: "DUPLICATE" });
      for (const g of ["payment", "inventory", "notification"] as const) this.enqueue(g, "orders.created", o, fx);
    }
    if (poison > 0 && this.rng.chance(Math.min(1, (rate * dt * poison) / this.eventScale))) {
      const o: SOrder = { id: `poison-${this.rng.hex(8)}`, p: this.rng.int(0, 5), valueCents: 0, region: "unknown" };
      this.enqueue("payment", "orders.created", o, fx);
    }
    this.processSampled(fractions, fx);
  }

  private pickPartition(weights: number[]): number {
    let r = this.rng.next();
    for (let p = 0; p < weights.length; p++) {
      r -= weights[p]!;
      if (r <= 0) return p;
    }
    return weights.length - 1;
  }

  private processSampled(fractions: Map<string, number>, fx: Effects): void {
    for (const [key, q] of this.queues) {
      if (!q.length) continue;
      const frac = fractions.get(key) ?? 1;
      const want = q.length * frac + (this.carry.get(key) ?? 0);
      let n = Math.floor(want);
      this.carry.set(key, want - n);
      const [group, topic] = key.split("|") as [ConsumerGroup, string];
      while (n > 0 && q.length && q[0]!.readyAt <= this.now) {
        const { o } = q.shift()!;
        n--;
        this.handle(group, topic, o, fx);
      }
    }
  }

  private handle(group: ConsumerGroup, topic: string, o: SOrder, fx: Effects): void {
    if (o.id.startsWith("poison-")) {
      this.ev(o, "dlq", "orders.dlq", { reason: "DECODE_ERROR: unknown magic byte" });
      return;
    }
    if (group === "payment" && topic === "orders.created") {
      if (o.terminal) return;
      o.paid = this.rng.chance(this.pPaid);
      if (o.paid) this.ev(o, "paid", "payments.completed", { valueCents: o.valueCents });
      else this.ev(o, "payment-failed", "payments.failed", { reason: this.knobsOf("payment", fx).providerDown ? "PROVIDER_UNAVAILABLE" : this.rng.pick(["CARD_DECLINED", "INSUFFICIENT_FUNDS", "FRAUD_SUSPECTED"]) });
      this.enqueue("shipping", o.paid ? "payments.completed" : "payments.failed", o, fx);
    } else if (group === "inventory" && topic === "orders.created") {
      if (o.terminal) return;
      o.reserved = this.rng.chance(this.pRes);
      if (o.reserved) this.ev(o, "reserved", "inventory.reserved");
      else this.ev(o, "rejected", "inventory.rejected", { reason: "OUT_OF_STOCK" });
      this.enqueue("shipping", o.reserved ? "inventory.reserved" : "inventory.rejected", o, fx);
    } else if (group === "shipping") {
      if (o.terminal || o.paid === undefined || o.reserved === undefined) return;
      if (o.paid && o.reserved) {
        o.terminal = "shipped";
        this.ev(o, "shipped", "orders.shipped", { carrier: this.rng.pick(CARRIERS) });
        this.enqueue("notification", "orders.shipped", o, fx);
      } else {
        o.terminal = "cancelled";
        const reason = !o.paid && !o.reserved ? "BOTH_FAILED" : !o.paid ? "PAYMENT_FAILED" : "OUT_OF_STOCK";
        this.ev(o, "cancelled", "orders.cancelled", { reason });
        this.enqueue("payment", "orders.cancelled", o, fx);
        this.enqueue("inventory", "orders.cancelled", o, fx);
        this.enqueue("notification", "orders.cancelled", o, fx);
      }
    } else if (group === "payment" && topic === "orders.cancelled") {
      if (o.paid) {
        this.ev(o, "refunded", "payments.refunded", { valueCents: o.valueCents });
        this.enqueue("notification", "payments.refunded", o, fx);
      }
    } else if (group === "inventory" && topic === "orders.cancelled") {
      if (o.reserved) this.ev(o, "released", "inventory.released");
    } else if (group === "notification") {
      this.ev(o, "notified", "notifications.sent", { reason: { "orders.created": "order-confirmed", "orders.shipped": "order-shipped", "orders.cancelled": "order-cancelled", "payments.refunded": "payment-refunded" }[topic] });
    }
  }

  /** Once per second: KPIs, rates, alerts, experiments. */
  private secondly(): void {
    const a = 0.35;
    const ema = (prev: number, v: number) => prev + a * (v - prev);
    for (const [t, v] of Object.entries(this.stepProduced)) this.rates[t] = round(v, 1);
    for (const t of Object.keys(TOPIC_PARTITIONS)) if (!(t in this.stepProduced)) this.rates[t] = 0;
    this.stepProduced = {};
    const k = this.kpis;
    k.ordersPerSec = round(ema(k.ordersPerSec, this.acc.created), 1);
    k.revenuePerMin = round(ema(k.revenuePerMin, this.acc.paid * this.pRes * 60 * AVG_ORDER_EUR));
    if (this.acc.paid + this.acc.failed > 0.5) k.paymentSuccessRate = round(ema(k.paymentSuccessRate, this.acc.paid / (this.acc.paid + this.acc.failed)), 4);
    if (this.acc.reserved + this.acc.rejected > 0.5) k.reservationSuccessRate = round(ema(k.reservationSuccessRate, this.acc.reserved / (this.acc.reserved + this.acc.rejected)), 4);
    k.dlqPerMin = round(ema(k.dlqPerMin, this.acc.dlq * 60), 1);
    let total = 0;
    const lagOf = (g: string) => {
      let s = 0;
      for (const t of CONSUMES[g as ConsumerGroup]) for (const v of this.backlog.get(`${g}|${t}`)!) s += v;
      return s;
    };
    for (const g of CONSUMER_GROUPS) total += lagOf(g);
    k.totalLag = Math.round(total);
    const fx = this.lastFx;
    const wait = (g: ConsumerGroup) => {
      const c = this.capacity(g, fx);
      return lagOf(g) / Math.max(c, 5);
    };
    const tl = Number(this.tuning.get("order-generator")!.lingerMs);
    const procLat = Math.max(Number(this.kn("payment", "processingLatencyMs", fx)), Number(this.kn("inventory", "processingLatencyMs", fx))) + Number(this.kn("shipping", "dbLatencyMs", fx)) * 3;
    const e2e = 900 + tl * 3 + procLat * 1.6 + Math.min(90, Math.max(wait("payment"), wait("inventory")) + wait("shipping")) * 1000;
    k.e2eP95Ms = Math.round(ema(k.e2eP95Ms, e2e * (1.5 + 0.1 * this.rng.next())));
    k.inFlight = Math.round(lagOf("payment") / 2 + lagOf("inventory") / 2 + lagOf("shipping") / 2 + k.ordersPerSec * 1.2);
    this.acc = { created: 0, paid: 0, failed: 0, reserved: 0, rejected: 0, dlq: 0, procPayment: 0 };

    this.evaluateAlerts();
    this.updateExperiments();
    if (this.drill?.status === "running" && this.now >= this.drillExpiresAt) {
      this.endDrill(this.drill, "expired");
      this.push("drill", this.drill);
    }
  }

  // ------------------------------------------------------------------ alerts

  lagOfGroup(g: string): number {
    let s = 0;
    for (const t of CONSUMES[g as ConsumerGroup] ?? []) for (const v of this.backlog.get(`${g}|${t}`) ?? []) s += v;
    return s;
  }

  private buildRules(): Rule[] {
    return [
      {
        name: "KafkaBrokerDown", severity: "critical", component: "kafka", dashboard: "kafka-broker", forSec: 5,
        summary: (l) => `Kafka broker ${l.kafka_broker} is down`,
        description: "A broker stopped reporting. Partitions led by it moved; replicas are out of sync.",
        eval: (s) => [0, 1, 2].map((b) => ({ labels: { kafka_broker: String(b), kafka_cluster: "kafka-lab" }, value: s.brokerUp[b] ? 0 : 1, active: !s.brokerUp[b] })),
      },
      {
        name: "KafkaUnderReplicatedPartitions", severity: "warning", component: "kafka", dashboard: "kafka-overview", forSec: 10,
        summary: (_l, v) => `${v} partitions are under-replicated`,
        description: "Some partitions have fewer in-sync replicas than configured.",
        eval: (s) => {
          const urp = s.brokerStates().reduce((a, b) => a + b.underReplicated, 0);
          return [{ labels: { kafka_cluster: "kafka-lab" }, value: urp, active: urp > 0 }];
        },
      },
      {
        name: "KafkaConsumerLagHigh", severity: "warning", component: "consumer", dashboard: "kafka-lag", forSec: 10,
        summary: (l, v) => `Consumer group ${l.consumergroup} lags ${Math.round(v).toLocaleString("en")} records`,
        description: "The group consumes slower than records arrive.",
        eval: (s) => CONSUMER_GROUPS.map((g) => ({ labels: { consumergroup: g }, value: s.lagOfGroup(g), active: s.lagOfGroup(g) > 2500 })),
      },
      {
        name: "PaymentSuccessRateLow", severity: "critical", component: "business", dashboard: "lab-business", forSec: 10,
        summary: (_l, v) => `Payment success rate is ${(v * 100).toFixed(0)}%`,
        description: "Far fewer authorisations succeed than usual. Revenue is at risk.",
        eval: (s) => [{ labels: { service: "payment" }, value: s.kpis.paymentSuccessRate, active: s.kpis.paymentSuccessRate < 0.85 }],
      },
      {
        name: "ReservationSuccessRateLow", severity: "warning", component: "business", dashboard: "lab-business", forSec: 10,
        summary: (_l, v) => `Only ${(v * 100).toFixed(0)}% of reservations succeed`,
        description: "Stock runs out or inventory rejects reservations.",
        eval: (s) => [{ labels: { service: "inventory" }, value: s.kpis.reservationSuccessRate, active: s.kpis.reservationSuccessRate < 0.85 }],
      },
      {
        name: "DeadLetterQueueActive", severity: "warning", component: "business", dashboard: "lab-business", forSec: 5,
        summary: (_l, v) => `${v.toFixed(0)} records/min go to orders.dlq`,
        description: "Records could not be processed and were dead-lettered.",
        eval: (s) => [{ labels: { topic: "orders.dlq" }, value: s.kpis.dlqPerMin, active: s.kpis.dlqPerMin > 5 }],
      },
      {
        name: "ServiceDown", severity: "critical", component: "platform", dashboard: "lab-services", forSec: 8,
        summary: (l) => `${l.service} has no ready replicas`,
        description: "No pod of the service is ready.",
        eval: (s) => [...s.svcs.values()].map((v) => ({ labels: { service: v.name }, value: v.ready, active: v.ready === 0 })),
      },
      {
        name: "EndToEndLatencyHigh", severity: "warning", component: "business", dashboard: "kafka-e2e", forSec: 15,
        summary: (_l, v) => `Order e2e p95 is ${(v / 1000).toFixed(1)} s`,
        description: "Orders take much longer from creation to shipment.",
        eval: (s) => [{ labels: {}, value: s.kpis.e2eP95Ms, active: s.kpis.e2eP95Ms > 15000 }],
      },
      {
        name: "ChaosExperimentActive", severity: "info", component: "chaos", dashboard: "lab-chaos", forSec: 0,
        summary: (_l, v) => `${v} chaos experiment(s) running`,
        description: "Expected degradation — an experiment is active.",
        eval: (s) => {
          const n = s.runningExperiments().length;
          return [{ labels: {}, value: n, active: n > 0 }];
        },
      },
      {
        name: "LabTestAlert", severity: "info", component: "platform", dashboard: "lab-monitoring", forSec: 2,
        summary: () => "Test alert from the controlboard",
        description: "Proves the path vmalert → Alertmanager → controlboard webhook works.",
        eval: (s) => [{ labels: {}, value: 1, active: s.now >= s.testAlertAt && s.now < s.testAlertUntil }],
      },
    ];
  }

  private evaluateAlerts(): void {
    for (const rule of this.rules) {
      for (const r of rule.eval(this)) {
        const fp = hashStr(rule.name + JSON.stringify(r.labels)) + hashStr(JSON.stringify(r.labels) + rule.name);
        let inst = this.alerts.get(fp);
        if (r.active) {
          if (!inst || inst.alert.status === "resolved") {
            inst = {
              since: this.now,
              alert: {
                fingerprint: fp,
                name: rule.name,
                status: rule.forSec === 0 ? "firing" : "pending",
                severity: rule.severity,
                component: rule.component,
                summary: rule.summary(r.labels, r.value),
                description: rule.description,
                startsAt: iso(this.now),
                labels: { alertname: rule.name, severity: rule.severity, component: rule.component, dashboard: rule.dashboard, ...r.labels },
                dashboardUrl: dashboardUrl(rule.dashboard),
                runbookUrl: `https://github.com/kafka-lab/runbooks/blob/main/${rule.name}.md`,
                generatorUrl: `http://vmalert.localhost:8080/vmalert/alert?group_id=lab&alert_id=${fp}`,
              },
            };
            this.alerts.set(fp, inst);
            if (inst.alert.status === "firing") this.onFiring(inst);
            this.push("alert", inst.alert);
          } else {
            inst.alert.summary = rule.summary(r.labels, r.value);
            if (inst.alert.status === "pending" && this.now - inst.since >= rule.forSec * 1000) {
              inst.alert = { ...inst.alert, status: "firing", startsAt: iso(this.now) };
              this.onFiring(inst);
              this.push("alert", inst.alert);
            }
          }
        } else if (inst && inst.alert.status !== "resolved") {
          const wasFiring = inst.alert.status === "firing";
          inst.alert = { ...inst.alert, status: "resolved", endsAt: iso(this.now) };
          inst.resolvedAt = this.now;
          if (wasFiring) this.addTimeline("alert", "success", `Resolved: ${rule.name}`, inst.alert.summary);
          this.push("alert", inst.alert);
        }
      }
    }
    for (const [fp, inst] of this.alerts) if (inst.resolvedAt && this.now - inst.resolvedAt > 15 * 60000) this.alerts.delete(fp);
  }

  private onFiring(inst: AlertInst): void {
    const a = inst.alert;
    this.addTimeline("alert", a.severity === "critical" ? "critical" : a.severity === "warning" ? "warning" : "info", `Firing: ${a.name}`, a.summary);
    if (a.name === "ChaosExperimentActive" || a.name === "LabTestAlert") return;
    for (const x of this.runningExperiments()) {
      x.alerts.add(a.name);
      if (x.firstAlertAt === undefined) x.firstAlertAt = this.now;
    }
  }

  // ------------------------------------------------------------------ experiments

  private updateExperiments(): void {
    for (const x of this.experiments) {
      const e = x.e;
      if (e.status === "scheduled" && this.now >= x.startsAtMs) {
        e.status = "running";
        this.addTimeline("experiment", "warning", `${e.name} started`, undefined, e.id);
        this.updateGenerations();
      }
      if (e.status === "running" || e.status === "recovering") {
        x.peakLag = Math.max(x.peakLag, this.kpis.totalLag);
        const failed = (1 - this.kpis.paymentSuccessRate) * this.kpis.ordersPerSec + (1 - this.kpis.reservationSuccessRate) * this.kpis.ordersPerSec;
        x.failedOrders += Math.max(0, failed - 0.08 * this.kpis.ordersPerSec);
        x.lostRevenue += Math.max(0, BASE_RATE * 0.92 - this.kpis.ordersPerSec * this.kpis.paymentSuccessRate * this.kpis.reservationSuccessRate) * AVG_ORDER_EUR * 0.5;
      }
      if (e.status === "running" && this.now >= x.endsAtMs && e.autoRecover) {
        e.status = "recovering";
        e.endedAt = iso(this.now);
        x.recoveringSince = this.now;
        this.addTimeline("recovery", "info", `${e.name} ended — reverting`, "waiting for KPIs to return to baseline", e.id);
        this.updateGenerations();
      } else if (e.status === "recovering") {
        const recovered = this.kpis.totalLag < 800 && this.kpis.paymentSuccessRate > 0.9 && this.kpis.reservationSuccessRate > 0.9 && this.brokerStates().every((b) => b.up && b.underReplicated === 0);
        if (recovered || this.now - x.recoveringSince! > 180000) {
          e.status = "completed";
          e.report = this.report(x, recovered ? Math.round((this.now - x.recoveringSince!) / 1000) : undefined);
          this.addTimeline("recovery", recovered ? "success" : "warning", recovered ? `${e.name} recovered` : `${e.name} did not fully recover`, recovered ? `back to baseline after ${e.report.timeToRecoverSec}s` : "KPIs still off baseline after 180 s", e.id);
          if (x.drillId && this.drill?.id === x.drillId && this.drill.status === "running") {
            this.endDrill(this.drill, "expired");
            this.push("drill", this.drill);
          }
        }
      }
      if (e.status === "running" || e.status === "recovering" || e.status === "scheduled") {
        this.updateObservations(x);
        this.push("experiment", e);
      }
    }
  }

  private report(x: Exp, timeToRecoverSec?: number): ExperimentReport {
    const r: ExperimentReport = {
      peakLag: x.peakLag,
      ordersAffected: Math.round(x.failedOrders + x.peakLag / 2),
      revenueImpactEur: round(x.lostRevenue / 60, 2),
      alerts: [...x.alerts],
    };
    if (x.firstAlertAt !== undefined) r.timeToDetectSec = Math.round((x.firstAlertAt - x.startsAtMs) / 1000);
    if (timeToRecoverSec !== undefined) r.timeToRecoverSec = timeToRecoverSec;
    return r;
  }

  private finishExperiment(x: Exp, status: "aborted" | "completed"): void {
    const e = x.e;
    const wasHidden = e.scenario === "hidden";
    e.status = status;
    e.endedAt = iso(this.now);
    e.report = this.report(x);
    this.addTimeline(wasHidden ? "drill" : "recovery", "info", `${e.name} stopped`, "fault reverted", e.id);
    this.updateGenerations();
    this.push("experiment", e);
  }

  private updateObservations(x: Exp): void {
    if (x.hidden && x.e.scenario === "hidden") {
      x.e.observations = [];
      return;
    }
    const p = x.e.params;
    const fx = this.lastFx;
    const obs: Omit<Observation, "at">[] = [];
    const k = this.kpis;
    const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
    const fmt = (v: number) => Math.round(v).toLocaleString("en");
    for (const svc of x.e.affected.filter((a) => this.svcs.has(a))) {
      const s = this.svcs.get(svc)!;
      const knobs = fx.knobs[svc];
      if (knobs && Object.keys(knobs).length) {
        obs.push({
          label: `${svc}: control snapshot applied`,
          requested: Object.entries(knobs).map(([kk, v]) => `${kk}=${v}`).join(", "),
          observed: `generation ${s.appliedGeneration}/${s.desiredGeneration} on ${s.ready}/${s.replicas} pods`,
          confirmed: s.appliedGeneration >= s.desiredGeneration,
        });
      }
    }
    const lagObs = (g: string, threshold = 1000) => obs.push({ label: `${g} consumer lag`, requested: "grows", observed: `${fmt(this.lagOfGroup(g))} records`, confirmed: this.lagOfGroup(g) > threshold });
    const brokerObs = (b: number) => {
      const st = this.brokerStates();
      obs.push({ label: `Broker ${b} stopped`, requested: "broker down", observed: this.brokerUp[b] ? "broker still up" : `down — leaders on ${st.filter((s) => s.up).map((s) => `${s.id}:${s.leaderCount}`).join(", ")}`, confirmed: !this.brokerUp[b] });
    };
    switch (x.def.id) {
      case "black-friday":
      case "traffic-tsunami":
      case "flash-sale": {
        const target = Number(this.knobsOf("order-generator", fx).ordersPerSecond ?? BASE_RATE);
        obs.push({ label: "Order rate", requested: `${fmt(target)} orders/s`, observed: `${fmt(k.ordersPerSec)} orders/s`, confirmed: k.ordersPerSec >= target * 0.8 });
        if (x.def.id === "flash-sale") obs.push({ label: "Reservation success", requested: "drops below 90%", observed: pct(k.reservationSuccessRate), confirmed: k.reservationSuccessRate < 0.9 });
        else lagObs("payment");
        break;
      }
      case "hot-partition": {
        const lag = this.backlog.get("payment|orders.created")!;
        const hp = Number(p.partition);
        const others = lag.filter((_, i) => i !== hp);
        const avg = others.reduce((a, b) => a + b, 0) / others.length;
        obs.push({ label: `Partition ${hp} is hot`, requested: `${pct(Number(p.ratio))} of traffic`, observed: `lag p${hp} ${fmt(lag[hp]!)} vs avg ${fmt(avg)}`, confirmed: lag[hp]! > avg * 3 + 50 });
        break;
      }
      case "fat-messages":
        obs.push({ label: "Record size", requested: `+${fmt(Number(p.paddingBytes))} bytes`, observed: `~${fmt(Number(this.kn("order-generator", "paddingBytes", fx)) + 420)} bytes per order`, confirmed: Number(this.kn("order-generator", "paddingBytes", fx)) > 0 });
        break;
      case "payment-provider-down":
        obs.push({ label: "Payment success rate", requested: "0%", observed: pct(k.paymentSuccessRate), confirmed: k.paymentSuccessRate < 0.2 });
        break;
      case "warehouse-overload":
        lagObs("inventory");
        break;
      case "notification-meltdown":
        lagObs("notification");
        break;
      case "silent-failure": {
        const svc = String(p.service);
        const st = this.serviceState(this.svcs.get(svc)!);
        obs.push({ label: `${svc} lag while pods are healthy`, requested: "no progress, no crash", observed: `lag ${fmt(this.lagOfGroup(svc))}, ${st.readyReplicas}/${st.replicas} ready`, confirmed: this.lagOfGroup(svc) > 500 && st.readyReplicas > 0 });
        break;
      }
      case "database-slowdown":
        obs.push({ label: "E2E p95 latency", requested: "> 2 s", observed: `${(k.e2eP95Ms / 1000).toFixed(1)} s`, confirmed: k.e2eP95Ms > 2000 });
        break;
      case "recovery-storm":
        for (const g of ["payment", "inventory", "shipping"]) lagObs(g, 300);
        break;
      case "consumer-crash-loop":
      case "rebalance-storm":
        obs.push({ label: x.def.id === "rebalance-storm" ? "Rebalances" : "Pod restarts", requested: "repeated", observed: x.def.id === "rebalance-storm" ? `${Math.floor((this.now - x.startsAtMs) / 1000 / Number(p.everySec)) + 1} rebalances` : `${x.restarts} restarts`, confirmed: x.def.id === "rebalance-storm" || x.restarts > 0 });
        lagObs(String(p.service), 200);
        break;
      case "regional-outage":
        for (const s of x.e.affected) {
          const v = this.svcs.get(s)!;
          obs.push({ label: `${s} pods`, requested: "0 ready", observed: `${v.ready}/${v.replicas} ready`, confirmed: v.ready === 0 });
        }
        break;
      case "kafka-broker-failure":
      case "broker-crash":
        brokerObs(Number(p.broker));
        obs.push({ label: "Under-replicated partitions", requested: "> 0", observed: String(this.brokerStates().reduce((a, b) => a + b.underReplicated, 0)), confirmed: this.brokerStates().some((b) => b.underReplicated > 0) });
        break;
      case "broker-rolling-restart":
        for (const b of [0, 1, 2]) obs.push({ label: `Broker ${b} restart`, requested: "down, then back", observed: this.brokerUp[b] ? "up" : "restarting", confirmed: !this.brokerUp[b] || this.catchUpUntil[b]! > 0 });
        break;
      case "broker-outage":
        for (const b of [0, 1, 2].filter((b) => b !== Number(p.survivor))) brokerObs(b);
        obs.push({ label: "Producers stalled", requested: "orders/s → 0", observed: `${fmt(k.ordersPerSec)} orders/s`, confirmed: k.ordersPerSec < 5 });
        break;
      case "network-chaos":
        lagObs(String(p.service), 300);
        break;
      case "poison-message":
        obs.push({ label: "DLQ rate", requested: "> 0", observed: `${k.dlqPerMin.toFixed(0)} records/min`, confirmed: k.dlqPerMin > 0.5 });
        break;
      case "duplicate-delivery":
        obs.push({ label: "Duplicates absorbed", requested: "no double charge", observed: `success rate ${pct(k.paymentSuccessRate)}, revenue ${fmt(k.revenuePerMin)} €/min`, confirmed: true });
        break;
    }
    x.e.observations = obs.map((o) => {
      if (o.confirmed && !x.confirmedAt.has(o.label)) x.confirmedAt.set(o.label, iso(this.now));
      return { ...o, confirmed: x.confirmedAt.has(o.label), at: x.confirmedAt.get(o.label) ?? iso(this.now) };
    });
  }

  // ------------------------------------------------------------------ drills & timeline

  private requireDrill(id: string): Drill {
    if (!this.drill || this.drill.id !== id) throw new ApiError(404, "not_found", `drill ${id} not found`);
    if (this.drill.status !== "running") throw new ApiError(409, "drill_finished", `drill ${id} is ${this.drill.status}`);
    return this.drill;
  }

  private endDrill(d: Drill, status: "solved" | "revealed" | "expired"): void {
    d.status = status;
    d.solvedAt = iso(this.now);
    d.scenario = this.drillScenario;
    const x = this.experiments.find((e) => e.drillId === d.id);
    if (x) {
      x.e.scenario = x.def.id;
      x.e.name = `${x.e.name}: ${x.def.name}`;
      x.e.params = this.defaultParams(x.def);
      x.e.affected = this.affectedOf(x.def, x.e.params);
      if (x.e.status === "running" || x.e.status === "scheduled") this.finishExperiment(x, "completed");
    }
    const label = { solved: `solved by ${this.drillPlayer} (${d.score} pts)`, revealed: "revealed", expired: "expired" }[status];
    this.addTimeline("drill", status === "solved" ? "success" : "info", `Incident drill ${label}`, `It was ${scenarioById(this.drillScenario!)?.name}`);
  }

  addTimeline(kind: TimelineEntry["kind"], severity: TimelineEntry["severity"], title: string, detail?: string, experimentId?: string): void {
    const t: TimelineEntry = { id: `tl-${String(++this.tlCounter).padStart(5, "0")}`, at: iso(this.now), kind, severity, title };
    if (detail) t.detail = detail;
    if (experimentId) t.experimentId = experimentId;
    this.timeline.unshift(t);
    if (this.timeline.length > 500) this.timeline.length = 500;
    this.push("timeline", t);
  }
}
