/**
 * Pure scene model of the live view. Consumes `tick` and `events` messages and produces a
 * render-ready state (stations, lanes with lag piles, parcels, trucks, brokers, effects, weather).
 * No Phaser, no DOM: the render layer only reads the public fields every frame.
 */
import type { Category, EventsBatch, Experiment, LiveEvent, LiveEventKind, Tick } from "../../api/types";
import { SERVICE_LABELS, SERVICE_NAMES, TOPIC_PARTITIONS, type ServiceName } from "../../lib/topology";
import { clamp, dist, lerp, pointAt, type Vec } from "./geometry";
import { BROKER_POS, BROKER_SIZE, COLORS, DLQ_POS, STATION_POS, STATION_RADIUS, TRUCK_DOCK, WORLD, buildLanes, laneId, type Lane } from "./layout";
import { Pool } from "./pool";
import { stationRate, stationStatus, weatherFor } from "./stations";
import type { AnalyticsScreen, BrokerView, Effect, EffectKind, Hit, LaneView, Parcel, StationView, Tooltip, Truck, Weather } from "./types";

export const MAX_PARCELS = 600;
/** Longest frame the model simulates in one go (slow frames still advance, tab returns do not jump). */
export const MAX_STEP_MS = 250;
export const MAX_TRUCKS = 8;
export const MAX_EFFECTS = 240;
export const PILE_CAP = 12;
export const PILE_SPACING = 9;
export const PILE_GAP = 4;
export const PARCEL_SPEED = 240; // world px per second
export const BATCH_SPREAD_MS = 250;
export const TRUCK_CAPACITY = 3;
export const TRUCK_MAX_LOADING_MS = 1400;
const HISTORY = 60;

export const KIND_COLOR: Record<LiveEventKind, number> = {
  created: COLORS.cyan,
  paid: COLORS.gold,
  "payment-failed": COLORS.red,
  refunded: COLORS.gold,
  reserved: COLORS.green,
  rejected: COLORS.orange,
  released: COLORS.green,
  shipped: COLORS.magenta,
  cancelled: COLORS.grey,
  notified: COLORS.blue,
  dlq: COLORS.red,
};

export const KIND_LABEL: Record<LiveEventKind, string> = {
  created: "Order created",
  paid: "Payment completed",
  "payment-failed": "Payment failed",
  refunded: "Payment refunded",
  reserved: "Stock reserved",
  rejected: "Reservation rejected",
  released: "Stock released",
  shipped: "Order shipped",
  cancelled: "Order cancelled",
  notified: "Customer notified",
  dlq: "Dead-lettered",
};

/** Lane bundle(s) an event travels on, as `topic>consumer`. */
const KIND_LANES: Partial<Record<LiveEventKind, string[]>> = {
  created: ["orders.created>payment", "orders.created>inventory"],
  paid: ["payments.completed>shipping"],
  "payment-failed": ["payments.failed>shipping"],
  reserved: ["inventory.reserved>shipping"],
  rejected: ["inventory.rejected>shipping"],
  shipped: ["orders.shipped>notification"],
  cancelled: ["orders.cancelled>notification"],
  refunded: ["payments.refunded>notification"],
  notified: ["notifications.sent>analytics"],
};

/** Station that flashes when the event happens. */
const KIND_STATION: Record<LiveEventKind, ServiceName> = {
  created: "order-generator",
  paid: "payment",
  "payment-failed": "payment",
  refunded: "payment",
  reserved: "inventory",
  rejected: "inventory",
  released: "inventory",
  shipped: "shipping",
  cancelled: "shipping",
  notified: "notification",
  dlq: "payment",
};

const EFFECT_TTL: Record<EffectKind, number> = {
  coin: 700,
  burst: 600,
  cross: 900,
  wave: 1200,
  coinBack: 900,
  release: 800,
  splash: 600,
  spark: 400,
};

const KIND_EFFECT: Partial<Record<LiveEventKind, EffectKind>> = {
  paid: "coin",
  "payment-failed": "burst",
  rejected: "cross",
  released: "release",
  refunded: "coinBack",
  notified: "wave",
};

interface Pending {
  due: number;
  event: LiveEvent;
}

export interface LiveModelOptions {
  maxParcels?: number;
  random?: () => number;
}

export class LiveModel {
  time = 0;
  eventScale = 1;
  dlqCount = 0;
  dropped = 0;
  hasTick = false;
  stations: StationView[];
  lanes: LaneView[] = [];
  brokers: BrokerView[];
  weather: Weather[] = [];
  analytics: AnalyticsScreen = { history: [], ordersPerSec: 0, revenuePerMin: 0 };
  readonly parcels: Pool<Parcel>;
  readonly effects: Pool<Effect>;
  readonly trucks: Pool<Truck>;
  private laneById = new Map<string, LaneView>();
  private lanesByBundle = new Map<string, LaneView[]>();
  private partitionsKey = "";
  private pending: Pending[] = [];
  private nextId = 1;
  private readonly random: () => number;

  constructor(opts: LiveModelOptions = {}) {
    this.random = opts.random ?? Math.random;
    this.parcels = new Pool<Parcel>(() => this.blankParcel(), opts.maxParcels ?? MAX_PARCELS);
    this.effects = new Pool<Effect>(() => ({ id: 0, kind: "spark", x: 0, y: 0, tx: 0, ty: 0, age: 0, ttl: 0, color: 0, seed: 0 }), MAX_EFFECTS);
    this.trucks = new Pool<Truck>(() => ({ id: 0, x: 0, y: 0, vx: 0, load: 0, loadingMs: 0, carrier: "", state: "loading", dead: false }), MAX_TRUCKS);
    this.stations = SERVICE_NAMES.map((id) => ({
      id,
      label: SERVICE_LABELS[id],
      ...STATION_POS[id],
      status: "unknown",
      replicas: 0,
      ready: 0,
      lag: 0,
      rate: 0,
      pulse: 0,
      affected: false,
      applying: false,
    }));
    this.brokers = BROKER_POS.map((p, id) => ({ id, ...p, up: true, controller: id === 0, leaders: 0, underReplicated: 0, downForMs: -1 }));
    this.setPartitions(TOPIC_PARTITIONS);
  }

  // ---------------------------------------------------------------- inputs

  applyTick(tick: Tick): void {
    this.hasTick = true;
    this.setPartitions(tick.partitions);
    for (const lane of this.lanes) {
      lane.lag = tick.lag[lane.group]?.[lane.topic]?.[lane.partition] ?? 0;
    }
    this.updatePiles();
    for (const st of this.stations) {
      const s = tick.services.find((x) => x.name === st.id);
      st.status = stationStatus(s);
      st.replicas = s?.replicas ?? 0;
      st.ready = s?.readyReplicas ?? 0;
      st.applying = !!s && s.appliedGeneration < s.desiredGeneration;
      const groupLag = tick.lag[st.id];
      st.lag = groupLag ? Object.values(groupLag).reduce((sum, arr) => sum + arr.reduce((a, b) => a + b, 0), 0) : 0;
      st.rate = stationRate(st.id, tick.rates);
    }
    for (const b of this.brokers) {
      const s = tick.brokers.find((x) => x.id === b.id);
      const up = s ? s.up : true;
      if (b.up && !up) b.downForMs = 0;
      if (up) b.downForMs = -1;
      b.up = up;
      b.controller = !!s?.isController;
      b.leaders = s?.leaderCount ?? 0;
      b.underReplicated = s?.underReplicated ?? 0;
    }
    const h = this.analytics.history;
    h.push(tick.kpis.ordersPerSec);
    if (h.length > HISTORY) h.shift();
    this.analytics.ordersPerSec = tick.kpis.ordersPerSec;
    this.analytics.revenuePerMin = tick.kpis.revenuePerMin;
  }

  applyEvents(batch: EventsBatch): void {
    this.eventScale = Math.max(1, batch.scale);
    this.updatePiles();
    const n = batch.items.length;
    batch.items.forEach((event, i) => this.pending.push({ due: this.time + (i / n) * BATCH_SPREAD_MS, event }));
  }

  applyExperiments(experiments: Experiment[], categories: Record<string, Category>): void {
    this.weather = weatherFor(experiments, categories);
    const affected = new Set(experiments.filter((e) => e.status === "running").flatMap((e) => e.affected));
    for (const st of this.stations) st.affected = affected.has(st.id);
  }

  // ---------------------------------------------------------------- simulation

  step(dtMs: number): void {
    const dt = clamp(dtMs, 0, MAX_STEP_MS);
    this.time += dt;
    if (this.pending.length) {
      const due = this.pending.filter((p) => p.due <= this.time);
      if (due.length) {
        this.pending = this.pending.filter((p) => p.due > this.time);
        for (const p of due) this.spawn(p.event);
      }
    }
    for (const st of this.stations) st.pulse = Math.max(0, st.pulse - dt / 600);
    for (const lane of this.lanes) lane.activity = Math.max(0, lane.activity - dt / 1500);
    for (const b of this.brokers) if (!b.up) b.downForMs += dt;
    this.stepParcels(dt);
    this.stepTrucks(dt);
    for (const e of this.effects.live) e.age += dt;
    this.effects.sweep((e) => e.age >= e.ttl);
  }

  private stepParcels(dt: number): void {
    for (const p of this.parcels.live) {
      p.age += dt;
      if (p.state === "falling") {
        const f = clamp(p.age / 900, 0, 1);
        p.x = lerp(p.fromX, p.toX, f);
        p.y = lerp(p.fromY, p.toY, f * f);
        p.alpha = 1 - f * 0.3;
        if (f >= 1) {
          p.dead = true;
          this.dlqCount++;
          this.addEffect("splash", p.toX, p.toY, COLORS.red);
        }
        continue;
      }
      const lane = this.laneById.get(p.laneId);
      if (!lane) {
        p.dead = true;
        continue;
      }
      if (p.state === "absorbing") {
        p.alpha = Math.max(0, p.alpha - dt / 180);
        p.size = Math.max(0, p.size - dt / 60);
        if (p.alpha <= 0) p.dead = true;
        continue;
      }
      const tail = this.pileTail(lane);
      p.t = Math.min(tail, p.t + (p.speed * dt) / 1000 / lane.path.length);
      const pos = pointAt(lane.path, p.t);
      p.x = pos.x;
      p.y = pos.y;
      if (p.fade) p.alpha = Math.max(0.15, 1 - p.t);
      if (p.t >= tail) p.state = "absorbing";
    }
    this.parcels.sweep((p) => p.dead);
  }

  private stepTrucks(dt: number): void {
    for (const t of this.trucks.live) {
      if (t.state === "loading") {
        t.loadingMs += dt;
        if (t.load >= TRUCK_CAPACITY || t.loadingMs >= TRUCK_MAX_LOADING_MS) t.state = "departing";
      } else {
        t.vx = Math.min(360, t.vx + dt * 0.5);
        t.x += (t.vx * dt) / 1000;
        if (t.x > WORLD.width + 80) t.dead = true;
      }
    }
    this.trucks.sweep((t) => t.dead);
  }

  // ---------------------------------------------------------------- spawning

  private spawn(e: LiveEvent): void {
    const station = this.station(KIND_STATION[e.kind]);
    station.pulse = 1;
    const fx = KIND_EFFECT[e.kind];
    if (fx === "coinBack") {
      const gen = STATION_POS["order-generator"];
      this.addEffect(fx, station.x, station.y, KIND_COLOR[e.kind], gen.x, gen.y);
    } else if (fx) {
      this.addEffect(fx, station.x, station.y, KIND_COLOR[e.kind]);
    }
    if (e.kind === "shipped") this.loadTruck(e.carrier ?? "");
    if (e.kind === "dlq") {
      const p = this.newParcel(e);
      if (p) {
        p.state = "falling";
        p.fromX = p.x = station.x + 30;
        p.fromY = p.y = station.y + STATION_RADIUS;
        p.toX = DLQ_POS.x;
        p.toY = DLQ_POS.y;
      }
      return;
    }
    for (const bundle of KIND_LANES[e.kind] ?? []) {
      const lanes = this.lanesByBundle.get(bundle)!;
      const lane = lanes[e.partition % lanes.length]!;
      const p = this.newParcel(e);
      if (!p) return;
      p.laneId = lane.id;
      p.fade = e.kind === "cancelled";
      const start = lane.path.points[0]!;
      p.x = start.x;
      p.y = start.y;
      lane.activity = 1;
    }
  }

  private newParcel(e: LiveEvent): Parcel | null {
    const p = this.parcels.acquire();
    if (!p) {
      this.dropped++;
      return null;
    }
    Object.assign(p, this.blankParcel(), {
      id: this.nextId++,
      orderId: e.orderId,
      kind: e.kind,
      topic: e.topic,
      partition: e.partition,
      color: KIND_COLOR[e.kind],
      speed: PARCEL_SPEED * (0.8 + 0.4 * this.random()),
    });
    return p;
  }

  private blankParcel(): Parcel {
    return { id: 0, orderId: "", kind: "created", topic: "", partition: 0, laneId: "", t: 0, speed: PARCEL_SPEED, x: 0, y: 0, fromX: 0, fromY: 0, toX: 0, toY: 0, color: 0, alpha: 1, size: 7, state: "moving", age: 0, fade: false, dead: false };
  }

  private loadTruck(carrier: string): void {
    const loading = this.trucks.live.find((t) => t.state === "loading");
    if (loading) {
      loading.load++;
      return;
    }
    const t = this.trucks.acquire();
    if (!t) return;
    Object.assign(t, { id: this.nextId++, x: TRUCK_DOCK.x, y: TRUCK_DOCK.y, vx: 0, load: 1, loadingMs: 0, carrier, state: "loading", dead: false });
  }

  private addEffect(kind: EffectKind, x: number, y: number, color: number, tx = x, ty = y): void {
    const e = this.effects.acquire();
    if (!e) return;
    Object.assign(e, { id: this.nextId++, kind, x, y, tx, ty, age: 0, ttl: EFFECT_TTL[kind], color, seed: this.random() });
  }

  // ---------------------------------------------------------------- lanes & piles

  private setPartitions(partitions: Record<string, number>): void {
    const key = JSON.stringify(Object.entries(partitions).sort());
    if (key === this.partitionsKey) return;
    this.partitionsKey = key;
    const old = this.laneById;
    this.lanes = buildLanes(partitions).map((l: Lane) => ({ ...l, lag: old.get(l.id)?.lag ?? 0, pile: 0, overflow: false, activity: 0 }));
    this.laneById = new Map(this.lanes.map((l) => [l.id, l]));
    this.lanesByBundle = new Map();
    for (const l of this.lanes) {
      const key = `${l.topic}>${l.to}`;
      const arr = this.lanesByBundle.get(key) ?? [];
      arr.push(l);
      this.lanesByBundle.set(key, arr);
    }
    this.updatePiles();
  }

  /** Lag in real records represented by one queued parcel. */
  get pileUnit(): number {
    return this.eventScale;
  }

  private updatePiles(): void {
    for (const lane of this.lanes) {
      const want = lane.lag > 0 ? Math.ceil(lane.lag / this.pileUnit) : 0;
      lane.pile = Math.min(PILE_CAP, want);
      lane.overflow = want > PILE_CAP;
    }
  }

  /** Path fraction where the queue in front of the consumer ends (moving parcels stop here). */
  pileTail(lane: LaneView): number {
    return 1 - (PILE_GAP + lane.pile * PILE_SPACING) / lane.path.length;
  }

  /** World positions of the queued parcels of a lane, front (at the station) first. */
  pileSlots(lane: LaneView): Vec[] {
    const slots: Vec[] = [];
    for (let i = 0; i < lane.pile; i++) slots.push(pointAt(lane.path, 1 - (PILE_GAP + i * PILE_SPACING) / lane.path.length));
    return slots;
  }

  lane(topic: string, to: string, partition: number): LaneView | undefined {
    return this.laneById.get(laneId(topic, to, partition));
  }

  station(id: ServiceName): StationView {
    return this.stations.find((s) => s.id === id)!;
  }

  /** "1 parcel = N orders" label for the legend. */
  get scaleLabel(): string {
    return `1 parcel = ${this.eventScale.toLocaleString("en")} ${this.eventScale === 1 ? "order" : "orders"}`;
  }

  // ---------------------------------------------------------------- interaction

  hitTest(x: number, y: number): Hit | null {
    const live = this.parcels.live;
    for (let i = live.length - 1; i >= 0; i--) {
      const p = live[i]!;
      if (p.state !== "absorbing" && dist(p, { x, y }) <= 10) return { type: "parcel", parcel: p };
    }
    for (const b of this.brokers) {
      if (Math.abs(x - b.x) <= BROKER_SIZE.w / 2 && Math.abs(y - b.y) <= BROKER_SIZE.h / 2) return { type: "broker", broker: b };
    }
    for (const s of this.stations) if (dist(s, { x, y }) <= STATION_RADIUS) return { type: "station", station: s };
    if (dist(DLQ_POS, { x, y }) <= 46) return { type: "dlq" };
    return null;
  }

  describe(hit: Hit): Tooltip {
    switch (hit.type) {
      case "parcel": {
        const p = hit.parcel;
        return { title: `Order ${p.orderId.slice(0, 8)}`, lines: [KIND_LABEL[p.kind], `${p.topic} · partition ${p.partition}`, "Click for the order timeline"] };
      }
      case "station": {
        const s = hit.station;
        const lines = [`Status: ${s.status}${s.applying ? " (applying config…)" : ""}`, `Replicas: ${s.ready}/${s.replicas} ready`, `Throughput: ${s.rate.toFixed(0)} records/s`];
        if (s.id !== "order-generator") lines.push(`Consumer lag: ${s.lag.toLocaleString("en")}`);
        return { title: s.label, lines };
      }
      case "broker": {
        const b = hit.broker;
        return {
          title: `Broker ${b.id}${b.controller ? " · KRaft controller" : ""}`,
          lines: [b.up ? "Up" : "DOWN", `Partition leaders: ${b.leaders}`, `Under-replicated: ${b.underReplicated}`],
        };
      }
      case "dlq":
        return { title: "Dead-letter queue (orders.dlq)", lines: [`${this.dlqCount} sampled records fell in`, "Undecodable or permanently failing records"] };
    }
  }
}
