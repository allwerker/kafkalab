/**
 * Canonical controlboard API types.
 *
 * The first section is copied VERBATIM from docs/contracts/controlboard-api.md (FROZEN); the only
 * change is the added `export` keyword. Do not edit it by hand — re-copy it from the contract.
 * The second section holds client-side helper types derived from the REST/WebSocket tables.
 */

// ---- BEGIN CONTRACT (verbatim) ----
export type Severity = "low" | "medium" | "high" | "critical";
export type Health = "up" | "degraded" | "down" | "unknown";

export interface ServiceState {
  name: string;               // "order-generator" | "payment" | "inventory" | "shipping" | "notification" | "analytics"
  replicas: number;           // desired
  readyReplicas: number;
  paused: boolean;
  desiredGeneration: number;  // control snapshot generation the board published
  appliedGeneration: number;  // min generation reported by the service's pods (observed)
  health: Health;
  knobs: Record<string, number | boolean | string>; // non-default knobs only
}

export interface BrokerState {
  id: number;                 // node id 0..2
  up: boolean;
  isController: boolean;      // active KRaft controller
  leaderCount: number;
  underReplicated: number;
}

export interface Kpis {
  ordersPerSec: number;
  revenuePerMin: number;        // EUR
  paymentSuccessRate: number;   // 0..1
  reservationSuccessRate: number;
  e2eP95Ms: number;             // order created -> shipped
  inFlight: number;
  dlqPerMin: number;
  totalLag: number;
}

export interface ScenarioParam {
  key: string; label: string;
  type: "number" | "boolean" | "select" | "service" | "broker";
  min?: number; max?: number; step?: number; unit?: string;
  options?: string[]; default: number | boolean | string;
}

export interface ScenarioDef {
  id: string;                   // kebab-case, e.g. "black-friday"
  name: string; description: string;
  category: "traffic" | "service" | "kubernetes" | "broker" | "network" | "data";
  severity: Severity;
  destructive: boolean;         // requires armed chaos + typed confirmation
  defaultDurationSec: number;
  params: ScenarioParam[];
  expectedSymptoms: string[];   // human readable
  dashboards: { title: string; url: string }[];
}

export type ExperimentStatus = "scheduled" | "running" | "recovering" | "completed" | "aborted" | "failed";

export interface Observation {        // "requested vs observed"
  label: string;               // e.g. "Broker 1 stopped"
  requested: string;           // what we asked for
  observed: string;            // what metrics/API confirm right now
  confirmed: boolean;
  at: string;
}

export interface Experiment {
  id: string;
  scenario: string;            // "hidden" during an unsolved drill
  name: string;                // "Incident Drill #3" during a drill
  status: ExperimentStatus;
  params: Record<string, number | boolean | string>;
  createdAt: string; startsAt: string; endsAt: string;
  endedAt?: string;
  autoRecover: boolean;
  affected: string[];          // services / brokers
  observations: Observation[];
  report?: ExperimentReport;
}

export interface ExperimentReport {
  timeToDetectSec?: number;    // first related alert firing
  timeToRecoverSec?: number;   // KPIs back to baseline after end
  peakLag: number;
  ordersAffected: number;
  revenueImpactEur: number;
  alerts: string[];            // alert names that fired
}

export interface Alert {
  fingerprint: string;
  name: string;
  status: "pending" | "firing" | "resolved";
  severity: "critical" | "warning" | "info";
  component: string;
  summary: string; description: string;
  startsAt: string; endsAt?: string;
  labels: Record<string, string>;
  dashboardUrl?: string; runbookUrl?: string; generatorUrl?: string;
}

export interface TimelineEntry {
  id: string; at: string;
  kind: "experiment" | "config" | "alert" | "recovery" | "simulation" | "drill" | "system";
  severity: "info" | "warning" | "critical" | "success";
  title: string; detail?: string;
  experimentId?: string;
}

export interface Link {
  id: string; title: string; url: string; description: string;
  icon: string;                // lucide icon name
  health: Health;
}

export interface State {
  simulation: "running" | "stopped";
  chaosArmed: boolean;
  services: ServiceState[];
  brokers: BrokerState[];
  kpis: Kpis;
  experiments: Experiment[];   // active + last 20
  alerts: Alert[];             // active (pending + firing) + resolved in last 15 min
  links: Link[];
  drill?: Drill;
}

export interface TuningSetting {
  key: "acks" | "lingerMs" | "batchMaxBytes" | "compression" | "fetchMaxBytes" | "fetchMaxWaitMs" | "fetchMinBytes" | "maxPollRecords";
  label: string; type: "number" | "select";
  min?: number; max?: number; step?: number; options?: string[];
  default: number | string; lossy?: string[]; // values that weaken delivery guarantees
  help: string;
}

export interface Drill {
  id: string; status: "running" | "solved" | "revealed" | "expired";
  difficulty: "easy" | "medium" | "hard";
  startedAt: string; solvedAt?: string;
  hintsUsed: number; hints: string[];     // revealed hints only
  guesses: { scenario: string; correct: boolean; at: string }[];
  score?: number;
  scenario?: string;                      // only after solved/revealed
  candidates: { id: string; name: string }[]; // scenarios the player can guess
}

export interface LeaderboardEntry { player: string; score: number; scenario: string; durationSec: number; at: string; }

export interface LiveEvent {          // sampled business event for the live view
  topic: string; partition: number; offset: number;
  orderId: string;
  kind: "created" | "paid" | "payment-failed" | "refunded" | "reserved" | "rejected" | "released"
      | "shipped" | "cancelled" | "notified" | "dlq";
  at: string;
  valueCents?: number; region?: string; reason?: string; carrier?: string;
}

export interface OrderTimeline {
  orderId: string;
  events: LiveEvent[];
  traceId?: string; traceUrl?: string; logsUrl?: string;
}
// ---- END CONTRACT ----

// ---- Derived helper types (REST + WebSocket tables of the contract) ----

export type Category = ScenarioDef["category"];
export type ParamValue = number | boolean | string;
export type AlertStatus = Alert["status"];
export type AlertSeverity = Alert["severity"];
export type TimelineKind = TimelineEntry["kind"];
export type LiveEventKind = LiveEvent["kind"];
export type TuningKey = TuningSetting["key"];
export type Difficulty = Drill["difficulty"];
export type SimulationStatus = State["simulation"];

export interface ApiErrorBody {
  error: string;
  message: string;
}

export interface StartExperimentRequest {
  scenario: string;
  params: Record<string, ParamValue>;
  durationSec: number;
  startDelaySec?: number;
  autoRecover: boolean;
  confirm?: string;
}

export interface TuningResponse {
  settings: TuningSetting[];
  services: Record<string, Partial<Record<TuningKey, number | string>>>;
}

export type TuningUpdate = Partial<Record<TuningKey, number | string | null>>;

/** Subset of `Knobs` (schemas/platform.avdl) that the controlboard edits directly. */
export interface KnobsPatch {
  paused?: boolean;
  processingLatencyMs?: number;
  processingJitterMs?: number;
  processingErrorProbability?: number;
  businessFailureProbability?: number | null;
  providerDown?: boolean;
  dbLatencyMs?: number;
  netLatencyMs?: number;
  netDropProbability?: number;
  warehouseOutage?: boolean;
  ordersPerSecond?: number | null;
  popularSku?: string | null;
  popularSkuProbability?: number;
  hotPartition?: number | null;
  hotPartitionRatio?: number;
  paddingBytes?: number;
  duplicateProbability?: number;
  poisonProbability?: number;
}

export interface Tick {
  kpis: Kpis;
  services: ServiceState[];
  brokers: BrokerState[];
  rates: Record<string, number>;
  lag: Record<string, Record<string, number[]>>;
  partitions: Record<string, number>;
}

export interface EventsBatch {
  scale: number;
  items: LiveEvent[];
}

export interface Hello {
  version: number;
  serverTime: string;
}

interface Envelope<T extends string, D> {
  type: T;
  ts: string;
  data: D;
}

export type ServerMessage =
  | Envelope<"hello", Hello>
  | Envelope<"state", State>
  | Envelope<"tick", Tick>
  | Envelope<"events", EventsBatch>
  | Envelope<"alert", Alert>
  | Envelope<"experiment", Experiment>
  | Envelope<"timeline", TimelineEntry>
  | Envelope<"drill", Drill>;

export type ServerMessageType = ServerMessage["type"];
