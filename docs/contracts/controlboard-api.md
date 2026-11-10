# Controlboard API contract (FROZEN)

Base: `http://board.localhost:8080`. JSON, camelCase, timestamps as RFC 3339 strings (UTC),
durations in seconds unless the field name says `Ms`. Errors: HTTP 4xx/5xx with
`{"error": "<code>", "message": "<human text>"}`.

The canonical type definitions are TypeScript below; the Go backend mirrors them in
`controlboard/backend/internal/api/types.go` and a test asserts the recorded fixtures in
`controlboard/api/fixtures/` decode into the Go types without unknown fields.

```ts
type Severity = "low" | "medium" | "high" | "critical";
type Health = "up" | "degraded" | "down" | "unknown";

interface ServiceState {
  name: string;               // "order-generator" | "payment" | "inventory" | "shipping" | "notification" | "analytics"
  replicas: number;           // desired
  readyReplicas: number;
  paused: boolean;
  desiredGeneration: number;  // control snapshot generation the board published
  appliedGeneration: number;  // min generation reported by the service's pods (observed)
  health: Health;
  knobs: Record<string, number | boolean | string>; // non-default knobs only
}

interface BrokerState {
  id: number;                 // node id 0..2
  up: boolean;
  isController: boolean;      // active KRaft controller
  leaderCount: number;
  underReplicated: number;
}

interface Kpis {
  ordersPerSec: number;
  revenuePerMin: number;        // EUR
  paymentSuccessRate: number;   // 0..1
  reservationSuccessRate: number;
  e2eP95Ms: number;             // order created -> shipped
  inFlight: number;
  dlqPerMin: number;
  totalLag: number;
}

interface ScenarioParam {
  key: string; label: string;
  type: "number" | "boolean" | "select" | "service" | "broker";
  min?: number; max?: number; step?: number; unit?: string;
  options?: string[]; default: number | boolean | string;
}

interface ScenarioDef {
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

type ExperimentStatus = "scheduled" | "running" | "recovering" | "completed" | "aborted" | "failed";

interface Observation {        // "requested vs observed"
  label: string;               // e.g. "Broker 1 stopped"
  requested: string;           // what we asked for
  observed: string;            // what metrics/API confirm right now
  confirmed: boolean;
  at: string;
}

interface Experiment {
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

interface ExperimentReport {
  timeToDetectSec?: number;    // first related alert firing
  timeToRecoverSec?: number;   // KPIs back to baseline after end
  peakLag: number;
  ordersAffected: number;
  revenueImpactEur: number;
  alerts: string[];            // alert names that fired
}

interface Alert {
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

interface TimelineEntry {
  id: string; at: string;
  kind: "experiment" | "config" | "alert" | "recovery" | "simulation" | "drill" | "system";
  severity: "info" | "warning" | "critical" | "success";
  title: string; detail?: string;
  experimentId?: string;
}

interface Link {
  id: string; title: string; url: string; description: string;
  icon: string;                // lucide icon name
  health: Health;
}

interface State {
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

interface TuningSetting {
  key: "acks" | "lingerMs" | "batchMaxBytes" | "compression" | "fetchMaxBytes" | "fetchMaxWaitMs" | "fetchMinBytes" | "maxPollRecords";
  label: string; type: "number" | "select";
  min?: number; max?: number; step?: number; options?: string[];
  default: number | string; lossy?: string[]; // values that weaken delivery guarantees
  help: string;
}

interface Drill {
  id: string; status: "running" | "solved" | "revealed" | "expired";
  difficulty: "easy" | "medium" | "hard";
  startedAt: string; solvedAt?: string;
  hintsUsed: number; hints: string[];     // revealed hints only
  guesses: { scenario: string; correct: boolean; at: string }[];
  score?: number;
  scenario?: string;                      // only after solved/revealed
  candidates: { id: string; name: string }[]; // scenarios the player can guess
}

interface LeaderboardEntry { player: string; score: number; scenario: string; durationSec: number; at: string; }

interface LiveEvent {          // sampled business event for the live view
  topic: string; partition: number; offset: number;
  orderId: string;
  kind: "created" | "paid" | "payment-failed" | "refunded" | "reserved" | "rejected" | "released"
      | "shipped" | "cancelled" | "notified" | "dlq";
  at: string;
  valueCents?: number; region?: string; reason?: string; carrier?: string;
}

interface OrderTimeline {
  orderId: string;
  events: LiveEvent[];
  traceId?: string; traceUrl?: string; logsUrl?: string;
}
```

## REST
| Method & path | Body | Response |
|---|---|---|
| `GET /api/state` | – | `State` |
| `GET /api/scenarios` | – | `ScenarioDef[]` |
| `POST /api/experiments` | `{scenario, params, durationSec, startDelaySec?, autoRecover, confirm?}` | `Experiment` (`409 chaos_disarmed`, `409 broker_fault_active`, `400 confirmation_required` — `confirm` must equal the scenario id) |
| `DELETE /api/experiments/{id}` | – | `Experiment` (stops and reverts) |
| `POST /api/chaos/arm` | `{armed: boolean}` | `{armed}` |
| `POST /api/panic` | – | `{reverted: number}` — reverts everything, disarms chaos |
| `POST /api/simulation` | `{action: "start"\|"stop"\|"reset"}` | `{simulation}` |
| `PUT /api/services/{name}/knobs` | `Partial<Knobs>` (see schemas/platform.avdl) | `ServiceState` (manual override, no expiry) |
| `DELETE /api/services/{name}/knobs` | – | `ServiceState` (back to baseline) |
| `POST /api/services/{name}/scale` | `{replicas: 0..6}` | `ServiceState` |
| `GET /api/timeline?limit=200` | – | `TimelineEntry[]` newest first |
| `GET /api/alerts` | – | `Alert[]` |
| `POST /api/alerts/test` | – | `204`; fires `LabTestAlert` through vmalert → Alertmanager → webhook |
| `POST /api/alerts/webhook` | Alertmanager webhook v4 | `204` |
| `GET /api/links` | – | `Link[]` |
| `GET /api/tuning` | – | `{settings: TuningSetting[], services: {[name]: Record<key, value>}}` |
| `PUT /api/tuning/{service}` | `Record<key, value \| null>` | same as GET |
| `POST /api/drills` | `{difficulty, player}` | `Drill` |
| `POST /api/drills/{id}/hint` | – | `Drill` |
| `POST /api/drills/{id}/guess` | `{scenario}` | `Drill` |
| `POST /api/drills/{id}/reveal` | – | `Drill` |
| `GET /api/drills/leaderboard` | – | `LeaderboardEntry[]` |
| `GET /api/orders/{orderId}` | – | `OrderTimeline` (from an in-memory ring of recent orders) |
| `GET /healthz`, `/readyz`, `/metrics` | – | – |

## WebSocket `GET /api/ws`
Server → client messages, each `{ "type": string, "ts": string, "data": ... }`:

| type | data | when |
|---|---|---|
| `hello` | `{version: 1, serverTime}` | on connect |
| `state` | `State` | on connect and every 10 s |
| `tick` | `{kpis: Kpis, services: ServiceState[], brokers: BrokerState[], rates: Record<topic, number>, lag: Record<group, Record<topic, number[]>>, partitions: Record<topic, number>}` | every 1 s; `lag[group][topic][p]` = lag of partition p |
| `events` | `{scale: number, items: LiveEvent[]}` | every 250 ms; `scale` = real events per item (adaptive sampling, items ≤ 40 per message) |
| `alert` | `Alert` | on every status change |
| `experiment` | `Experiment` | on every change |
| `timeline` | `TimelineEntry` | on every new entry |
| `drill` | `Drill` | on every change |

Client → server: `{"type": "ping"}` (server answers `{"type":"pong"}`). Clients reconnect with
exponential backoff (1 s → 15 s) and rely on the `state` message to resync.

Consumer group names for `lag`: `payment`, `inventory`, `shipping`, `notification`, `analytics`
(+ `controlboard-*` is never a group; the board reads without a group).
