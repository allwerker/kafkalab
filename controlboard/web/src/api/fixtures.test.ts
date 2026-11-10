/**
 * Guards the recorded fixtures in controlboard/api/fixtures/ against drifting from the contract:
 * every object must have exactly the contract's fields (required present, nothing unknown).
 * The Go backend runs the same check against its own types.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const dir = join(__dirname, "..", "..", "..", "api", "fixtures");
const load = (name: string): unknown => JSON.parse(readFileSync(join(dir, name), "utf8"));

type Shape = { req: string[]; opt?: string[]; nested?: Record<string, Shape | [Shape]> };

const kpis: Shape = { req: ["ordersPerSec", "revenuePerMin", "paymentSuccessRate", "reservationSuccessRate", "e2eP95Ms", "inFlight", "dlqPerMin", "totalLag"] };
const service: Shape = { req: ["name", "replicas", "readyReplicas", "paused", "desiredGeneration", "appliedGeneration", "health", "knobs"] };
const broker: Shape = { req: ["id", "up", "isController", "leaderCount", "underReplicated"] };
const observation: Shape = { req: ["label", "requested", "observed", "confirmed", "at"] };
const report: Shape = { req: ["peakLag", "ordersAffected", "revenueImpactEur", "alerts"], opt: ["timeToDetectSec", "timeToRecoverSec"] };
const experiment: Shape = {
  req: ["id", "scenario", "name", "status", "params", "createdAt", "startsAt", "endsAt", "autoRecover", "affected", "observations"],
  opt: ["endedAt", "report"],
  nested: { observations: [observation], report },
};
const alert: Shape = {
  req: ["fingerprint", "name", "status", "severity", "component", "summary", "description", "startsAt", "labels"],
  opt: ["endsAt", "dashboardUrl", "runbookUrl", "generatorUrl"],
};
const timeline: Shape = { req: ["id", "at", "kind", "severity", "title"], opt: ["detail", "experimentId"] };
const link: Shape = { req: ["id", "title", "url", "description", "icon", "health"] };
const drill: Shape = {
  req: ["id", "status", "difficulty", "startedAt", "hintsUsed", "hints", "guesses", "candidates"],
  opt: ["solvedAt", "score", "scenario"],
  nested: { guesses: [{ req: ["scenario", "correct", "at"] }], candidates: [{ req: ["id", "name"] }] },
};
const state: Shape = {
  req: ["simulation", "chaosArmed", "services", "brokers", "kpis", "experiments", "alerts", "links"],
  opt: ["drill"],
  nested: { services: [service], brokers: [broker], kpis, experiments: [experiment], alerts: [alert], links: [link], drill },
};
const param: Shape = { req: ["key", "label", "type", "default"], opt: ["min", "max", "step", "unit", "options"] };
const scenario: Shape = {
  req: ["id", "name", "description", "category", "severity", "destructive", "defaultDurationSec", "params", "expectedSymptoms", "dashboards"],
  nested: { params: [param], dashboards: [{ req: ["title", "url"] }] },
};
const liveEvent: Shape = { req: ["topic", "partition", "offset", "orderId", "kind", "at"], opt: ["valueCents", "region", "reason", "carrier"] };
const tuningSetting: Shape = { req: ["key", "label", "type", "default", "help"], opt: ["min", "max", "step", "options", "lossy"] };

function check(value: unknown, shape: Shape, path: string): string[] {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return [`${path}: not an object`];
  const obj = value as Record<string, unknown>;
  const errors: string[] = [];
  for (const k of shape.req) if (!(k in obj)) errors.push(`${path}.${k}: missing`);
  const known = new Set([...shape.req, ...(shape.opt ?? [])]);
  for (const k of Object.keys(obj)) if (!known.has(k)) errors.push(`${path}.${k}: not in contract`);
  for (const [k, sub] of Object.entries(shape.nested ?? {})) {
    if (!(k in obj)) continue;
    if (Array.isArray(sub)) (obj[k] as unknown[]).forEach((v, i) => errors.push(...check(v, sub[0], `${path}.${k}[${i}]`)));
    else errors.push(...check(obj[k], sub, `${path}.${k}`));
  }
  return errors;
}

const list = (shape: Shape) => (v: unknown, p: string) => (v as unknown[]).flatMap((x, i) => check(x, shape, `${p}[${i}]`));
const one = (shape: Shape) => (v: unknown, p: string) => check(v, shape, p);

const REST: Record<string, (v: unknown, p: string) => string[]> = {
  "state.json": one(state),
  "scenarios.json": list(scenario),
  "experiment.json": one(experiment),
  "experiment-stopped.json": one(experiment),
  "timeline.json": list(timeline),
  "alerts.json": list(alert),
  "links.json": list(link),
  "service-state.json": one(service),
  "service-knobs.json": one(service),
  "drill.json": one(drill),
  "drill-solved.json": one(drill),
  "leaderboard.json": list({ req: ["player", "score", "scenario", "durationSec", "at"] }),
  "order-timeline.json": one({ req: ["orderId", "events"], opt: ["traceId", "traceUrl", "logsUrl"], nested: { events: [liveEvent] } }),
  "chaos-arm.json": one({ req: ["armed"] }),
  "panic.json": one({ req: ["reverted"] }),
  "simulation.json": one({ req: ["simulation"] }),
  "error.json": one({ req: ["error", "message"] }),
  "tuning.json": one({ req: ["settings", "services"], nested: { settings: [tuningSetting] } }),
};

const WS: Record<string, Shape> = {
  hello: { req: ["version", "serverTime"] },
  state,
  tick: { req: ["kpis", "services", "brokers", "rates", "lag", "partitions"], nested: { kpis, services: [service], brokers: [broker] } },
  events: { req: ["scale", "items"], nested: { items: [liveEvent] } },
  alert,
  experiment,
  timeline,
  drill,
};

describe("contract fixtures", () => {
  it("shouldMatchTheContractExactlyWhenRestFixturesAreDecoded", () => {
    // GIVEN: one recorded fixture per REST response type
    // WHEN: each is checked field by field against the frozen contract
    const errors = Object.entries(REST).flatMap(([file, validate]) => validate(load(file), file));
    // THEN: no field is missing and none is unknown
    expect(errors).toEqual([]);
  });

  it("shouldMatchTheContractExactlyWhenTheRecordedWebSocketStreamIsReplayed", () => {
    // GIVEN: two minutes of recorded WebSocket traffic incl. Black Friday and a broker crash
    const lines = readFileSync(join(dir, "ws-stream.jsonl"), "utf8").trim().split("\n");
    const seen = new Set<string>();
    // WHEN: every envelope and payload is checked
    const errors = lines.flatMap((line, i) => {
      const m = JSON.parse(line) as { type: string; ts: string; data: unknown };
      seen.add(m.type);
      const env = check(m, { req: ["type", "ts", "data"] }, `line ${i + 1}`);
      const shape = WS[m.type];
      return shape ? [...env, ...check(m.data, shape, `line ${i + 1} ${m.type}`)] : [...env, `line ${i + 1}: unknown type ${m.type}`];
    });
    // THEN: everything matches and every message type occurs, including a crash and a resolved alert
    expect(errors).toEqual([]);
    expect([...seen].sort()).toEqual(["alert", "events", "experiment", "hello", "state", "tick", "timeline"]);
    const alerts = lines.map((l) => JSON.parse(l) as { type: string; data: { status?: string; name?: string } }).filter((m) => m.type === "alert");
    expect(alerts.some((a) => a.data.status === "firing")).toBe(true);
    expect(alerts.some((a) => a.data.status === "resolved" && a.data.name === "KafkaBrokerDown")).toBe(true);
  });
});
