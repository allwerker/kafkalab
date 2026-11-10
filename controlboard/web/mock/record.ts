/**
 * Records the contract fixtures in controlboard/api/fixtures/ from a deterministic simulation run:
 *  - ws-stream.jsonl: ~2 minutes of WebSocket messages (Black Friday spike, broker crash,
 *    alerts firing and resolving, a test alert),
 *  - one JSON file per REST response type.
 * `bun run record` — output is byte-for-byte reproducible (fixed seed and virtual clock).
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { handleRest } from "./api.ts";
import { Sim } from "./sim.ts";

const outDir = join(import.meta.dir, "..", "..", "api", "fixtures");
mkdirSync(outDir, { recursive: true });

const START = Date.parse("2026-01-15T10:00:00.000Z");
const sim = new Sim({ seed: 7, startMs: START, sampledOrdersPerSec: 6, maxItems: 40 });
const lines: string[] = [];
const rest = (method: string, path: string, body?: unknown) => {
  const r = handleRest(sim, method, new URL(`http://board.localhost:8080${path}`), body);
  if (r.status >= 400) throw new Error(`${method} ${path} → ${r.status} ${JSON.stringify(r.body)}`);
  return r.body;
};
const flush = () => {
  for (const m of sim.drain()) lines.push(JSON.stringify(m));
};

// Warm-up (not recorded) so the stream starts in steady state.
for (let i = 0; i < 80; i++) sim.step(250);
sim.drain();

lines.push(JSON.stringify(sim.hello()));
lines.push(JSON.stringify({ type: "state", ts: new Date(sim.now).toISOString(), data: sim.state() }));

const script: Record<number, () => void> = {
  4: () => rest("POST", "/api/alerts/test"),
  6: () => rest("POST", "/api/chaos/arm", { armed: true }),
  12: () => rest("POST", "/api/experiments", { scenario: "black-friday", params: { ordersPerSecond: 1000 }, durationSec: 35, autoRecover: true }),
  62: () => rest("POST", "/api/experiments", { scenario: "broker-crash", params: { broker: 1 }, durationSec: 25, autoRecover: true, confirm: "broker-crash" }),
};
for (let step = 0; step < 120 * 4; step++) {
  const sec = step / 4;
  if (Number.isInteger(sec) && script[sec]) script[sec]!();
  sim.step(250);
  flush();
}
writeFileSync(join(outDir, "ws-stream.jsonl"), lines.join("\n") + "\n");

// ---- REST fixtures ----
const write = (name: string, body: unknown) => writeFileSync(join(outDir, name), JSON.stringify(body, null, 2) + "\n");
const experiments = sim.state().experiments;
write("state.json", rest("GET", "/api/state"));
write("scenarios.json", rest("GET", "/api/scenarios"));
write("timeline.json", rest("GET", "/api/timeline?limit=200"));
write("alerts.json", rest("GET", "/api/alerts"));
write("links.json", rest("GET", "/api/links"));
write("experiment.json", experiments.find((e) => e.report) ?? experiments[0]);
write("chaos-arm.json", rest("POST", "/api/chaos/arm", { armed: true }));
write("simulation.json", rest("POST", "/api/simulation", { action: "start" }));
write("service-state.json", rest("POST", "/api/services/payment/scale", { replicas: 3 }));
write("service-knobs.json", rest("PUT", "/api/services/notification/knobs", { processingLatencyMs: 250, paused: false }));
write("tuning.json", rest("PUT", "/api/tuning/payment", { acks: "leader", lingerMs: 20, maxPollRecords: null }));
const orderId = sim.anyOrderId();
if (!orderId) throw new Error("no recorded order with a complete timeline");
write("order-timeline.json", rest("GET", `/api/orders/${orderId}`));
const drill = rest("POST", "/api/drills", { difficulty: "medium", player: "ada" }) as { id: string; candidates: { id: string }[] };
for (let i = 0; i < 40; i++) sim.step(250);
rest("POST", `/api/drills/${drill.id}/hint`);
write("drill.json", rest("POST", `/api/drills/${drill.id}/hint`));
const answer = (sim as unknown as { drillScenario: string }).drillScenario;
const wrong = drill.candidates.find((c) => c.id !== answer)!.id;
rest("POST", `/api/drills/${drill.id}/guess`, { scenario: wrong });
write("drill-solved.json", rest("POST", `/api/drills/${drill.id}/guess`, { scenario: answer }));
write("leaderboard.json", rest("GET", "/api/drills/leaderboard"));
write("experiment-stopped.json", rest("DELETE", `/api/experiments/${(rest("POST", "/api/experiments", { scenario: "hot-partition", params: {}, durationSec: 60, autoRecover: true }) as { id: string }).id}`));
write("panic.json", rest("POST", "/api/panic"));
const err = handleRest(sim, "POST", new URL("http://x/api/experiments"), { scenario: "broker-outage", params: {}, durationSec: 60, autoRecover: true });
write("error.json", err.body);

console.log(`wrote ${lines.length} WebSocket messages and REST fixtures to ${outDir}`);
