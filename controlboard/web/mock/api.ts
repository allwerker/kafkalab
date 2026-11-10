/** REST router of the mock backend (shared by the Bun server and the fixture recorder). */
import { ApiError, type Sim } from "./sim.ts";

export interface RestResult {
  status: number;
  body?: unknown;
}

export function handleRest(sim: Sim, method: string, url: URL, body: unknown): RestResult {
  const path = url.pathname;
  const b = (body ?? {}) as Record<string, unknown>;
  const m = (re: RegExp) => path.match(re);
  try {
    if (method === "GET" && path === "/healthz") return { status: 200, body: { ok: true } };
    if (method === "GET" && path === "/api/state") return { status: 200, body: sim.state() };
    if (method === "GET" && path === "/api/scenarios") return { status: 200, body: sim.scenarios() };
    if (method === "POST" && path === "/api/experiments") return { status: 200, body: sim.startExperiment(b) };
    let r = m(/^\/api\/experiments\/([^/]+)$/);
    if (method === "DELETE" && r) return { status: 200, body: sim.stopExperiment(decodeURIComponent(r[1]!)) };
    if (method === "POST" && path === "/api/chaos/arm") return { status: 200, body: sim.arm(b.armed === true) };
    if (method === "POST" && path === "/api/panic") return { status: 200, body: sim.panic() };
    if (method === "POST" && path === "/api/simulation") return { status: 200, body: sim.setSimulation(String(b.action)) };
    r = m(/^\/api\/services\/([^/]+)\/knobs$/);
    if (r && method === "PUT") return { status: 200, body: sim.putKnobs(r[1]!, b) };
    if (r && method === "DELETE") return { status: 200, body: sim.resetKnobs(r[1]!) };
    r = m(/^\/api\/services\/([^/]+)\/scale$/);
    if (r && method === "POST") return { status: 200, body: sim.scale(r[1]!, b.replicas) };
    if (method === "GET" && path === "/api/timeline") return { status: 200, body: sim.timelineList(Number(url.searchParams.get("limit") ?? "200")) };
    if (method === "GET" && path === "/api/alerts") return { status: 200, body: sim.alertList() };
    if (method === "POST" && path === "/api/alerts/test") {
      sim.testAlert();
      return { status: 204 };
    }
    if (method === "POST" && path === "/api/alerts/webhook") return { status: 204 };
    if (method === "GET" && path === "/api/links") return { status: 200, body: sim.state().links };
    if (method === "GET" && path === "/api/tuning") return { status: 200, body: sim.tuningResponse() };
    r = m(/^\/api\/tuning\/([^/]+)$/);
    if (r && method === "PUT") return { status: 200, body: sim.putTuning(r[1]!, b) };
    if (method === "POST" && path === "/api/drills") return { status: 200, body: sim.startDrill(b) };
    if (method === "GET" && path === "/api/drills/leaderboard") return { status: 200, body: sim.leaderboard };
    r = m(/^\/api\/drills\/([^/]+)\/(hint|guess|reveal)$/);
    if (r && method === "POST") {
      const [, id, action] = r as unknown as [string, string, string];
      if (action === "hint") return { status: 200, body: sim.hint(id) };
      if (action === "guess") return { status: 200, body: sim.guess(id, b.scenario) };
      return { status: 200, body: sim.reveal(id) };
    }
    r = m(/^\/api\/orders\/([^/]+)$/);
    if (r && method === "GET") return { status: 200, body: sim.order(decodeURIComponent(r[1]!)) };
    return { status: 404, body: { error: "not_found", message: `no route for ${method} ${path}` } };
  } catch (e) {
    if (e instanceof ApiError) return { status: e.status, body: { error: e.code, message: e.message } };
    return { status: 500, body: { error: "internal", message: String(e) } };
  }
}
