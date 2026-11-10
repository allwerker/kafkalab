/**
 * Mock controlboard backend: REST + WebSocket per docs/contracts/controlboard-api.md, driven by a
 * deterministic seeded simulation. `bun run mock` → http://127.0.0.1:8090
 *
 * Env: MOCK_PORT (8090), MOCK_SEED (42), MOCK_STATIC (serve a built dist/ directory too).
 */
import type { ServerWebSocket } from "bun";
import { handleRest } from "./api.ts";
import { Sim } from "./sim.ts";

const port = Number(process.env.MOCK_PORT ?? 8090);
const sim = new Sim({ seed: Number(process.env.MOCK_SEED ?? 42) });
const clients = new Set<ServerWebSocket<unknown>>();
const staticDir = process.env.MOCK_STATIC;

// Warm up so the first screen already shows traffic.
for (let i = 0; i < 40; i++) sim.step(250);
sim.drain();

setInterval(() => {
  sim.step(250);
  const msgs = sim.drain();
  if (!clients.size) return;
  for (const m of msgs) {
    const s = JSON.stringify(m);
    for (const c of clients) c.send(s);
  }
}, 250);

const server = Bun.serve({
  port,
  hostname: "127.0.0.1",
  async fetch(req, srv) {
    const url = new URL(req.url);
    if (url.pathname === "/api/ws") {
      if (srv.upgrade(req)) return undefined;
      return new Response("upgrade failed", { status: 400 });
    }
    if (url.pathname.startsWith("/api/") || url.pathname === "/healthz") {
      let body: unknown;
      if (req.method !== "GET" && req.method !== "DELETE") {
        const text = await req.text();
        try {
          body = text ? JSON.parse(text) : undefined;
        } catch {
          return Response.json({ error: "invalid_json", message: "request body is not JSON" }, { status: 400 });
        }
      }
      const r = handleRest(sim, req.method, url, body);
      // Push resulting messages (timeline, experiment, ...) immediately.
      for (const m of sim.drain()) {
        const s = JSON.stringify(m);
        for (const c of clients) c.send(s);
      }
      return r.status === 204 ? new Response(null, { status: 204 }) : Response.json(r.body, { status: r.status });
    }
    if (staticDir) {
      const file = Bun.file(`${staticDir}${url.pathname === "/" ? "/index.html" : url.pathname}`);
      if (await file.exists()) return new Response(file);
      return new Response(Bun.file(`${staticDir}/index.html`));
    }
    return new Response("Kafka Lab mock backend — see /api/state", { status: 404 });
  },
  websocket: {
    open(ws) {
      clients.add(ws);
      ws.send(JSON.stringify(sim.hello()));
      ws.send(JSON.stringify({ type: "state", ts: new Date(sim.now).toISOString(), data: sim.state() }));
    },
    message(ws, raw) {
      try {
        const m = JSON.parse(String(raw)) as { type?: string };
        if (m.type === "ping") ws.send(JSON.stringify({ type: "pong" }));
      } catch {
        /* ignore malformed client messages */
      }
    },
    close(ws) {
      clients.delete(ws);
    },
  },
});

console.log(`Kafka Lab mock backend on http://${server.hostname}:${server.port}`);
