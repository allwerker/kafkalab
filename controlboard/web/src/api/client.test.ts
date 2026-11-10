import { describe, expect, it, vi } from "vitest";
import { ApiError, api, createApi } from "./client";

function respond(status: number, body?: unknown, statusText = "") {
  return vi.fn(async (_url: string, _init?: RequestInit) =>
    new Response(body === undefined ? null : typeof body === "string" ? body : JSON.stringify(body), { status, statusText }),
  );
}

describe("REST client", () => {
  it("shouldPostTheExperimentRequestWhenTheOperatorStartsAScenario", async () => {
    // GIVEN: a backend that accepts the Black Friday experiment
    const fetch = respond(200, { id: "exp-1", status: "running" });
    const client = createApi(fetch, "http://board");
    // WHEN: the operator starts Black Friday for three minutes
    const e = await client.startExperiment({ scenario: "black-friday", params: { ordersPerSecond: 1000 }, durationSec: 180, autoRecover: true });
    // THEN: a JSON POST goes to /api/experiments and the created experiment comes back
    const [url, init] = fetch.mock.calls[0]!;
    expect(url).toBe("http://board/api/experiments");
    expect(init?.method).toBe("POST");
    expect(JSON.parse(String(init?.body))).toEqual({ scenario: "black-friday", params: { ordersPerSecond: 1000 }, durationSec: 180, autoRecover: true });
    expect((init?.headers as Record<string, string>)["Content-Type"]).toBe("application/json");
    expect(e).toEqual({ id: "exp-1", status: "running" });
  });

  it("shouldRaiseTheContractErrorCodeWhenChaosIsDisarmed", async () => {
    // GIVEN: the backend refuses a destructive scenario because chaos is not armed
    const client = createApi(respond(409, { error: "chaos_disarmed", message: "Arm chaos first." }));
    // WHEN: the broker crash is started
    const err = await client.startExperiment({ scenario: "broker-crash", params: {}, durationSec: 60, autoRecover: true }).catch((e: unknown) => e);
    // THEN: the error carries status, code and the human message
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 409, code: "chaos_disarmed", message: "Arm chaos first." });
  });

  it("shouldFallBackToHttpStatusWhenTheErrorBodyIsNotJson", async () => {
    // GIVEN: a proxy answering 502 with an HTML page
    const client = createApi(respond(502, "<html>bad gateway</html>", "Bad Gateway"));
    // WHEN: the state is requested
    const err = await client.state().catch((e: unknown) => e);
    // THEN: the error falls back to the HTTP status
    expect(err).toMatchObject({ status: 502, code: "http_502", message: "Bad Gateway" });
  });

  it("shouldKeepDefaultsWhenTheErrorBodyLacksFields", async () => {
    // GIVEN: a 500 with an empty JSON object and no status text
    const client = createApi(respond(500, {}));
    // WHEN: the timeline is requested
    const err = await client.timeline().catch((e: unknown) => e);
    // THEN: generic code and message are used
    expect(err).toMatchObject({ code: "http_500", message: "HTTP 500" });
  });

  it("shouldReportTheBackendAsUnreachableWhenTheNetworkFails", async () => {
    // GIVEN: the backend is not running
    const client = createApi(vi.fn(async () => Promise.reject(new Error("ECONNREFUSED"))));
    // WHEN: PANIC is pressed
    const err = await client.panic().catch((e: unknown) => e);
    // THEN: a network error with status 0 is raised
    expect(err).toMatchObject({ status: 0, code: "network_error" });
    expect((err as Error).message).toContain("ECONNREFUSED");
  });

  it("shouldResolveWithoutBodyWhenTheBackendAnswersNoContent", async () => {
    // GIVEN: the test-alert endpoint answers 204
    const client = createApi(respond(204));
    // WHEN: a test alert is sent
    // THEN: the call resolves to undefined
    await expect(client.testAlert()).resolves.toBeUndefined();
  });

  it("shouldCallEveryContractEndpointWhenTheBoardUsesIt", async () => {
    // GIVEN: a backend that answers every call with an empty object
    const fetch = respond(200, {});
    const c = createApi(fetch);
    // WHEN: every client method is used once (ids with special characters included)
    await c.state();
    await c.scenarios();
    await c.stopExperiment("exp 1");
    await c.armChaos(true);
    await c.panic();
    await c.simulation("reset");
    await c.putKnobs("payment", { processingLatencyMs: 200 });
    await c.resetKnobs("payment");
    await c.scale("payment", 3);
    await c.timeline(50);
    await c.alerts();
    await c.links();
    await c.tuning();
    await c.putTuning("payment", { acks: null });
    await c.startDrill("hard", "ada");
    await c.hint("d1");
    await c.guess("d1", "black-friday");
    await c.reveal("d1");
    await c.leaderboard();
    await c.order("o/1");
    // THEN: each hits its documented method and path
    const calls = fetch.mock.calls.map(([u, i]) => `${i?.method} ${u}`);
    expect(calls).toEqual([
      "GET /api/state",
      "GET /api/scenarios",
      "DELETE /api/experiments/exp%201",
      "POST /api/chaos/arm",
      "POST /api/panic",
      "POST /api/simulation",
      "PUT /api/services/payment/knobs",
      "DELETE /api/services/payment/knobs",
      "POST /api/services/payment/scale",
      "GET /api/timeline?limit=50",
      "GET /api/alerts",
      "GET /api/links",
      "GET /api/tuning",
      "PUT /api/tuning/payment",
      "POST /api/drills",
      "POST /api/drills/d1/hint",
      "POST /api/drills/d1/guess",
      "POST /api/drills/d1/reveal",
      "GET /api/drills/leaderboard",
      "GET /api/orders/o%2F1",
    ]);
  });

  it("shouldUseTheGlobalFetchWhenNoFetchIsInjected", async () => {
    // GIVEN: the app-wide client and a stubbed browser fetch
    const fetch = respond(200, []);
    vi.stubGlobal("fetch", fetch);
    // WHEN: alerts are loaded
    await api.alerts();
    // THEN: the browser fetch was used on the same origin
    expect(fetch.mock.calls[0]![0]).toBe("/api/alerts");
    vi.unstubAllGlobals();
  });
});
