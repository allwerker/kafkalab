import { describe, expect, it, vi } from "vitest";
import type { SocketLike, Timers } from "../api/socket";
import type { ScenarioDef, ServerMessage } from "../api/types";
import { Emitter } from "../lib/emitter";
import type { LiveBusEvents } from "../live/EventBus";
import { tick } from "../test/live";
import { connectBoard } from "./connection";
import { createBoardStore } from "./store";

class FakeSocket implements SocketLike {
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  send() {}
  close() {}
}

const noTimers: Timers = { setTimeout: () => 0, clearTimeout: () => undefined, setInterval: () => 0, clearInterval: () => undefined };
const flush = () => new Promise((r) => setTimeout(r, 0));

function setup(api = { scenarios: vi.fn(async () => [{ id: "black-friday" } as ScenarioDef]), timeline: vi.fn(async () => [{ id: "t1", at: "2026-01-15T10:00:00Z", kind: "system" as const, severity: "info" as const, title: "boot" }]) }) {
  const store = createBoardStore();
  const bus = new Emitter<LiveBusEvents>();
  const sockets: FakeSocket[] = [];
  const stop = connectBoard({
    store,
    api,
    bus,
    url: "ws://board/api/ws",
    socket: {
      createSocket: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
      timers: noTimers,
    },
  });
  const send = (m: ServerMessage) => sockets[0]!.onmessage!({ data: JSON.stringify(m) });
  return { store, bus, sockets, stop, send, api };
}

describe("board connection", () => {
  it("shouldLoadScenariosAndTimelineWhenTheBoardStarts", async () => {
    // GIVEN: a backend with a scenario library and a timeline
    const { store, sockets } = setup();
    // WHEN: the board starts and the REST calls return
    await flush();
    // THEN: the store holds both and the WebSocket is connecting
    expect(store.getState().scenarios).toHaveLength(1);
    expect(store.getState().timeline).toHaveLength(1);
    expect(store.getState().connection).toBe("connecting");
    expect(sockets).toHaveLength(1);
  });

  it("shouldForwardTicksAndEventsToTheLiveViewWhenTheyArrive", () => {
    // GIVEN: a connected board with the live view listening on the bus
    const { store, bus, sockets, send } = setup();
    const ticks: unknown[] = [];
    const events: unknown[] = [];
    bus.on("tick", (t) => ticks.push(t));
    bus.on("events", (e) => events.push(e));
    sockets[0]!.onopen!({});
    // WHEN: a tick and two event batches (same scale, then a new scale) arrive
    send({ type: "tick", ts: "", data: tick() });
    send({ type: "events", ts: "", data: { scale: 1, items: [] } });
    send({ type: "events", ts: "", data: { scale: 30, items: [] } });
    // THEN: the live view gets all of them and the store only tracks KPIs and the new scale
    expect(ticks).toHaveLength(1);
    expect(events).toHaveLength(2);
    expect(store.getState().kpis?.ordersPerSec).toBe(50);
    expect(store.getState().eventScale).toBe(30);
    expect(store.getState().connection).toBe("open");
  });

  it("shouldRefetchStaticDataWhenTheFirstLoadFailedAndTheServerSaysHello", async () => {
    // GIVEN: the backend was down when the board loaded, so REST calls failed
    const api = { scenarios: vi.fn(async () => Promise.reject(new Error("down"))), timeline: vi.fn(async () => Promise.reject(new Error("down"))) };
    const { send } = setup(api);
    await flush();
    // WHEN: the WebSocket finally connects and the server says hello
    send({ type: "hello", ts: "", data: { version: 1, serverTime: new Date().toISOString() } });
    await flush();
    // THEN: scenarios and timeline are requested again
    expect(api.scenarios).toHaveBeenCalledTimes(2);
    expect(api.timeline).toHaveBeenCalledTimes(2);
  });

  it("shouldNotRefetchWhenScenariosAreAlreadyLoaded", async () => {
    // GIVEN: a board that already has its scenario library
    const { send, api } = setup();
    await flush();
    // WHEN: the server says hello after a reconnect
    send({ type: "hello", ts: "", data: { version: 1, serverTime: new Date().toISOString() } });
    // THEN: no extra REST round-trip happens
    expect(api.scenarios).toHaveBeenCalledTimes(1);
  });

  it("shouldCloseTheSocketWhenTheBoardIsDisposed", () => {
    // GIVEN: a running connection
    const { store, stop } = setup();
    // WHEN: the board is disposed
    stop();
    // THEN: the connection reports closed
    expect(store.getState().connection).toBe("closed");
  });
});
