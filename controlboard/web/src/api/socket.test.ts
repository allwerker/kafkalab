import { afterEach, describe, expect, it, vi } from "vitest";
import { LiveSocket, backoffDelay, parseMessage, wsUrl, type SocketLike, type Timers } from "./socket";

class FakeSocket implements SocketLike {
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  sent: string[] = [];
  closed = false;
  send(d: string) {
    this.sent.push(d);
  }
  close() {
    this.closed = true;
  }
}

function fakeTimers() {
  const timeouts: { fn: () => void; ms: number; id: number }[] = [];
  const intervals: { fn: () => void; ms: number; id: number }[] = [];
  let id = 0;
  const timers: Timers = {
    setTimeout: (fn, ms) => (timeouts.push({ fn, ms, id: ++id }), id),
    clearTimeout: (x) => timeouts.splice(timeouts.findIndex((t) => t.id === x), 1),
    setInterval: (fn, ms) => (intervals.push({ fn, ms, id: ++id }), id),
    clearInterval: (x) => intervals.splice(intervals.findIndex((t) => t.id === x), 1),
  };
  return { timers, timeouts, intervals };
}

function setup() {
  const sockets: FakeSocket[] = [];
  const t = fakeTimers();
  const statuses: [string, number | undefined][] = [];
  const messages: unknown[] = [];
  const live = new LiveSocket({
    url: "ws://board/api/ws",
    onMessage: (m) => messages.push(m),
    onStatus: (s, r) => statuses.push([s, r]),
    createSocket: () => {
      const s = new FakeSocket();
      sockets.push(s);
      return s;
    },
    timers: t.timers,
    pingIntervalMs: 1000,
  });
  return { live, sockets, statuses, messages, ...t };
}

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("live socket", () => {
  it("shouldDeliverServerMessagesWhenTheConnectionIsOpen", () => {
    // GIVEN: the board connects and the backend accepts
    const { live, sockets, statuses, messages } = setup();
    live.start();
    sockets[0]!.onopen!({});
    // WHEN: the backend sends hello, a pong and garbage
    sockets[0]!.onmessage!({ data: JSON.stringify({ type: "hello", ts: "t", data: { version: 1, serverTime: "t" } }) });
    sockets[0]!.onmessage!({ data: JSON.stringify({ type: "pong" }) });
    sockets[0]!.onmessage!({ data: "{oops" });
    // THEN: only the hello reaches the board and the status is open
    expect(messages).toHaveLength(1);
    expect(statuses).toEqual([
      ["connecting", undefined],
      ["open", undefined],
    ]);
  });

  it("shouldReconnectWithGrowingBackoffWhenTheBackendKeepsDropping", () => {
    // GIVEN: an open connection
    const { live, sockets, statuses, timeouts } = setup();
    live.start();
    sockets[0]!.onopen!({});
    // WHEN: the connection drops three times in a row without ever opening again
    for (let i = 0; i < 3; i++) {
      sockets[i]!.onerror!({});
      sockets[i]!.onclose!({});
      timeouts.shift()!.fn();
    }
    // THEN: the board waits 1 s, 2 s, then 4 s before each new attempt
    expect(statuses.filter(([s, r]) => s === "reconnecting" && r !== undefined).map(([, r]) => r)).toEqual([1000, 2000, 4000]);
    expect(sockets).toHaveLength(4);
  });

  it("shouldResetTheBackoffWhenAReconnectSucceeds", () => {
    // GIVEN: one failed reconnect
    const { live, sockets, statuses, timeouts } = setup();
    live.start();
    sockets[0]!.onclose!({});
    timeouts.shift()!.fn();
    // WHEN: the second socket opens and later drops again
    sockets[1]!.onopen!({});
    sockets[1]!.onclose!({});
    // THEN: the next wait starts again at one second
    expect(statuses.at(-1)).toEqual(["reconnecting", 1000]);
  });

  it("shouldPingTheServerWhileConnectedAndStopWhenClosed", () => {
    // GIVEN: an open connection
    const { live, sockets, intervals } = setup();
    live.start();
    sockets[0]!.onopen!({});
    // WHEN: the keep-alive interval fires
    intervals[0]!.fn();
    // THEN: a ping was sent; after the socket closes the interval is gone
    expect(sockets[0]!.sent).toEqual([JSON.stringify({ type: "ping" })]);
    sockets[0]!.onclose!({});
    expect(intervals).toHaveLength(0);
  });

  it("shouldRetryWhenTheSocketCannotEvenBeCreated", () => {
    // GIVEN: a browser that throws when creating the WebSocket
    const t = fakeTimers();
    const statuses: string[] = [];
    const live = new LiveSocket({
      url: "ws://x",
      onMessage: () => undefined,
      onStatus: (s) => statuses.push(s),
      createSocket: () => {
        throw new Error("blocked");
      },
      timers: t.timers,
    });
    // WHEN: the connection starts
    live.start();
    // THEN: a reconnect is scheduled instead of crashing
    expect(statuses).toEqual(["connecting", "reconnecting"]);
    expect(t.timeouts).toHaveLength(1);
  });

  it("shouldStopReconnectingWhenTheBoardIsClosed", () => {
    // GIVEN: an open connection and a pending reconnect from an earlier drop
    const { live, sockets, statuses, timeouts } = setup();
    live.start();
    live.start();
    sockets[0]!.onopen!({});
    // WHEN: the board is closed
    live.stop();
    // THEN: the socket is closed, no reconnect is scheduled and the status is closed
    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.closed).toBe(true);
    expect(timeouts).toHaveLength(0);
    expect(statuses.at(-1)).toEqual(["closed", undefined]);
  });

  it("shouldCancelAPendingReconnectWhenStoppedWhileWaiting", () => {
    // GIVEN: the connection dropped and a retry is pending
    const { live, sockets, timeouts } = setup();
    live.start();
    sockets[0]!.onclose!({});
    expect(timeouts).toHaveLength(1);
    // WHEN: the board is closed during the wait
    live.stop();
    // THEN: the retry is cancelled
    expect(timeouts).toHaveLength(0);
  });

  it("shouldNotReconnectWhenADropArrivesAfterStopping", () => {
    // GIVEN: a stopped connection whose old socket still fires close
    const { live, sockets, timeouts } = setup();
    live.start();
    const s = sockets[0]!;
    const onclose = s.onclose!;
    live.stop();
    // WHEN: the late close event arrives
    onclose({});
    // THEN: nothing is scheduled
    expect(timeouts).toHaveLength(0);
  });

  it("shouldUseBrowserWebSocketAndTimersWhenNothingIsInjected", () => {
    // GIVEN: a browser WebSocket stub and fake clock
    vi.useFakeTimers();
    const created: string[] = [];
    vi.stubGlobal(
      "WebSocket",
      class extends FakeSocket {
        constructor(url: string) {
          super();
          created.push(url);
          queueMicrotask(() => this.onclose?.({}));
        }
      },
    );
    const live = new LiveSocket({ url: "ws://h/api/ws", onMessage: () => undefined, onStatus: () => undefined });
    // WHEN: the board connects and the first attempt fails
    live.start();
    return Promise.resolve().then(() => {
      vi.advanceTimersByTime(1000);
      // THEN: a second WebSocket is created after one second
      expect(created).toEqual(["ws://h/api/ws", "ws://h/api/ws"]);
      live.stop();
    });
  });

  it("shouldStartAndStopKeepAliveWithBrowserTimersWhenNothingIsInjected", () => {
    // GIVEN: a browser WebSocket that opens immediately
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    vi.stubGlobal(
      "WebSocket",
      class extends FakeSocket {
        constructor() {
          super();
          sockets.push(this);
        }
      },
    );
    const live = new LiveSocket({ url: "ws://h", onMessage: () => undefined, onStatus: () => undefined, pingIntervalMs: 500 });
    live.start();
    sockets[0]!.onopen!({});
    // WHEN: half a second passes and the board is closed
    vi.advanceTimersByTime(500);
    live.stop();
    vi.advanceTimersByTime(2000);
    // THEN: exactly one ping went out
    expect(sockets[0]!.sent).toHaveLength(1);
  });
});

describe("socket shutdown with browser timers", () => {
  it("shouldCancelTheBrowserRetryTimerWhenStoppedDuringBackoff", () => {
    // GIVEN: a browser WebSocket that is refused immediately, so a retry is pending
    vi.useFakeTimers();
    const sockets: FakeSocket[] = [];
    vi.stubGlobal(
      "WebSocket",
      class extends FakeSocket {
        constructor() {
          super();
          sockets.push(this);
        }
      },
    );
    const live = new LiveSocket({ url: "ws://h", onMessage: () => undefined, onStatus: () => undefined });
    live.start();
    sockets[0]!.onclose!({});
    // WHEN: the board is closed while waiting and time passes
    live.stop();
    vi.advanceTimersByTime(30000);
    // THEN: no new connection attempt is made
    expect(sockets).toHaveLength(1);
  });
});

describe("socket helpers", () => {
  it("shouldCapTheBackoffAtFifteenSecondsWhenRetriesPileUp", () => {
    // GIVEN: many failed attempts
    // WHEN: the delays are computed
    // THEN: they double from one second and never exceed fifteen
    expect([0, 1, 2, 3, 4, 10].map(backoffDelay)).toEqual([1000, 2000, 4000, 8000, 15000, 15000]);
  });

  it("shouldUseSecureWebSocketsWhenThePageIsServedOverHttps", () => {
    // GIVEN: the board served over https and over http
    // WHEN: the WebSocket URL is derived
    // THEN: wss and ws are used respectively
    expect(wsUrl({ protocol: "https:", host: "board.example" })).toBe("wss://board.example/api/ws");
    expect(wsUrl({ protocol: "http:", host: "board.localhost:8080" })).toBe("ws://board.localhost:8080/api/ws");
  });

  it("shouldRejectFramesThatAreNotContractMessagesWhenParsing", () => {
    // GIVEN: binary data, JSON null, a message without type and an unknown type
    // WHEN: they are parsed
    // THEN: none of them is accepted, while a tick is
    expect(parseMessage(new ArrayBuffer(2))).toBeNull();
    expect(parseMessage("null")).toBeNull();
    expect(parseMessage(JSON.stringify({ data: 1 }))).toBeNull();
    expect(parseMessage(JSON.stringify({ type: "bogus" }))).toBeNull();
    expect(parseMessage(JSON.stringify({ type: "tick", ts: "t", data: {} }))?.type).toBe("tick");
  });
});
