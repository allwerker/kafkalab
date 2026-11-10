/** WebSocket connection with exponential reconnect backoff (1 s → 15 s) and keep-alive pings. */
import type { ServerMessage } from "./types";

export type ConnectionStatus = "connecting" | "open" | "reconnecting" | "closed";

export interface SocketLike {
  onopen: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  send(data: string): void;
  close(): void;
}

export interface Timers {
  setTimeout(fn: () => void, ms: number): unknown;
  clearTimeout(id: unknown): void;
  setInterval(fn: () => void, ms: number): unknown;
  clearInterval(id: unknown): void;
}

export interface LiveSocketOptions {
  url: string;
  onMessage: (msg: ServerMessage) => void;
  onStatus: (status: ConnectionStatus, retryInMs?: number) => void;
  createSocket?: (url: string) => SocketLike;
  timers?: Timers;
  pingIntervalMs?: number;
}

export const MIN_BACKOFF_MS = 1000;
export const MAX_BACKOFF_MS = 15000;

/** Delay before reconnect attempt `attempt` (0-based): 1 s, 2 s, 4 s, 8 s, 15 s, 15 s, ... */
export function backoffDelay(attempt: number): number {
  return Math.min(MAX_BACKOFF_MS, MIN_BACKOFF_MS * 2 ** attempt);
}

/** ws(s)://host/api/ws for the page location. */
export function wsUrl(loc: { protocol: string; host: string }): string {
  return `${loc.protocol === "https:" ? "wss:" : "ws:"}//${loc.host}/api/ws`;
}

const MESSAGE_TYPES = new Set(["hello", "state", "tick", "events", "alert", "experiment", "timeline", "drill"]);

/** Parses a raw frame; returns null for pongs, unknown types and garbage. */
export function parseMessage(raw: unknown): ServerMessage | null {
  if (typeof raw !== "string") return null;
  try {
    const m = JSON.parse(raw) as { type?: unknown; data?: unknown };
    if (typeof m !== "object" || m === null || typeof m.type !== "string" || !MESSAGE_TYPES.has(m.type)) return null;
    return m as ServerMessage;
  } catch {
    return null;
  }
}

const browserTimers: Timers = {
  setTimeout: (fn, ms) => globalThis.setTimeout(fn, ms),
  clearTimeout: (id) => globalThis.clearTimeout(id as number),
  setInterval: (fn, ms) => globalThis.setInterval(fn, ms),
  clearInterval: (id) => globalThis.clearInterval(id as number),
};

export class LiveSocket {
  private ws: SocketLike | null = null;
  private attempt = 0;
  private retryTimer: unknown = null;
  private pingTimer: unknown = null;
  private stopped = true;
  private readonly timers: Timers;
  private readonly create: (url: string) => SocketLike;

  constructor(private readonly opts: LiveSocketOptions) {
    this.timers = opts.timers ?? browserTimers;
    this.create = opts.createSocket ?? ((url) => new WebSocket(url) as unknown as SocketLike);
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.retryTimer !== null) this.timers.clearTimeout(this.retryTimer);
    this.retryTimer = null;
    this.clearPing();
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      ws.onclose = null;
      ws.close();
    }
    this.opts.onStatus("closed");
  }

  private connect(): void {
    this.opts.onStatus(this.attempt === 0 ? "connecting" : "reconnecting");
    let ws: SocketLike;
    try {
      ws = this.create(this.opts.url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.attempt = 0;
      this.opts.onStatus("open");
      this.clearPing();
      this.pingTimer = this.timers.setInterval(() => ws.send(JSON.stringify({ type: "ping" })), this.opts.pingIntervalMs ?? 20000);
    };
    ws.onmessage = (ev) => {
      const msg = parseMessage(ev.data);
      if (msg) this.opts.onMessage(msg);
    };
    ws.onerror = () => {
      /* onclose follows and handles the reconnect */
    };
    ws.onclose = () => {
      this.ws = null;
      this.clearPing();
      this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    const delay = backoffDelay(this.attempt);
    this.attempt++;
    this.opts.onStatus("reconnecting", delay);
    this.retryTimer = this.timers.setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, delay);
  }

  private clearPing(): void {
    if (this.pingTimer !== null) this.timers.clearInterval(this.pingTimer);
    this.pingTimer = null;
  }
}
