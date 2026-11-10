/** Wires the WebSocket and the initial REST loads into the store and the live-view EventBus. */
import type { StoreApi } from "zustand/vanilla";
import type { Api } from "../api/client";
import { LiveSocket, type LiveSocketOptions } from "../api/socket";
import type { ServerMessage } from "../api/types";
import type { Emitter } from "../lib/emitter";
import type { LiveBusEvents } from "../live/EventBus";
import type { Board } from "./store";

export interface ConnectOptions {
  store: StoreApi<Board>;
  api: Pick<Api, "scenarios" | "timeline">;
  bus: Emitter<LiveBusEvents>;
  url: string;
  socket?: Partial<LiveSocketOptions>;
}

/** Starts the live connection; returns a disposer. */
export function connectBoard({ store, api, bus, url, socket }: ConnectOptions): () => void {
  const loadStatic = () => {
    api.scenarios().then(
      (s) => store.getState().setScenarios(s),
      () => undefined,
    );
    api.timeline(200).then(
      (t) => store.getState().addTimeline(t),
      () => undefined,
    );
  };
  const onMessage = (msg: ServerMessage) => {
    if (msg.type === "events") {
      bus.emit("events", msg.data);
      if (msg.data.scale !== store.getState().eventScale) store.getState().receive(msg);
      return;
    }
    if (msg.type === "tick") bus.emit("tick", msg.data);
    store.getState().receive(msg);
    // After a reconnect the state message resyncs everything; refetch the static parts too.
    if (msg.type === "hello" && store.getState().scenarios.length === 0) loadStatic();
  };
  const live = new LiveSocket({
    url,
    onMessage,
    onStatus: (status, retry) => store.getState().setConnection(status, retry),
    ...socket,
  });
  loadStatic();
  live.start();
  return () => live.stop();
}
