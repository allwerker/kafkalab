/** Board store (zustand): server state from the WebSocket plus REST-driven updates. */
import { createStore, type StoreApi } from "zustand/vanilla";
import { useStore } from "zustand";
import type { Drill, Experiment, ScenarioDef, ServerMessage, ServiceState, TimelineEntry } from "../api/types";
import type { ConnectionStatus } from "../api/socket";
import { initialBoard, mergeTimeline, reduce, upsertExperiment, upsertService, type BoardData } from "./reducer";

export interface BoardActions {
  receive(msg: ServerMessage): void;
  setConnection(status: ConnectionStatus, retryInMs?: number): void;
  setScenarios(scenarios: ScenarioDef[]): void;
  addTimeline(entries: TimelineEntry[]): void;
  markAlertsRead(): void;
  putExperiment(e: Experiment): void;
  putService(s: ServiceState): void;
  setChaosArmed(armed: boolean): void;
  setDrill(d: Drill): void;
}

export type Board = BoardData & BoardActions;

export function createBoardStore(now: () => number = Date.now): StoreApi<Board> {
  return createStore<Board>()((set) => ({
    ...initialBoard(),
    receive: (msg) => set((s) => reduce(s, msg, now())),
    setConnection: (connection, retryInMs) => set({ connection, retryInMs }),
    setScenarios: (scenarios) => set({ scenarios }),
    addTimeline: (entries) => set((s) => ({ timeline: mergeTimeline(s.timeline, entries) })),
    markAlertsRead: () => set({ unreadAlerts: [] }),
    putExperiment: (e) => set((s) => ({ experiments: upsertExperiment(s.experiments, e) })),
    putService: (svc) => set((s) => ({ services: upsertService(s.services, svc) })),
    setChaosArmed: (chaosArmed) => set({ chaosArmed }),
    setDrill: (drill) => set({ drill }),
  }));
}

export const boardStore = createBoardStore();

export function useBoard<T>(selector: (b: Board) => T, store: StoreApi<Board> = boardStore): T {
  return useStore(store, selector);
}
