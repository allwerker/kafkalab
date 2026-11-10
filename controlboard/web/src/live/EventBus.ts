/**
 * React ↔ Phaser bridge, modelled after the official phaserjs/template-react-ts EventBus.
 * React → scene: live data (tick, events, experiments). Scene → React: hover / click / ready.
 */
import type { Category, EventsBatch, Experiment, Tick } from "../api/types";
import { Emitter } from "../lib/emitter";
import type { ServiceName } from "../lib/topology";

export interface HoverInfo {
  title: string;
  lines: string[];
  /** position relative to the canvas element, CSS pixels */
  x: number;
  y: number;
}

export interface LiveBusEvents extends Record<string, unknown> {
  tick: Tick;
  events: EventsBatch;
  experiments: { experiments: Experiment[]; categories: Record<string, Category> };
  "scene-ready": { fps: number };
  hover: HoverInfo | null;
  "select-order": { orderId: string };
  "select-station": { id: ServiceName };
  "select-broker": { id: number };
  fps: number;
}

export const EventBus = new Emitter<LiveBusEvents>();
