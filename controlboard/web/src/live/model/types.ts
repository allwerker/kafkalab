import type { LiveEventKind } from "../../api/types";
import type { ServiceName } from "../../lib/topology";
import type { Lane } from "./layout";

export type StationStatus = "ok" | "degraded" | "down" | "paused" | "unknown";

export interface StationView {
  id: ServiceName;
  label: string;
  x: number;
  y: number;
  status: StationStatus;
  replicas: number;
  ready: number;
  /** total consumer lag of the station's group (0 for the generator) */
  lag: number;
  /** records/s the station produces */
  rate: number;
  /** 0..1 activity flash, decays every frame */
  pulse: number;
  /** a running experiment targets this station */
  affected: boolean;
  applying: boolean;
}

export interface LaneView extends Lane {
  lag: number;
  /** parcels queued in front of the consumer (lag / pileUnit, capped) */
  pile: number;
  /** pile exceeded the cap */
  overflow: boolean;
  /** 0..1 recent traffic glow */
  activity: number;
}

export type ParcelState = "moving" | "absorbing" | "falling";

export interface Parcel {
  id: number;
  orderId: string;
  kind: LiveEventKind;
  topic: string;
  partition: number;
  /** lane the parcel rides on; "" for parcels that fall into the DLQ pit */
  laneId: string;
  t: number;
  speed: number;
  x: number;
  y: number;
  fromX: number;
  fromY: number;
  toX: number;
  toY: number;
  color: number;
  alpha: number;
  size: number;
  state: ParcelState;
  age: number;
  fade: boolean;
  dead: boolean;
}

export type EffectKind = "coin" | "burst" | "cross" | "wave" | "coinBack" | "release" | "splash" | "spark";

export interface Effect {
  id: number;
  kind: EffectKind;
  x: number;
  y: number;
  tx: number;
  ty: number;
  age: number;
  ttl: number;
  color: number;
  seed: number;
}

export interface Truck {
  id: number;
  x: number;
  y: number;
  vx: number;
  load: number;
  loadingMs: number;
  carrier: string;
  state: "loading" | "departing";
  dead: boolean;
}

export interface BrokerView {
  id: number;
  x: number;
  y: number;
  up: boolean;
  controller: boolean;
  leaders: number;
  underReplicated: number;
  /** ms since the broker went down (drives sparks), -1 when up */
  downForMs: number;
}

export type WeatherKind = "rush" | "glitch" | "lightning" | "smoke" | "matrix" | "fog";

export interface Weather {
  kind: WeatherKind;
  /** station ids or `broker-N` targets; empty = whole scene */
  targets: string[];
  intensity: number;
  experimentId: string;
}

export interface AnalyticsScreen {
  history: number[];
  ordersPerSec: number;
  revenuePerMin: number;
}

export type Hit =
  | { type: "parcel"; parcel: Parcel }
  | { type: "station"; station: StationView }
  | { type: "broker"; broker: BrokerView }
  | { type: "dlq" };

export interface Tooltip {
  title: string;
  lines: string[];
}
