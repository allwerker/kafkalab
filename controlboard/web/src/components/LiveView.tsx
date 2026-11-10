import { useEffect, useRef, useState } from "react";
import { Radio } from "lucide-react";
import { categoryMap } from "../lib/domain";
import { EventBus, type HoverInfo } from "../live/EventBus";
import { KIND_COLOR, KIND_LABEL } from "../live/model/LiveModel";
import { useBoard } from "../store/store";
import { OrderDrawer } from "./OrderDrawer";
import { SideCard, type Selection } from "./SideCard";

export interface LiveGame {
  destroy(): void;
}

export type StartLiveGame = (parent: HTMLElement, bus: typeof EventBus) => LiveGame;

/** Loads the Phaser render layer lazily so it stays out of the main chunk (and out of unit tests). */
export const loadRenderer = (): Promise<StartLiveGame> => import("../live/render/game").then((m) => m.startLiveGame);

const LEGEND = (["created", "paid", "payment-failed", "reserved", "rejected", "shipped", "cancelled", "refunded", "notified", "dlq"] as const).map((k) => ({
  kind: k,
  label: KIND_LABEL[k],
  color: `#${KIND_COLOR[k].toString(16).padStart(6, "0")}`,
}));

export function LiveView({ load = loadRenderer }: { load?: () => Promise<StartLiveGame> }) {
  const host = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<HoverInfo | null>(null);
  const [orderId, setOrderId] = useState<string | null>(null);
  const [selection, setSelection] = useState<Selection | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  const [fps, setFps] = useState<number | null>(null);
  const eventScale = useBoard((b) => b.eventScale);
  const experiments = useBoard((b) => b.experiments);
  const scenarios = useBoard((b) => b.scenarios);
  const hasState = useBoard((b) => b.hasState);

  useEffect(() => {
    let game: LiveGame | null = null;
    let cancelled = false;
    load().then(
      (start) => {
        if (!cancelled && host.current) game = start(host.current, EventBus);
      },
      (e: unknown) => setFailed(String((e as Error).message ?? e)),
    );
    const offs = [
      EventBus.on("hover", setHover),
      EventBus.on("select-order", ({ orderId }) => setOrderId(orderId)),
      EventBus.on("select-station", ({ id }) => setSelection({ type: "station", id })),
      EventBus.on("select-broker", ({ id }) => setSelection({ type: "broker", id })),
      EventBus.on("fps", setFps),
    ];
    return () => {
      cancelled = true;
      offs.forEach((off) => off());
      game?.destroy();
    };
  }, [load]);

  useEffect(() => {
    EventBus.emit("experiments", { experiments, categories: categoryMap(scenarios) });
  }, [experiments, scenarios]);

  return (
    <section className="live" aria-label="Live view">
      <div className="live__canvas" ref={host} data-testid="live-canvas-host" />
      <div className="live__hud">
        <span className="live__title">
          <Radio size={14} className={hasState ? "pulse" : ""} /> Live flow
        </span>
        <span className="live__scale mono" data-testid="scale-label">
          1 parcel = {eventScale.toLocaleString("en")} {eventScale === 1 ? "order" : "orders"}
        </span>
        {fps !== null && <span className="live__fps mono">{fps} fps</span>}
      </div>
      <ul className="live__legend" aria-label="Legend">
        {LEGEND.map((l) => (
          <li key={l.kind}>
            <i style={{ background: l.color, boxShadow: `0 0 6px ${l.color}` }} />
            {l.label}
          </li>
        ))}
      </ul>
      {failed && <div className="live__error">Live view unavailable: {failed}</div>}
      {hover && (
        <div className="live__tooltip" style={{ left: hover.x + 14, top: hover.y + 14 }} role="tooltip">
          <b>{hover.title}</b>
          {hover.lines.map((l) => (
            <span key={l}>{l}</span>
          ))}
        </div>
      )}
      {orderId && <OrderDrawer orderId={orderId} onClose={() => setOrderId(null)} />}
      {selection && !orderId && <SideCard selection={selection} onClose={() => setSelection(null)} />}
    </section>
  );
}
