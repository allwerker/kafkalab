import { useState } from "react";
import { Bell, FlaskConical, HeartPulse, Play, ShieldAlert, SlidersHorizontal, Target, type LucideIcon } from "lucide-react";
import type { TimelineEntry } from "../api/types";
import { TIMELINE_KINDS, filterTimeline } from "../lib/domain";
import { fmtClock } from "../lib/format";
import { useBoard } from "../store/store";
import { Empty } from "./ui";

export const KIND_ICON: Record<TimelineEntry["kind"], LucideIcon> = {
  experiment: FlaskConical,
  config: SlidersHorizontal,
  alert: Bell,
  recovery: HeartPulse,
  simulation: Play,
  drill: Target,
  system: ShieldAlert,
};

export function Timeline() {
  const timeline = useBoard((b) => b.timeline);
  const [kinds, setKinds] = useState(() => new Set<string>(TIMELINE_KINDS));
  const toggle = (k: string) => {
    const next = new Set(kinds);
    if (next.has(k)) next.delete(k);
    else next.add(k);
    setKinds(next);
  };
  const entries = filterTimeline(timeline, kinds);
  return (
    <div className="timeline">
      <div className="filters" role="group" aria-label="Filter timeline by kind">
        {TIMELINE_KINDS.map((k) => {
          const Icon = KIND_ICON[k];
          return (
            <button key={k} className={`chip-btn ${kinds.has(k) ? "is-on" : ""}`} aria-pressed={kinds.has(k)} onClick={() => toggle(k)}>
              <Icon size={12} /> {k}
            </button>
          );
        })}
      </div>
      {entries.length === 0 && <Empty>Nothing in the timeline for this filter.</Empty>}
      <ol className="tl">
        {entries.map((t) => {
          const Icon = KIND_ICON[t.kind];
          return (
            <li key={t.id} className={`tl__item tl__item--${t.severity}`} data-testid="timeline-entry">
              <span className="tl__icon">
                <Icon size={13} />
              </span>
              <time className="mono" dateTime={t.at}>
                {fmtClock(t.at)}
              </time>
              <div>
                <div className="tl__title">{t.title}</div>
                {t.detail && <div className="tl__detail">{t.detail}</div>}
              </div>
            </li>
          );
        })}
      </ol>
    </div>
  );
}
