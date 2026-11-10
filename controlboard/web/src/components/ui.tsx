import { useEffect, useRef, type ReactNode } from "react";
import {
  Activity,
  BellRing,
  Database,
  Gauge,
  Layers,
  Link as LinkIcon,
  Radar,
  ScrollText,
  Siren,
  Waypoints,
  X,
  type LucideIcon,
} from "lucide-react";
import type { Health, Severity } from "../api/types";

const ICONS: Record<string, LucideIcon> = {
  gauge: Gauge,
  layers: Layers,
  database: Database,
  radar: Radar,
  "bell-ring": BellRing,
  siren: Siren,
  "scroll-text": ScrollText,
  waypoints: Waypoints,
  activity: Activity,
};

/** Lucide icon by kebab-case name (from Link.icon), falling back to a link icon. */
export function NamedIcon({ name, size = 18 }: { name: string; size?: number }) {
  const Icon = ICONS[name] ?? LinkIcon;
  return <Icon size={size} aria-hidden="true" />;
}

export function HealthDot({ health, label }: { health: Health; label?: string }) {
  return <span className={`dot dot--${health}`} role="img" aria-label={label ?? `health: ${health}`} title={health} />;
}

export function SeverityBadge({ severity }: { severity: Severity }) {
  const bars = { low: 1, medium: 2, high: 3, critical: 4 }[severity];
  return (
    <span className={`sev sev--${severity}`} aria-label={`severity ${severity}`}>
      {[1, 2, 3, 4].map((i) => (
        <i key={i} className={i <= bars ? "on" : ""} />
      ))}
      <span className="sev__label">{severity}</span>
    </span>
  );
}

export function Panel({ title, icon, actions, children, className = "" }: { title: ReactNode; icon?: ReactNode; actions?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <section className={`panel ${className}`}>
      <header className="panel__head">
        <h2 className="panel__title">
          {icon}
          {title}
        </h2>
        {actions && <div className="panel__actions">{actions}</div>}
      </header>
      <div className="panel__body">{children}</div>
    </section>
  );
}

export function Empty({ children }: { children: ReactNode }) {
  return <p className="empty">{children}</p>;
}

export function Modal({ title, onClose, children, tone = "default" }: { title: string; onClose: () => void; children: ReactNode; tone?: "default" | "danger" }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const prev = document.activeElement as HTMLElement | null;
    ref.current?.querySelector<HTMLElement>("input, button:not(.modal__close)")?.focus();
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("keydown", onKey);
      prev?.focus?.();
    };
  }, [onClose]);
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div className={`modal modal--${tone}`} role="dialog" aria-modal="true" aria-label={title} ref={ref}>
        <header className="modal__head">
          <h3>{title}</h3>
          <button className="icon-btn modal__close" onClick={onClose} aria-label="Close">
            <X size={16} />
          </button>
        </header>
        <div className="modal__body">{children}</div>
      </div>
    </div>
  );
}

export function Meter({ value, tone = "cyan", label }: { value: number; tone?: string; label: string }) {
  const pct = Math.round(Math.min(1, Math.max(0, value)) * 100);
  return (
    <div className={`meter meter--${tone}`} role="progressbar" aria-label={label} aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
      <div className="meter__fill" style={{ width: `${pct}%` }} />
    </div>
  );
}
