import { useState } from "react";
import { OctagonX, Pause, Play, RotateCcw, ShieldAlert, ShieldCheck, Wifi, WifiOff } from "lucide-react";
import type { Kpis } from "../api/types";
import { fmtCompact, fmtEur, fmtMs, fmtPct } from "../lib/format";
import { useBoard } from "../store/store";
import { toast, toastError } from "../store/toasts";
import { useApi } from "./context";
import { Modal } from "./ui";

interface KpiDef {
  key: keyof Kpis;
  label: string;
  format: (v: number) => string;
  /** returns a tone for the current value */
  tone: (v: number) => "ok" | "warn" | "bad";
}

export const KPI_DEFS: KpiDef[] = [
  { key: "ordersPerSec", label: "Orders/s", format: (v) => fmtCompact(v), tone: () => "ok" },
  { key: "revenuePerMin", label: "Revenue/min", format: fmtEur, tone: () => "ok" },
  { key: "paymentSuccessRate", label: "Payments OK", format: (v) => fmtPct(v), tone: (v) => (v >= 0.9 ? "ok" : v >= 0.75 ? "warn" : "bad") },
  { key: "reservationSuccessRate", label: "Reservations OK", format: (v) => fmtPct(v), tone: (v) => (v >= 0.9 ? "ok" : v >= 0.75 ? "warn" : "bad") },
  { key: "e2eP95Ms", label: "E2E p95", format: fmtMs, tone: (v) => (v < 5000 ? "ok" : v < 20000 ? "warn" : "bad") },
  { key: "inFlight", label: "In flight", format: fmtCompact, tone: (v) => (v < 2000 ? "ok" : v < 10000 ? "warn" : "bad") },
  { key: "totalLag", label: "Total lag", format: fmtCompact, tone: (v) => (v < 1000 ? "ok" : v < 10000 ? "warn" : "bad") },
  { key: "dlqPerMin", label: "DLQ/min", format: (v) => fmtCompact(v), tone: (v) => (v < 1 ? "ok" : v < 10 ? "warn" : "bad") },
];

function Sparkline({ values }: { values: number[] }) {
  if (values.length < 2) return <svg className="spark" viewBox="0 0 60 18" aria-hidden="true" />;
  const max = Math.max(...values);
  const min = Math.min(...values);
  const span = max - min || 1;
  const pts = values.map((v, i) => `${((i / (values.length - 1)) * 60).toFixed(1)},${(16 - ((v - min) / span) * 14).toFixed(1)}`).join(" ");
  return (
    <svg className="spark" viewBox="0 0 60 18" aria-hidden="true">
      <polyline points={pts} />
    </svg>
  );
}

export function KpiTicker() {
  const kpis = useBoard((b) => b.kpis);
  const history = useBoard((b) => b.kpiHistory);
  return (
    <div className="kpis" aria-label="Live KPIs">
      {KPI_DEFS.map((d) => {
        const v = kpis ? kpis[d.key] : null;
        const tone = v === null ? "ok" : d.tone(v);
        return (
          <div className={`kpi kpi--${tone}`} key={d.key} data-testid={`kpi-${d.key}`}>
            <span className="kpi__label">{d.label}</span>
            <span className="kpi__value mono">{v === null ? "—" : d.format(v)}</span>
            <Sparkline values={history.slice(-30).map((h) => h.kpis[d.key])} />
          </div>
        );
      })}
    </div>
  );
}

export function TopBar() {
  const apiClient = useApi();
  const simulation = useBoard((b) => b.simulation);
  const chaosArmed = useBoard((b) => b.chaosArmed);
  const connection = useBoard((b) => b.connection);
  const retryInMs = useBoard((b) => b.retryInMs);
  const setChaosArmed = useBoard((b) => b.setChaosArmed);
  const [confirmArm, setConfirmArm] = useState(false);
  const [busy, setBusy] = useState(false);

  const run = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    try {
      await fn();
    } catch (e) {
      toastError(e);
    } finally {
      setBusy(false);
    }
  };

  const arm = (armed: boolean) =>
    run(async () => {
      const r = await apiClient.armChaos(armed);
      setChaosArmed(r.armed);
      setConfirmArm(false);
      toast(r.armed ? "warning" : "info", r.armed ? "Chaos armed" : "Chaos disarmed");
    });

  const panic = () =>
    run(async () => {
      const r = await apiClient.panic();
      setChaosArmed(false);
      toast("success", "PANIC: baseline restored", `${r.reverted} change(s) reverted, chaos disarmed`);
    });

  const sim = (action: "start" | "stop" | "reset") =>
    run(async () => {
      const r = await apiClient.simulation(action);
      toast("info", `Simulation ${action === "reset" ? "reset" : r.simulation}`);
    });

  const online = connection === "open";
  return (
    <header className="topbar">
      <div className="brand">
        <div className="brand__logo" aria-hidden="true">
          <span />
          <span />
          <span />
        </div>
        <div>
          <div className="brand__name">Kafka Lab</div>
          <div className="brand__sub">Chaos control room</div>
        </div>
      </div>

      <div className="simctl" role="group" aria-label="Simulation">
        <span className={`status-pill status-pill--${simulation}`}>
          <i />
          {simulation === "running" ? "Running" : "Stopped"}
        </span>
        {simulation === "running" ? (
          <button className="icon-btn" onClick={() => sim("stop")} disabled={busy} title="Stop simulation" aria-label="Stop simulation">
            <Pause size={16} />
          </button>
        ) : (
          <button className="icon-btn" onClick={() => sim("start")} disabled={busy} title="Start simulation" aria-label="Start simulation">
            <Play size={16} />
          </button>
        )}
        <button className="icon-btn" onClick={() => sim("reset")} disabled={busy} title="Reset to baseline" aria-label="Reset simulation">
          <RotateCcw size={16} />
        </button>
      </div>

      <KpiTicker />

      <div className="topbar__right">
        <span className={`conn conn--${connection}`} title={online ? "Live" : connection} aria-label={`connection ${connection}`}>
          {online ? <Wifi size={14} /> : <WifiOff size={14} />}
          {online ? "Live" : connection === "reconnecting" && retryInMs ? `Retry ${Math.round(retryInMs / 1000)}s` : connection}
        </span>
        <button
          className={`chaos-toggle ${chaosArmed ? "chaos-toggle--armed" : ""}`}
          role="switch"
          aria-checked={chaosArmed}
          onClick={() => (chaosArmed ? arm(false) : setConfirmArm(true))}
          disabled={busy}
        >
          {chaosArmed ? <ShieldAlert size={16} /> : <ShieldCheck size={16} />}
          <span>{chaosArmed ? "Chaos armed" : "Chaos safe"}</span>
          <i className="chaos-toggle__track">
            <b />
          </i>
        </button>
        <button className="panic" onClick={panic} disabled={busy} title="Revert everything and disarm chaos">
          <OctagonX size={18} />
          PANIC
        </button>
      </div>

      {confirmArm && (
        <Modal title="Arm chaos?" tone="danger" onClose={() => setConfirmArm(false)}>
          <p>
            Arming allows <strong>destructive scenarios</strong> — broker failures, network faults and regional outages. They only affect this local lab and
            revert automatically, and PANIC reverts everything at once.
          </p>
          <div className="modal__actions">
            <button className="btn" onClick={() => setConfirmArm(false)}>
              Keep safe
            </button>
            <button className="btn btn--danger" onClick={() => arm(true)}>
              Arm chaos
            </button>
          </div>
        </Modal>
      )}
    </header>
  );
}
