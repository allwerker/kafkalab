import { useState } from "react";
import { Loader2, Minus, Pause, Play, Plus, RotateCcw, Save } from "lucide-react";
import type { KnobsPatch, ServiceState } from "../api/types";
import { isApplying } from "../lib/domain";
import { fmtCompact } from "../lib/format";
import { SERVICE_LABELS, type ServiceName } from "../lib/topology";
import { useBoard } from "../store/store";
import { toast, toastError } from "../store/toasts";
import { useApi } from "./context";
import { Empty, HealthDot } from "./ui";

function groupLag(lag: Record<string, Record<string, number[]>>, name: string): number | null {
  const g = lag[name];
  if (!g) return null;
  return Object.values(g).reduce((s, arr) => s + arr.reduce((a, b) => a + b, 0), 0);
}

function KnobEditor({ svc, onSave }: { svc: ServiceState; onSave: (patch: KnobsPatch) => void }) {
  const isGenerator = svc.name === "order-generator";
  const [latency, setLatency] = useState(Number(svc.knobs.processingLatencyMs ?? 0));
  const [failure, setFailure] = useState(svc.knobs.businessFailureProbability === undefined ? "" : String(svc.knobs.businessFailureProbability));
  const [rate, setRate] = useState(svc.knobs.ordersPerSecond === undefined ? "" : String(svc.knobs.ordersPerSecond));
  const save = () => {
    if (isGenerator) onSave({ ordersPerSecond: rate === "" ? null : Number(rate) });
    else onSave({ processingLatencyMs: latency, businessFailureProbability: failure === "" ? null : Math.min(1, Math.max(0, Number(failure))) });
  };
  return (
    <form
      className="knob-editor"
      onSubmit={(e) => {
        e.preventDefault();
        save();
      }}
    >
      {isGenerator ? (
        <label className="field field--inline">
          <span>Orders/s</span>
          <input type="number" className="mono" min={0} max={3000} placeholder="50" value={rate} onChange={(e) => setRate(e.target.value)} aria-label={`${svc.name} orders per second`} />
        </label>
      ) : (
        <>
          <label className="field field--inline">
            <span>Latency ms</span>
            <input type="number" className="mono" min={0} max={10000} step={10} value={latency} onChange={(e) => setLatency(Number(e.target.value) || 0)} aria-label={`${svc.name} latency`} />
          </label>
          <label className="field field--inline">
            <span>Failure p</span>
            <input type="number" className="mono" min={0} max={1} step={0.01} placeholder="—" value={failure} onChange={(e) => setFailure(e.target.value)} aria-label={`${svc.name} failure probability`} />
          </label>
        </>
      )}
      <button type="submit" className="icon-btn" aria-label={`Apply knobs to ${svc.name}`} title="Apply">
        <Save size={14} />
      </button>
    </form>
  );
}

function ServiceRow({ svc, lag }: { svc: ServiceState; lag: number | null }) {
  const apiClient = useApi();
  const putService = useBoard((b) => b.putService);
  const call = async (fn: () => Promise<ServiceState>, message: string) => {
    try {
      putService(await fn());
      toast("info", message);
    } catch (e) {
      toastError(e);
    }
  };
  const scale = (n: number) => call(() => apiClient.scale(svc.name, n), `${svc.name} → ${n} replicas`);
  const knobs = Object.entries(svc.knobs);
  const applying = isApplying(svc);
  const label = SERVICE_LABELS[svc.name as ServiceName] ?? svc.name;
  return (
    <article className={`svc svc--${svc.health} ${svc.paused ? "svc--paused" : ""}`} data-testid={`service-${svc.name}`}>
      <header>
        <HealthDot health={svc.health} label={`${svc.name} health ${svc.health}`} />
        <h4>{label}</h4>
        {svc.paused && <span className="tag tag--blue">paused</span>}
        <span className="grow" />
        <span className={`gen mono ${applying ? "gen--applying" : ""}`} title="applied / desired control generation">
          {applying ? (
            <>
              <Loader2 size={12} className="spin" /> applying… {svc.appliedGeneration}/{svc.desiredGeneration}
            </>
          ) : (
            <>gen {svc.appliedGeneration}</>
          )}
        </span>
      </header>
      <div className="svc__row">
        <div className="replicas" role="group" aria-label={`${svc.name} replicas`}>
          <button className="icon-btn" onClick={() => scale(svc.replicas - 1)} disabled={svc.replicas <= 0} aria-label={`Scale ${svc.name} down`}>
            <Minus size={13} />
          </button>
          <div className="pips" aria-hidden="true">
            {[1, 2, 3, 4, 5, 6].map((i) => (
              <i key={i} className={i <= svc.readyReplicas ? "ready" : i <= svc.replicas ? "starting" : ""} />
            ))}
          </div>
          <button className="icon-btn" onClick={() => scale(svc.replicas + 1)} disabled={svc.replicas >= 6} aria-label={`Scale ${svc.name} up`}>
            <Plus size={13} />
          </button>
          <span className="mono small">
            {svc.readyReplicas}/{svc.replicas}
          </span>
        </div>
        {lag !== null && (
          <span className="svc__lag mono" title="consumer lag">
            lag <b>{fmtCompact(lag)}</b>
          </span>
        )}
        <button
          className={`btn btn--sm ${svc.paused ? "btn--primary" : "btn--ghost"}`}
          onClick={() => call(() => apiClient.putKnobs(svc.name, { paused: !svc.paused }), `${svc.name} ${svc.paused ? "resumed" : "paused"}`)}
        >
          {svc.paused ? <Play size={12} /> : <Pause size={12} />} {svc.paused ? "Resume" : "Pause"}
        </button>
      </div>
      <KnobEditor svc={svc} onSave={(patch) => call(() => apiClient.putKnobs(svc.name, patch), `${svc.name} knobs applied`)} />
      {knobs.length > 0 && (
        <div className="knobs">
          {knobs.map(([k, v]) => (
            <span key={k} className="tag tag--knob mono">
              {k}={String(v)}
            </span>
          ))}
          <button className="btn btn--sm btn--ghost" onClick={() => call(() => apiClient.resetKnobs(svc.name), `${svc.name} back to baseline`)} aria-label={`Reset knobs of ${svc.name}`}>
            <RotateCcw size={12} /> Reset
          </button>
        </div>
      )}
    </article>
  );
}

export function Services() {
  const services = useBoard((b) => b.services);
  const lag = useBoard((b) => b.lag);
  if (!services.length) return <Empty>Waiting for service state…</Empty>;
  return (
    <div className="services">
      {services.map((s) => (
        <ServiceRow key={s.name} svc={s} lag={groupLag(lag, s.name)} />
      ))}
    </div>
  );
}
