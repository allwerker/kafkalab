import { Crown, LayoutDashboard, Server, Warehouse, X } from "lucide-react";
import { fmtCompact } from "../lib/format";
import { CONSUMES, GRAFANA_BASE, SERVICE_LABELS, dashboardUrl, type ServiceName } from "../lib/topology";
import { PRODUCES } from "../live/model/stations";
import { useBoard } from "../store/store";
import { HealthDot } from "./ui";

export type Selection = { type: "station"; id: ServiceName } | { type: "broker"; id: number };

export function SideCard({ selection, onClose }: { selection: Selection; onClose: () => void }) {
  const services = useBoard((b) => b.services);
  const brokers = useBoard((b) => b.brokers);
  const lag = useBoard((b) => b.lag);
  const rates = useBoard((b) => b.rates);
  const grafana = useBoard((b) => b.links.find((l) => l.id === "grafana")?.url ?? GRAFANA_BASE);

  if (selection.type === "broker") {
    const b = brokers.find((x) => x.id === selection.id);
    return (
      <aside className="sidecard" role="dialog" aria-label={`Broker ${selection.id}`}>
        <header>
          <Server size={16} />
          <h3>Broker {selection.id}</h3>
          {b?.isController && (
            <span className="tag tag--amber">
              <Crown size={11} /> controller
            </span>
          )}
          <button className="icon-btn" onClick={onClose} aria-label="Close">
            <X size={14} />
          </button>
        </header>
        <dl className="facts">
          <dt>Status</dt>
          <dd>{b ? (b.up ? "up" : "DOWN") : "unknown"}</dd>
          <dt>Partition leaders</dt>
          <dd className="mono">{b?.leaderCount ?? 0}</dd>
          <dt>Under-replicated</dt>
          <dd className="mono">{b?.underReplicated ?? 0}</dd>
        </dl>
        <div className="sidecard__links">
          <a href={dashboardUrl("kafka-broker", grafana, { kafka_broker: String(selection.id) })} target="_blank" rel="noreferrer">
            <LayoutDashboard size={13} /> Broker dashboard
          </a>
          <a href={dashboardUrl("kafka-kraft", grafana)} target="_blank" rel="noreferrer">
            <LayoutDashboard size={13} /> KRaft controller
          </a>
        </div>
      </aside>
    );
  }

  const id = selection.id;
  const s = services.find((x) => x.name === id);
  const groupLag = lag[id] ?? {};
  const consumes = id === "order-generator" ? [] : (CONSUMES[id as keyof typeof CONSUMES] ?? []);
  const produced = (PRODUCES[id] ?? []).reduce((sum, t) => sum + (rates[t] ?? 0), 0);
  return (
    <aside className="sidecard" role="dialog" aria-label={SERVICE_LABELS[id]}>
      <header>
        <Warehouse size={16} />
        <h3>{SERVICE_LABELS[id]}</h3>
        {s && <HealthDot health={s.health} />}
        <button className="icon-btn" onClick={onClose} aria-label="Close">
          <X size={14} />
        </button>
      </header>
      <dl className="facts">
        <dt>Replicas</dt>
        <dd className="mono">
          {s?.readyReplicas ?? 0}/{s?.replicas ?? 0} ready
        </dd>
        <dt>Produces</dt>
        <dd className="mono">{fmtCompact(produced)} rec/s</dd>
        {s?.paused && (
          <>
            <dt>Paused</dt>
            <dd>yes</dd>
          </>
        )}
      </dl>
      {consumes.length > 0 && (
        <table className="lag-table">
          <thead>
            <tr>
              <th>Topic</th>
              <th>Lag per partition</th>
              <th>Σ</th>
            </tr>
          </thead>
          <tbody>
            {consumes.map((t) => {
              const parts = groupLag[t] ?? [];
              const max = Math.max(1, ...parts);
              return (
                <tr key={t}>
                  <td className="mono small">{t}</td>
                  <td>
                    <div className="lagbars" aria-label={`${t} lag per partition`}>
                      {parts.map((v, p) => (
                        <i key={p} style={{ height: `${Math.max(2, (v / max) * 18)}px` }} title={`p${p}: ${v}`} className={v > 1000 ? "hot" : ""} />
                      ))}
                    </div>
                  </td>
                  <td className="mono small">{fmtCompact(parts.reduce((a, b) => a + b, 0))}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
      <div className="sidecard__links">
        <a href={dashboardUrl("lab-services", grafana, { service: id })} target="_blank" rel="noreferrer">
          <LayoutDashboard size={13} /> Service dashboard
        </a>
        {consumes.length > 0 && (
          <a href={dashboardUrl("kafka-lag", grafana, { consumergroup: id })} target="_blank" rel="noreferrer">
            <LayoutDashboard size={13} /> Consumer lag
          </a>
        )}
      </div>
    </aside>
  );
}
