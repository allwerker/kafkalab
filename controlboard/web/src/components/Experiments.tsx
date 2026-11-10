import { CheckCircle2, CircleDashed, Clock, FlaskConical, Square, Timer } from "lucide-react";
import type { Experiment } from "../api/types";
import { confirmedCount, experimentProgress, isActive } from "../lib/domain";
import { fmtClock, fmtCompact, fmtDuration, fmtEur } from "../lib/format";
import { useBoard } from "../store/store";
import { toast, toastError } from "../store/toasts";
import { useApi, useNow } from "./context";
import { Empty, Meter } from "./ui";

const STATUS_TONE: Record<Experiment["status"], string> = {
  scheduled: "blue",
  running: "amber",
  recovering: "cyan",
  completed: "green",
  aborted: "grey",
  failed: "red",
};

function ObservationTable({ e }: { e: Experiment }) {
  if (!e.observations.length) return <p className="muted small">{e.scenario === "hidden" ? "Observations are hidden during the drill." : "Waiting for the first observation…"}</p>;
  return (
    <table className="obs">
      <thead>
        <tr>
          <th />
          <th>Check</th>
          <th>Requested</th>
          <th>Observed</th>
        </tr>
      </thead>
      <tbody>
        {e.observations.map((o) => (
          <tr key={o.label} className={o.confirmed ? "is-confirmed" : ""}>
            <td>{o.confirmed ? <CheckCircle2 size={15} aria-label="confirmed" /> : <CircleDashed size={15} aria-label="not yet observed" className="spin-slow" />}</td>
            <td>{o.label}</td>
            <td className="mono">{o.requested}</td>
            <td className="mono">
              {o.observed}
              {o.confirmed && <span className="muted small"> · {fmtClock(o.at)}</span>}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function ReportCard({ e }: { e: Experiment }) {
  const r = e.report!;
  return (
    <div className="report" aria-label={`Report for ${e.name}`}>
      <div className="report__stat">
        <span>Time to detect</span>
        <b className="mono">{r.timeToDetectSec === undefined ? "not detected" : fmtDuration(r.timeToDetectSec)}</b>
      </div>
      <div className="report__stat">
        <span>Time to recover</span>
        <b className="mono">{r.timeToRecoverSec === undefined ? "—" : fmtDuration(r.timeToRecoverSec)}</b>
      </div>
      <div className="report__stat">
        <span>Peak lag</span>
        <b className="mono">{fmtCompact(r.peakLag)}</b>
      </div>
      <div className="report__stat">
        <span>Orders affected</span>
        <b className="mono">{fmtCompact(r.ordersAffected)}</b>
      </div>
      <div className="report__stat">
        <span>Revenue impact</span>
        <b className="mono">{fmtEur(r.revenueImpactEur)}</b>
      </div>
      <div className="report__alerts">
        {r.alerts.length ? r.alerts.map((a) => <span key={a} className="tag tag--alert">{a}</span>) : <span className="muted small">No alerts fired</span>}
      </div>
    </div>
  );
}

function ExperimentCard({ e, now }: { e: Experiment; now: number }) {
  const apiClient = useApi();
  const putExperiment = useBoard((b) => b.putExperiment);
  const p = experimentProgress(e, now);
  const active = isActive(e);
  const stop = async () => {
    try {
      putExperiment(await apiClient.stopExperiment(e.id));
      toast("info", `${e.name} stopped`);
    } catch (err) {
      toastError(err);
    }
  };
  const confirmed = confirmedCount(e);
  return (
    <article className={`exp exp--${e.status}`} data-testid={`experiment-${e.id}`}>
      <header>
        <FlaskConical size={16} />
        <h4>{e.name}</h4>
        <span className={`tag tag--${STATUS_TONE[e.status]}`}>{e.status}</span>
        <span className="grow" />
        {e.observations.length > 0 && (
          <span className="muted small mono" title="confirmed observations">
            {confirmed}/{e.observations.length} confirmed
          </span>
        )}
        {active && (
          <button className="btn btn--sm btn--ghost" onClick={stop} aria-label={`Stop ${e.name}`}>
            <Square size={12} /> Stop
          </button>
        )}
      </header>
      {active && (
        <div className="exp__progress">
          {e.status === "scheduled" ? (
            <span className="mono">
              <Clock size={13} /> starts in {fmtDuration(p.remainingSec)}
            </span>
          ) : e.status === "recovering" ? (
            <span className="mono">
              <Timer size={13} /> recovering — waiting for KPIs to return to baseline
            </span>
          ) : p.overdue ? (
            <span className="mono warn">
              <Timer size={13} /> past its end — manual recovery (press Stop)
            </span>
          ) : (
            <span className="mono">
              <Timer size={13} /> {fmtDuration(p.remainingSec)} left
            </span>
          )}
          <Meter value={e.status === "recovering" ? 1 : p.fraction} tone={e.status === "recovering" ? "cyan" : "amber"} label={`${e.name} progress`} />
        </div>
      )}
      {e.affected.length > 0 && (
        <div className="chips chips--inline">
          {e.affected.map((a) => (
            <span key={a} className="tag">
              {a}
            </span>
          ))}
        </div>
      )}
      <ObservationTable e={e} />
      {e.report && <ReportCard e={e} />}
    </article>
  );
}

export function Experiments() {
  const experiments = useBoard((b) => b.experiments);
  const now = useNow();
  const active = experiments.filter(isActive);
  const past = experiments.filter((e) => !isActive(e)).slice(0, 8);
  return (
    <div className="experiments">
      <h3 className="subhead">
        Active <span className="count">{active.length}</span>
      </h3>
      {active.length === 0 && <Empty>No experiment running. Launch one from the Scenarios tab.</Empty>}
      {active.map((e) => (
        <ExperimentCard key={e.id} e={e} now={now} />
      ))}
      {past.length > 0 && (
        <>
          <h3 className="subhead">Recent</h3>
          {past.map((e) => (
            <ExperimentCard key={e.id} e={e} now={now} />
          ))}
        </>
      )}
    </div>
  );
}
