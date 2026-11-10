import { useState } from "react";
import { Flame, Lock, Play, Rocket, Square, TriangleAlert } from "lucide-react";
import type { ParamValue, ScenarioDef, ScenarioParam } from "../api/types";
import { CATEGORY_LABEL, coerceParam, defaultParams, groupScenarios, isActive, startBlocker } from "../lib/domain";
import { fmtDuration } from "../lib/format";
import { useBoard } from "../store/store";
import { describeError, toast, toastError } from "../store/toasts";
import { useApi } from "./context";
import { Empty, Modal, SeverityBadge } from "./ui";

function ParamField({ param, value, onChange }: { param: ScenarioParam; value: ParamValue; onChange: (v: ParamValue) => void }) {
  const id = `param-${param.key}`;
  if (param.type === "boolean") {
    return (
      <label className="field field--check">
        <input id={id} type="checkbox" checked={value === true} onChange={(e) => onChange(e.target.checked)} />
        <span>{param.label}</span>
      </label>
    );
  }
  if (param.type === "broker") {
    return (
      <fieldset className="field">
        <legend>{param.label}</legend>
        <div className="segmented">
          {(param.options ?? ["0", "1", "2"]).map((o) => (
            <button type="button" key={o} className={String(value) === o ? "is-on" : ""} aria-pressed={String(value) === o} onClick={() => onChange(coerceParam(param, o))}>
              Broker {o}
            </button>
          ))}
        </div>
      </fieldset>
    );
  }
  if (param.type === "select" || param.type === "service") {
    return (
      <label className="field" htmlFor={id}>
        <span>{param.label}</span>
        <select id={id} value={String(value)} onChange={(e) => onChange(coerceParam(param, e.target.value))}>
          {(param.options ?? []).map((o) => (
            <option key={o} value={o}>
              {o}
            </option>
          ))}
        </select>
      </label>
    );
  }
  return (
    <label className="field" htmlFor={id}>
      <span>
        {param.label}
        {param.unit && <em> ({param.unit})</em>}
      </span>
      <div className="field__range">
        <input type="range" aria-label={`${param.label} slider`} min={param.min} max={param.max} step={param.step} value={Number(value)} onChange={(e) => onChange(coerceParam(param, e.target.value))} />
        <input id={id} type="number" className="mono" min={param.min} max={param.max} step={param.step} value={Number(value)} onChange={(e) => onChange(coerceParam(param, e.target.value))} />
      </div>
    </label>
  );
}

export function LaunchDialog({ scenario, onClose }: { scenario: ScenarioDef; onClose: () => void }) {
  const apiClient = useApi();
  const chaosArmed = useBoard((b) => b.chaosArmed);
  const putExperiment = useBoard((b) => b.putExperiment);
  const [params, setParams] = useState(() => defaultParams(scenario.params));
  const [duration, setDuration] = useState(scenario.defaultDurationSec);
  const [delay, setDelay] = useState(0);
  const [autoRecover, setAutoRecover] = useState(true);
  const [confirm, setConfirm] = useState("");
  const [error, setError] = useState<{ title: string; detail: string } | null>(null);
  const [busy, setBusy] = useState(false);
  const blocker = startBlocker(scenario, chaosArmed, confirm);

  const start = async () => {
    setBusy(true);
    setError(null);
    try {
      const e = await apiClient.startExperiment({
        scenario: scenario.id,
        params,
        durationSec: duration,
        startDelaySec: delay,
        autoRecover,
        ...(scenario.destructive ? { confirm } : {}),
      });
      putExperiment(e);
      toast("warning", `${scenario.name} ${e.status === "scheduled" ? "scheduled" : "started"}`, `${fmtDuration(duration)}${autoRecover ? ", auto-recover" : ", manual recovery"}`);
      onClose();
    } catch (err) {
      setError(describeError(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal title={`Launch: ${scenario.name}`} onClose={onClose} tone={scenario.destructive ? "danger" : "default"}>
      <p className="muted">{scenario.description}</p>
      <form
        className="launch-form"
        onSubmit={(e) => {
          e.preventDefault();
          if (!blocker) void start();
        }}
      >
        {scenario.params.map((p) => (
          <ParamField key={p.key} param={p} value={params[p.key]!} onChange={(v) => setParams({ ...params, [p.key]: v })} />
        ))}
        <div className="launch-form__row">
          <label className="field" htmlFor="launch-duration">
            <span>Duration (s)</span>
            <input id="launch-duration" type="number" className="mono" min={5} max={3600} value={duration} onChange={(e) => setDuration(Math.max(5, Number(e.target.value) || 5))} />
          </label>
          <label className="field" htmlFor="launch-delay">
            <span>Start delay (s)</span>
            <input id="launch-delay" type="number" className="mono" min={0} max={600} value={delay} onChange={(e) => setDelay(Math.max(0, Number(e.target.value) || 0))} />
          </label>
        </div>
        <label className="field field--check">
          <input type="checkbox" checked={autoRecover} onChange={(e) => setAutoRecover(e.target.checked)} />
          <span>Recover automatically when the duration ends</span>
        </label>
        {scenario.destructive && (
          <div className="danger-zone">
            <p>
              <TriangleAlert size={15} /> Destructive. {chaosArmed ? "Type the scenario id to confirm." : "Arm chaos in the top bar first."}
            </p>
            <input aria-label="Confirm scenario id" className="mono" placeholder={scenario.id} value={confirm} disabled={!chaosArmed} onChange={(e) => setConfirm(e.target.value)} />
          </div>
        )}
        {error && (
          <div className="form-error" role="alert">
            <strong>{error.title}</strong> {error.detail}
          </div>
        )}
        <div className="modal__actions">
          {blocker && <span className="muted small">{blocker}</span>}
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className={`btn ${scenario.destructive ? "btn--danger" : "btn--primary"}`} disabled={!!blocker || busy}>
            <Rocket size={15} /> {delay > 0 ? "Schedule" : "Start"}
          </button>
        </div>
      </form>
    </Modal>
  );
}

function ScenarioCard({ scenario, onLaunch }: { scenario: ScenarioDef; onLaunch: () => void }) {
  const apiClient = useApi();
  const chaosArmed = useBoard((b) => b.chaosArmed);
  const putExperiment = useBoard((b) => b.putExperiment);
  const experiments = useBoard((b) => b.experiments);
  const running = experiments.filter((e) => e.scenario === scenario.id && isActive(e));
  const locked = scenario.destructive && !chaosArmed;
  const stop = async (id: string) => {
    try {
      putExperiment(await apiClient.stopExperiment(id));
      toast("info", `${scenario.name} stopped`, "Fault reverted.");
    } catch (e) {
      toastError(e);
    }
  };
  return (
    <article className={`scenario scenario--${scenario.category} ${running.length ? "scenario--running" : ""} ${locked ? "scenario--locked" : ""}`} data-testid={`scenario-${scenario.id}`}>
      <header>
        <h4>{scenario.name}</h4>
        <SeverityBadge severity={scenario.severity} />
      </header>
      <p>{scenario.description}</p>
      <ul className="chips">
        {scenario.expectedSymptoms.map((s) => (
          <li key={s}>{s}</li>
        ))}
      </ul>
      <footer>
        <span className="muted small mono">{fmtDuration(scenario.defaultDurationSec)}</span>
        {scenario.destructive && (
          <span className="tag tag--danger" title="Requires armed chaos and typed confirmation">
            {locked ? <Lock size={11} /> : <Flame size={11} />} destructive
          </span>
        )}
        <span className="grow" />
        {running.map((e) => (
          <button key={e.id} className="btn btn--sm btn--ghost" onClick={() => stop(e.id)} aria-label={`Stop ${scenario.name}`}>
            <Square size={12} /> Stop
          </button>
        ))}
        <button className="btn btn--sm btn--primary" onClick={onLaunch} disabled={locked} title={locked ? "Arm chaos first" : `Launch ${scenario.name}`} aria-label={`Launch ${scenario.name}`}>
          <Play size={12} /> Launch
        </button>
      </footer>
    </article>
  );
}

export function ScenarioLauncher() {
  const scenarios = useBoard((b) => b.scenarios);
  const [selected, setSelected] = useState<ScenarioDef | null>(null);
  if (!scenarios.length) return <Empty>Loading scenario library…</Empty>;
  return (
    <div className="scenarios">
      {groupScenarios(scenarios).map((g) => (
        <section key={g.category} className={`scenario-group scenario-group--${g.category}`}>
          <h3>
            {CATEGORY_LABEL[g.category]} <span className="count">{g.scenarios.length}</span>
          </h3>
          <div className="scenario-grid">
            {g.scenarios.map((s) => (
              <ScenarioCard key={s.id} scenario={s} onLaunch={() => setSelected(s)} />
            ))}
          </div>
        </section>
      ))}
      {selected && <LaunchDialog scenario={selected} onClose={() => setSelected(null)} />}
    </div>
  );
}

