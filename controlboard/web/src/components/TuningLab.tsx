import { useEffect, useState } from "react";
import { RotateCcw, Save, TriangleAlert } from "lucide-react";
import type { TuningKey, TuningResponse, TuningSetting, TuningUpdate } from "../api/types";
import { isLossy } from "../lib/domain";
import { SERVICE_LABELS, type ServiceName } from "../lib/topology";
import { toast, toastError } from "../store/toasts";
import { useApi } from "./context";
import { Empty } from "./ui";

type Values = Partial<Record<TuningKey, number | string>>;

function SettingField({ s, value, applied, onChange }: { s: TuningSetting; value: number | string | undefined; applied: number | string | undefined; onChange: (v: number | string) => void }) {
  const lossy = isLossy(s, value);
  const changed = value !== applied;
  const id = `tuning-${s.key}`;
  return (
    <div className={`tuning-field ${lossy ? "tuning-field--lossy" : ""} ${changed ? "tuning-field--changed" : ""}`}>
      <label htmlFor={id}>
        {s.label}
        {value !== s.default && <span className="tag tag--knob">non-default</span>}
      </label>
      {s.type === "select" ? (
        <select id={id} value={String(value ?? s.default)} onChange={(e) => onChange(e.target.value)}>
          {(s.options ?? []).map((o) => (
            <option key={o} value={o}>
              {o}
              {s.lossy?.includes(o) ? " (lossy)" : ""}
            </option>
          ))}
        </select>
      ) : (
        <input id={id} type="number" className="mono" min={s.min} max={s.max} step={s.step} value={Number(value ?? s.default)} onChange={(e) => onChange(Number(e.target.value))} />
      )}
      <p className="help">{s.help}</p>
      {lossy && (
        <p className="lossy" role="alert">
          <TriangleAlert size={13} /> Weakens delivery guarantees — records can be lost on broker failure.
        </p>
      )}
      <p className="muted small mono">
        applied: {String(applied ?? s.default)} · default: {String(s.default)}
      </p>
    </div>
  );
}

export function TuningLab() {
  const apiClient = useApi();
  const [data, setData] = useState<TuningResponse | null>(null);
  const [service, setService] = useState<string>("payment");
  const [draft, setDraft] = useState<Values>({});
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    apiClient.tuning().then(setData, toastError);
  }, [apiClient]);

  useEffect(() => {
    if (data) setDraft({ ...data.services[service] });
  }, [data, service]);

  if (!data) return <Empty>Loading client settings…</Empty>;
  const applied = data.services[service] ?? {};

  const submit = async (values: TuningUpdate, message: string) => {
    setBusy(true);
    try {
      setData(await apiClient.putTuning(service, values));
      toast("success", message);
    } catch (e) {
      toastError(e);
    } finally {
      setBusy(false);
    }
  };

  const changes: TuningUpdate = Object.fromEntries(Object.entries(draft).filter(([k, v]) => applied[k as TuningKey] !== v));
  const anyLossy = data.settings.some((s) => isLossy(s, draft[s.key]));

  return (
    <div className="tuning">
      <div className="tabs tabs--pills" role="tablist" aria-label="Service">
        {Object.keys(data.services).map((name) => (
          <button key={name} role="tab" aria-selected={name === service} className={name === service ? "is-on" : ""} onClick={() => setService(name)}>
            {SERVICE_LABELS[name as ServiceName] ?? name}
          </button>
        ))}
      </div>
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void submit(changes, `${service}: client tuning applied`);
        }}
      >
        <div className="tuning-grid">
          {data.settings.map((s) => (
            <SettingField key={s.key} s={s} value={draft[s.key]} applied={applied[s.key]} onChange={(v) => setDraft({ ...draft, [s.key]: v })} />
          ))}
        </div>
        <div className="tuning__actions">
          {anyLossy && (
            <span className="warn small">
              <TriangleAlert size={13} /> lossy configuration
            </span>
          )}
          <span className="grow" />
          <button
            type="button"
            className="btn"
            disabled={busy}
            onClick={() => submit(Object.fromEntries(data.settings.map((s) => [s.key, null])) as TuningUpdate, `${service}: client tuning reset to defaults`)}
          >
            <RotateCcw size={14} /> Reset to defaults
          </button>
          <button type="submit" className="btn btn--primary" disabled={busy || Object.keys(changes).length === 0}>
            <Save size={14} /> Apply {Object.keys(changes).length > 0 ? `(${Object.keys(changes).length})` : ""}
          </button>
        </div>
      </form>
    </div>
  );
}
