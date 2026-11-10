import { useState } from "react";
import { Boxes, FlaskConical, Gauge, History, Rocket, Rows3, SlidersHorizontal, Target, X, type LucideIcon } from "lucide-react";
import { loadPref, savePref } from "../lib/browser";
import { isActive } from "../lib/domain";
import { useBoard } from "../store/store";
import { toastStore, useToasts } from "../store/toasts";
import { AlertPanel } from "./AlertPanel";
import { Experiments } from "./Experiments";
import { IncidentDrill } from "./IncidentDrill";
import { Launchpad } from "./Launchpad";
import { LiveView, type StartLiveGame } from "./LiveView";
import { ScenarioLauncher } from "./ScenarioLauncher";
import { Services } from "./Services";
import { Timeline } from "./Timeline";
import { TopBar } from "./TopBar";
import { TuningLab } from "./TuningLab";

type TabId = "scenarios" | "experiments" | "services" | "tuning" | "drill" | "launchpad";

const TABS: { id: TabId; label: string; icon: LucideIcon }[] = [
  { id: "scenarios", label: "Scenarios", icon: Rocket },
  { id: "experiments", label: "Experiments", icon: FlaskConical },
  { id: "services", label: "Services", icon: Boxes },
  { id: "tuning", label: "Tuning Lab", icon: SlidersHorizontal },
  { id: "drill", label: "Incident Drill", icon: Target },
  { id: "launchpad", label: "Launchpad", icon: Gauge },
];

function Toasts() {
  const toasts = useToasts();
  return (
    <div className="toasts" aria-live="polite">
      {toasts.map((t) => (
        <div key={t.id} className={`toast toast--${t.tone}`} role={t.tone === "error" ? "alert" : "status"}>
          <div>
            <b>{t.title}</b>
            {t.detail && <span>{t.detail}</span>}
          </div>
          <button className="icon-btn" aria-label="Dismiss" onClick={() => toastStore.getState().dismiss(t.id)}>
            <X size={13} />
          </button>
        </div>
      ))}
    </div>
  );
}

function BrokerStrip() {
  const brokers = useBoard((b) => b.brokers);
  const services = useBoard((b) => b.services);
  const up = brokers.filter((b) => b.up).length;
  const healthy = services.filter((s) => s.health === "up").length;
  return (
    <div className="mini-stats" aria-label="Cluster summary">
      <span>
        <Rows3 size={13} /> brokers <b className="mono">{up}/{brokers.length || 3}</b>
      </span>
      <span>
        <Boxes size={13} /> services healthy <b className="mono">{healthy}/{services.length || 6}</b>
      </span>
    </div>
  );
}

export function App({ loadLive }: { loadLive?: () => Promise<StartLiveGame> }) {
  const [tab, setTab] = useState<TabId>(() => loadPref<TabId>("tab", "scenarios"));
  const activeCount = useBoard((b) => b.experiments.filter(isActive).length);
  const select = (id: TabId) => {
    setTab(id);
    savePref("tab", id);
  };
  return (
    <div className="app">
      <TopBar />
      <main className="stage">
        <div className="stage__hero">{loadLive ? <LiveView load={loadLive} /> : <LiveView />}</div>
        <AlertPanel />
      </main>
      <section className="deck">
        <nav className="tabs" role="tablist" aria-label="Control deck">
          {TABS.map((t) => (
            <button key={t.id} role="tab" aria-selected={tab === t.id} className={tab === t.id ? "is-on" : ""} onClick={() => select(t.id)}>
              <t.icon size={15} /> {t.label}
              {t.id === "experiments" && activeCount > 0 && <span className="badge badge--amber">{activeCount}</span>}
            </button>
          ))}
          <span className="grow" />
          <BrokerStrip />
        </nav>
        <div className="deck__body" role="tabpanel">
          {tab === "scenarios" && <ScenarioLauncher />}
          {tab === "experiments" && (
            <div className="split">
              <Experiments />
              <div className="split__side">
                <h3 className="subhead">
                  <History size={14} /> Timeline
                </h3>
                <Timeline />
              </div>
            </div>
          )}
          {tab === "services" && <Services />}
          {tab === "tuning" && <TuningLab />}
          {tab === "drill" && <IncidentDrill />}
          {tab === "launchpad" && <Launchpad />}
        </div>
      </section>
      <Toasts />
    </div>
  );
}
