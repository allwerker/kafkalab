import { useEffect, useRef, useState } from "react";
import { Bell, BellOff, BookOpen, ExternalLink, LayoutDashboard, Send, Volume2, VolumeX } from "lucide-react";
import type { Alert } from "../api/types";
import { beep, loadPref, notify, requestNotifications, savePref } from "../lib/browser";
import { groupAlerts, newlyFiring } from "../lib/domain";
import { fmtAge } from "../lib/format";
import { useBoard } from "../store/store";
import { toast, toastError } from "../store/toasts";
import { useApi, useNow } from "./context";
import { Empty } from "./ui";

function AlertCard({ alert, now, unread }: { alert: Alert; now: number; unread: boolean }) {
  const age = alert.status === "resolved" && alert.endsAt ? `resolved ${fmtAge(alert.endsAt, now)} ago` : fmtAge(alert.startsAt, now);
  return (
    <li className={`alert alert--${alert.severity} alert--${alert.status} ${unread ? "alert--unread" : ""}`} data-testid="alert">
      <div className="alert__head">
        <span className={`alert__sev alert__sev--${alert.severity}`}>{alert.severity}</span>
        <span className="alert__name">{alert.name}</span>
        <span className="alert__age mono" title={alert.startsAt}>
          {age}
        </span>
      </div>
      <p className="alert__summary">{alert.summary}</p>
      <div className="alert__meta">
        <span className="tag">{alert.component}</span>
        {alert.dashboardUrl && (
          <a href={alert.dashboardUrl} target="_blank" rel="noreferrer" title="Open dashboard">
            <LayoutDashboard size={13} /> Dashboard
          </a>
        )}
        {alert.runbookUrl && (
          <a href={alert.runbookUrl} target="_blank" rel="noreferrer" title="Open runbook">
            <BookOpen size={13} /> Runbook
          </a>
        )}
        {alert.generatorUrl && (
          <a href={alert.generatorUrl} target="_blank" rel="noreferrer" title="Open rule in vmalert">
            <ExternalLink size={13} /> Rule
          </a>
        )}
      </div>
    </li>
  );
}

function Group({ title, alerts, now, unread, tone }: { title: string; alerts: Alert[]; now: number; unread: Set<string>; tone: string }) {
  if (!alerts.length) return null;
  return (
    <div className={`alert-group alert-group--${tone}`}>
      <h3>
        {title} <span className="count">{alerts.length}</span>
      </h3>
      <ul>
        {alerts.map((a) => (
          <AlertCard key={a.fingerprint} alert={a} now={now} unread={unread.has(a.fingerprint)} />
        ))}
      </ul>
    </div>
  );
}

export function AlertPanel() {
  const apiClient = useApi();
  const alerts = useBoard((b) => b.alerts);
  const unreadList = useBoard((b) => b.unreadAlerts);
  const markRead = useBoard((b) => b.markAlertsRead);
  const now = useNow();
  const [sound, setSound] = useState(() => loadPref("sound", false));
  const [notifications, setNotifications] = useState(() => loadPref("notifications", false));
  const prev = useRef<Alert[]>(alerts);

  useEffect(() => {
    const fresh = newlyFiring(prev.current, alerts);
    prev.current = alerts;
    for (const a of fresh) {
      if (sound) beep(a.severity);
      if (notifications) notify(`${a.severity.toUpperCase()}: ${a.name}`, a.summary);
    }
  }, [alerts, sound, notifications]);

  const toggleSound = () => {
    setSound(!sound);
    savePref("sound", !sound);
    if (!sound) beep("info");
  };

  const toggleNotifications = async () => {
    if (notifications) {
      setNotifications(false);
      savePref("notifications", false);
      return;
    }
    const granted = await requestNotifications();
    if (!granted) {
      toast("warning", "Notifications blocked", "The browser did not grant permission.");
      return;
    }
    setNotifications(true);
    savePref("notifications", true);
  };

  const sendTest = async () => {
    try {
      await apiClient.testAlert();
      toast("info", "Test alert sent", "LabTestAlert fires through vmalert → Alertmanager in a few seconds.");
    } catch (e) {
      toastError(e);
    }
  };

  const groups = groupAlerts(alerts);
  const unread = new Set(unreadList);
  return (
    <section className="panel alert-panel" onMouseEnter={() => unread.size && markRead()} aria-label="Alerts">
      <header className="panel__head">
        <h2 className="panel__title">
          <Bell size={16} /> Alerts
          {unread.size > 0 && (
            <span className="badge" aria-label={`${unread.size} unread alerts`}>
              {unread.size}
            </span>
          )}
        </h2>
        <div className="panel__actions">
          <button className={`icon-btn ${sound ? "is-on" : ""}`} onClick={toggleSound} aria-pressed={sound} aria-label="Alert sound" title="Alert sound">
            {sound ? <Volume2 size={15} /> : <VolumeX size={15} />}
          </button>
          <button className={`icon-btn ${notifications ? "is-on" : ""}`} onClick={toggleNotifications} aria-pressed={notifications} aria-label="Browser notifications" title="Browser notifications">
            {notifications ? <Bell size={15} /> : <BellOff size={15} />}
          </button>
          <button className="btn btn--ghost btn--sm" onClick={sendTest} title="Fire LabTestAlert">
            <Send size={13} /> Test
          </button>
        </div>
      </header>
      <div className="panel__body alert-panel__body">
        <div className="alert-summary">
          <span className="alert-summary__item alert-summary__item--firing">
            <b className="mono">{groups.firing.length}</b> firing
          </span>
          <span className="alert-summary__item alert-summary__item--pending">
            <b className="mono">{groups.pending.length}</b> pending
          </span>
          <span className="alert-summary__item alert-summary__item--resolved">
            <b className="mono">{groups.resolved.length}</b> resolved
          </span>
        </div>
        {alerts.length === 0 && <Empty>All quiet. No alerts in the last 15 minutes.</Empty>}
        <Group title="Firing" tone="firing" alerts={groups.firing} now={now} unread={unread} />
        <Group title="Pending" tone="pending" alerts={groups.pending} now={now} unread={unread} />
        <Group title="Resolved" tone="resolved" alerts={groups.resolved} now={now} unread={unread} />
      </div>
    </section>
  );
}
