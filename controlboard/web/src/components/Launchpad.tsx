import { useEffect, useState } from "react";
import { ArrowUpRight, LayoutDashboard } from "lucide-react";
import type { Link } from "../api/types";
import { DASHBOARDS, GRAFANA_BASE, dashboardUrl } from "../lib/topology";
import { useBoard } from "../store/store";
import { useApi } from "./context";
import { Empty, HealthDot, NamedIcon } from "./ui";

export function Launchpad() {
  const apiClient = useApi();
  const stateLinks = useBoard((b) => b.links);
  const [fetched, setFetched] = useState<Link[] | null>(null);
  useEffect(() => {
    apiClient.links().then(setFetched, () => undefined);
  }, [apiClient]);
  const links = fetched ?? stateLinks;
  const grafana = links.find((l) => l.id === "grafana")?.url ?? GRAFANA_BASE;
  const folders = ["Kafka Lab", "Kafka (bundle)"] as const;
  return (
    <div className="launchpad">
      {links.length === 0 && <Empty>No links yet.</Empty>}
      <div className="link-grid">
        {links.map((l) => (
          <a key={l.id} className={`link-card link-card--${l.health}`} href={l.url} target="_blank" rel="noreferrer" data-testid={`link-${l.id}`}>
            <span className="link-card__icon">
              <NamedIcon name={l.icon} size={20} />
            </span>
            <span className="link-card__text">
              <b>
                {l.title} <ArrowUpRight size={13} />
              </b>
              <span>{l.description}</span>
            </span>
            <HealthDot health={l.health} label={`${l.title} ${l.health}`} />
          </a>
        ))}
      </div>
      {folders.map((folder) => (
        <section key={folder} className="dash-folder">
          <h3 className="subhead">
            <LayoutDashboard size={14} /> Grafana · {folder}
          </h3>
          <div className="dash-grid">
            {DASHBOARDS.filter((d) => d.folder === folder).map((d) => (
              <a key={d.uid} className="dash-link" href={dashboardUrl(d.uid, grafana)} target="_blank" rel="noreferrer">
                {d.title}
                <span className="mono">{d.uid}</span>
              </a>
            ))}
          </div>
        </section>
      ))}
    </div>
  );
}
