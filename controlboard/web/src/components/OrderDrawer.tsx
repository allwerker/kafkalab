import { useEffect, useState } from "react";
import { ExternalLink, Package, ScrollText, Waypoints, X } from "lucide-react";
import { ApiError } from "../api/client";
import type { OrderTimeline } from "../api/types";
import { fmtClock } from "../lib/format";
import { KIND_LABEL } from "../live/model/LiveModel";
import { useApi } from "./context";

export function OrderDrawer({ orderId, onClose }: { orderId: string; onClose: () => void }) {
  const apiClient = useApi();
  const [order, setOrder] = useState<OrderTimeline | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    setOrder(null);
    setError(null);
    apiClient.order(orderId).then(
      (o) => alive && setOrder(o),
      (e: unknown) => alive && setError(e instanceof ApiError && e.status === 404 ? "This order already left the recent-orders ring." : String((e as Error).message)),
    );
    return () => {
      alive = false;
    };
  }, [apiClient, orderId]);

  const value = order?.events.find((e) => e.valueCents !== undefined)?.valueCents;
  const region = order?.events.find((e) => e.region)?.region;
  return (
    <aside className="drawer" role="dialog" aria-label={`Order ${orderId}`}>
      <header className="drawer__head">
        <Package size={18} />
        <div>
          <h3>Order</h3>
          <code className="mono small">{orderId}</code>
        </div>
        <button className="icon-btn" onClick={onClose} aria-label="Close order">
          <X size={16} />
        </button>
      </header>
      {error && <p className="form-error">{error}</p>}
      {!order && !error && <p className="muted">Loading order timeline…</p>}
      {order && (
        <>
          <div className="drawer__facts">
            {value !== undefined && (
              <span>
                <b className="mono">€{(value / 100).toFixed(2)}</b> value
              </span>
            )}
            {region && (
              <span>
                <b>{region}</b> region
              </span>
            )}
            <span>
              <b className="mono">{order.events.length}</b> events
            </span>
          </div>
          <ol className="order-tl">
            {order.events.map((e, i) => (
              <li key={`${e.topic}-${e.offset}-${i}`} className={`order-tl__item order-tl__item--${e.kind}`}>
                <time className="mono">{fmtClock(e.at)}</time>
                <div>
                  <b>{KIND_LABEL[e.kind]}</b>
                  <span className="muted small mono">
                    {" "}
                    {e.topic}[{e.partition}]@{e.offset}
                  </span>
                  {(e.reason || e.carrier) && <div className="small">{e.reason ?? `via ${e.carrier}`}</div>}
                </div>
              </li>
            ))}
          </ol>
          <div className="drawer__links">
            {order.traceUrl && (
              <a className="btn btn--sm" href={order.traceUrl} target="_blank" rel="noreferrer">
                <Waypoints size={13} /> Trace <ExternalLink size={11} />
              </a>
            )}
            {order.logsUrl && (
              <a className="btn btn--sm" href={order.logsUrl} target="_blank" rel="noreferrer">
                <ScrollText size={13} /> Logs <ExternalLink size={11} />
              </a>
            )}
          </div>
        </>
      )}
    </aside>
  );
}
