/** Number, money and time formatting for the control room (English locale, monospace-friendly). */

export function fmtInt(n: number): string {
  return Math.round(n).toLocaleString("en");
}

/** 950 → "950", 12_340 → "12.3k", 4_200_000 → "4.2M" */
export function fmtCompact(n: number): string {
  const abs = Math.abs(n);
  if (abs < 1000) return abs < 10 && !Number.isInteger(n) ? n.toFixed(1) : String(Math.round(n));
  if (abs < 1e6) return `${(n / 1e3).toFixed(abs < 1e4 ? 2 : 1)}k`;
  return `${(n / 1e6).toFixed(abs < 1e7 ? 2 : 1)}M`;
}

export function fmtPct(ratio: number, digits = 1): string {
  return `${(ratio * 100).toFixed(digits)}%`;
}

export function fmtEur(n: number): string {
  return `€${fmtCompact(n)}`;
}

/** Duration in seconds → "45s", "3m 05s", "1h 02m" */
export function fmtDuration(totalSec: number): string {
  const s = Math.max(0, Math.round(totalSec));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, "0")}s`;
  return `${Math.floor(s / 3600)}h ${String(Math.floor((s % 3600) / 60)).padStart(2, "0")}m`;
}

/** Milliseconds → "850 ms" or "2.4 s" */
export function fmtMs(ms: number): string {
  return ms < 1000 ? `${Math.round(ms)} ms` : `${(ms / 1000).toFixed(1)} s`;
}

/** Age of an RFC 3339 timestamp relative to `now` (ms) → "12s", "4m 10s". */
export function fmtAge(iso: string, now: number): string {
  return fmtDuration((now - Date.parse(iso)) / 1000);
}

/** RFC 3339 → local "HH:MM:SS" */
export function fmtClock(iso: string): string {
  const d = new Date(iso);
  return [d.getHours(), d.getMinutes(), d.getSeconds()].map((v) => String(v).padStart(2, "0")).join(":");
}

export function shortId(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}

export function humanize(key: string): string {
  const spaced = key.replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/[-_.]/g, " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1).toLowerCase();
}
