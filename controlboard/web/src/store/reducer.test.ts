import { describe, expect, it } from "vitest";
import type { Alert, Experiment, ServerMessage, State, TimelineEntry } from "../api/types";
import { BASE_KPIS, broker, service, tick } from "../test/live";
import { ALERT_CAP, KPI_HISTORY, TIMELINE_CAP, initialBoard, mergeTimeline, reduce, upsertExperiment, upsertService } from "./reducer";

const msg = <T extends ServerMessage["type"]>(type: T, data: Extract<ServerMessage, { type: T }>["data"]) => ({ type, ts: "2026-01-15T10:00:00Z", data }) as ServerMessage;

function alert(fp: string, status: Alert["status"], patch: Partial<Alert> = {}): Alert {
  return { fingerprint: fp, name: "KafkaBrokerDown", status, severity: "critical", component: "kafka", summary: "", description: "", startsAt: "2026-01-15T10:00:00Z", labels: {}, ...patch };
}

function experiment(id: string, createdAt: string, patch: Partial<Experiment> = {}): Experiment {
  return { id, scenario: "black-friday", name: "Black Friday", status: "running", params: {}, createdAt, startsAt: createdAt, endsAt: createdAt, autoRecover: true, affected: [], observations: [], ...patch };
}

function entry(id: string, at: string): TimelineEntry {
  return { id, at, kind: "experiment", severity: "info", title: id };
}

function state(patch: Partial<State> = {}): State {
  return {
    simulation: "running",
    chaosArmed: true,
    services: [service("payment")],
    brokers: [broker(0)],
    kpis: BASE_KPIS,
    experiments: [],
    alerts: [],
    links: [],
    ...patch,
  };
}

describe("board reducer", () => {
  it("shouldComputeTheClockOffsetWhenTheServerSaysHello", () => {
    // GIVEN: the server clock is 2 s ahead of the browser
    const now = Date.parse("2026-01-15T10:00:00Z");
    // WHEN: hello arrives
    const s = reduce(initialBoard(), msg("hello", { version: 1, serverTime: "2026-01-15T10:00:02Z" }), now);
    // THEN: countdowns will use a +2 s offset
    expect(s.clockOffsetMs).toBe(2000);
  });

  it("shouldResyncEverythingWhenAFullStateArrives", () => {
    // GIVEN: a board that had unread alerts, one of which has since resolved
    const before = { ...initialBoard(), unreadAlerts: ["still-firing", "gone"] };
    const s0 = state({
      alerts: [alert("still-firing", "firing")],
      experiments: [experiment("old", "2026-01-15T09:00:00Z"), experiment("new", "2026-01-15T10:00:00Z")],
      drill: { id: "d1", status: "running", difficulty: "easy", startedAt: "", hintsUsed: 0, hints: [], guesses: [], candidates: [] },
    });
    // WHEN: the periodic state message arrives
    const s = reduce(before, msg("state", s0), 0);
    // THEN: the board mirrors the server, sorts experiments newest first and keeps only still-firing unread alerts
    expect(s.hasState).toBe(true);
    expect(s.chaosArmed).toBe(true);
    expect(s.experiments.map((e) => e.id)).toEqual(["new", "old"]);
    expect(s.unreadAlerts).toEqual(["still-firing"]);
    expect(s.drill?.id).toBe("d1");
  });

  it("shouldRecordKpiHistoryAndLagWhenTicksArrive", () => {
    // GIVEN: an empty board
    let s = initialBoard();
    // WHEN: more ticks than the history keeps arrive
    for (let i = 0; i < KPI_HISTORY + 5; i++) s = reduce(s, msg("tick", tick({ kpis: { ...BASE_KPIS, ordersPerSec: i } })), i * 1000);
    // THEN: only the newest samples are kept and the latest lag and rates are visible
    expect(s.kpiHistory).toHaveLength(KPI_HISTORY);
    expect(s.kpiHistory.at(-1)!.kpis.ordersPerSec).toBe(KPI_HISTORY + 4);
    expect(s.kpis?.ordersPerSec).toBe(KPI_HISTORY + 4);
    expect(s.lag.payment).toBeDefined();
    expect(s.partitions["orders.created"]).toBe(6);
  });

  it("shouldOnlyChangeStateWhenTheEventScaleChanges", () => {
    // GIVEN: a board at scale 1
    const s = initialBoard();
    // WHEN: an events batch with the same scale and one with a new scale arrive
    const same = reduce(s, msg("events", { scale: 1, items: [] }), 0);
    const changed = reduce(s, msg("events", { scale: 40, items: [] }), 0);
    // THEN: the first is a no-op, the second updates the scale label
    expect(same).toBe(s);
    expect(changed.eventScale).toBe(40);
  });

  it("shouldMarkAnAlertUnreadWhenItStartsFiring", () => {
    // GIVEN: a pending broker alert
    let s = reduce(initialBoard(), msg("alert", alert("b1", "pending")), 0);
    expect(s.unreadAlerts).toEqual([]);
    // WHEN: it fires, and the same firing update is received twice
    s = reduce(s, msg("alert", alert("b1", "firing")), 0);
    s = reduce(s, msg("alert", alert("b1", "firing", { summary: "updated" })), 0);
    // THEN: it is unread once, updated in place
    expect(s.unreadAlerts).toEqual(["b1"]);
    expect(s.alerts).toHaveLength(1);
    expect(s.alerts[0]!.summary).toBe("updated");
  });

  it("shouldClearUnreadWhenTheAlertResolves", () => {
    // GIVEN: an unread firing alert
    let s = reduce(initialBoard(), msg("alert", alert("b1", "firing")), 0);
    // WHEN: it resolves
    s = reduce(s, msg("alert", alert("b1", "resolved")), 0);
    // THEN: the badge no longer counts it
    expect(s.unreadAlerts).toEqual([]);
  });

  it("shouldNotDuplicateUnreadWhenAnUnreadAlertFiresAgainAfterAResync", () => {
    // GIVEN: an alert known as unread but whose stored status is pending (e.g. after a resync race)
    const s0 = { ...initialBoard(), unreadAlerts: ["x"], alerts: [alert("x", "pending")] };
    // WHEN: the firing update arrives
    const s = reduce(s0, msg("alert", alert("x", "firing")), 0);
    // THEN: it is still listed once
    expect(s.unreadAlerts).toEqual(["x"]);
  });

  it("shouldCapTheAlertListWhenAnAlertStormHits", () => {
    // GIVEN: a board already holding the maximum number of alerts
    let s = initialBoard();
    for (let i = 0; i < ALERT_CAP; i++) s = reduce(s, msg("alert", alert(`a${i}`, "resolved")), 0);
    // WHEN: one more alert arrives
    s = reduce(s, msg("alert", alert("new", "pending")), 0);
    // THEN: the newest is first and the list stays capped
    expect(s.alerts).toHaveLength(ALERT_CAP);
    expect(s.alerts[0]!.fingerprint).toBe("new");
  });

  it("shouldUpsertExperimentsAndTimelineAndDrillWhenUpdatesArrive", () => {
    // GIVEN: a board with one running experiment
    let s = reduce(initialBoard(), msg("experiment", experiment("e1", "2026-01-15T10:00:00Z")), 0);
    // WHEN: it completes, a timeline entry arrives twice and the drill changes
    s = reduce(s, msg("experiment", experiment("e1", "2026-01-15T10:00:00Z", { status: "completed" })), 0);
    s = reduce(s, msg("timeline", entry("t1", "2026-01-15T10:00:01Z")), 0);
    s = reduce(s, msg("timeline", entry("t1", "2026-01-15T10:00:01Z")), 0);
    s = reduce(s, msg("drill", { id: "d", status: "solved", difficulty: "hard", startedAt: "", hintsUsed: 1, hints: [], guesses: [], candidates: [] }), 0);
    // THEN: everything is updated without duplicates
    expect(s.experiments).toHaveLength(1);
    expect(s.experiments[0]!.status).toBe("completed");
    expect(s.timeline.map((t) => t.id)).toEqual(["t1"]);
    expect(s.drill?.status).toBe("solved");
  });
});

describe("list helpers", () => {
  it("shouldSortTheTimelineNewestFirstAndCapItWhenMerging", () => {
    // GIVEN: an existing timeline and a REST page that overlaps it
    const existing = [entry("b", "2026-01-15T10:00:02Z")];
    const incoming = [entry("a", "2026-01-15T10:00:01Z"), entry("b", "2026-01-15T10:00:02Z"), entry("c", "2026-01-15T10:00:02Z")];
    // WHEN: they are merged
    const merged = mergeTimeline(existing, incoming);
    // THEN: duplicates are dropped and same-second entries are ordered by id, newest first
    expect(merged.map((t) => t.id)).toEqual(["c", "b", "a"]);
    const many = mergeTimeline([], Array.from({ length: TIMELINE_CAP + 10 }, (_, i) => entry(`e${String(i).padStart(4, "0")}`, "2026-01-15T10:00:00Z")));
    expect(many).toHaveLength(TIMELINE_CAP);
  });

  it("shouldReplaceOrAppendServicesWhenARestResponseArrives", () => {
    // GIVEN: a board knowing payment
    const list = [service("payment")];
    // WHEN: payment is scaled and inventory is reported for the first time
    const scaled = upsertService(list, service("payment", { replicas: 4 }));
    const added = upsertService(list, service("inventory"));
    // THEN: payment is replaced in place and inventory appended
    expect(scaled[0]!.replicas).toBe(4);
    expect(added.map((s) => s.name)).toEqual(["payment", "inventory"]);
  });

  it("shouldKeepExperimentsNewestFirstWhenANewOneIsStarted", () => {
    // GIVEN: an older experiment
    const list = [experiment("old", "2026-01-15T09:00:00Z")];
    // WHEN: a newer one is added
    // THEN: it comes first
    expect(upsertExperiment(list, experiment("new", "2026-01-15T10:00:00Z")).map((e) => e.id)).toEqual(["new", "old"]);
  });
});
