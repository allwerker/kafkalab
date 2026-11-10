import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import type { Experiment } from "../api/types";
import { service } from "../test/live";
import { boardStore, createBoardStore, useBoard } from "./store";

describe("board store", () => {
  it("shouldApplyRestResultsImmediatelyWhenTheOperatorActs", () => {
    // GIVEN: a fresh board store with a fixed clock
    const store = createBoardStore(() => 1000);
    const s = store.getState();
    const exp: Experiment = { id: "e", scenario: "s", name: "S", status: "running", params: {}, createdAt: "", startsAt: "", endsAt: "", autoRecover: true, affected: [], observations: [] };
    // WHEN: the operator scales payment, arms chaos, starts an experiment and a drill, and scenarios + timeline load
    s.putService(service("payment", { replicas: 5 }));
    s.setChaosArmed(true);
    s.putExperiment(exp);
    s.setDrill({ id: "d", status: "running", difficulty: "easy", startedAt: "", hintsUsed: 0, hints: [], guesses: [], candidates: [] });
    s.setScenarios([]);
    s.addTimeline([{ id: "t", at: "2026-01-15T10:00:00Z", kind: "config", severity: "info", title: "x" }]);
    s.setConnection("reconnecting", 2000);
    s.receive({ type: "tick", ts: "", data: { kpis: { ordersPerSec: 1, revenuePerMin: 0, paymentSuccessRate: 1, reservationSuccessRate: 1, e2eP95Ms: 0, inFlight: 0, dlqPerMin: 0, totalLag: 0 }, services: [], brokers: [], rates: {}, lag: {}, partitions: {} } });
    // THEN: the board reflects every change right away
    const after = store.getState();
    expect(after.chaosArmed).toBe(true);
    expect(after.experiments).toHaveLength(1);
    expect(after.drill?.id).toBe("d");
    expect(after.timeline).toHaveLength(1);
    expect(after.connection).toBe("reconnecting");
    expect(after.retryInMs).toBe(2000);
    expect(after.kpiHistory[0]!.at).toBe(1000);
  });

  it("shouldClearTheUnreadBadgeWhenAlertsAreMarkedRead", () => {
    // GIVEN: two unread alerts
    const store = createBoardStore();
    store.setState({ unreadAlerts: ["a", "b"] });
    // WHEN: the operator looks at the alert panel
    store.getState().markAlertsRead();
    // THEN: nothing is unread
    expect(store.getState().unreadAlerts).toEqual([]);
  });

  it("shouldRerenderComponentsWhenTheSelectedSliceChanges", () => {
    // GIVEN: a component reading the chaos flag from the app-wide store
    const { result } = renderHook(() => useBoard((b) => b.chaosArmed));
    expect(result.current).toBe(false);
    // WHEN: chaos gets armed
    act(() => boardStore.getState().setChaosArmed(true));
    // THEN: the component sees it
    expect(result.current).toBe(true);
    act(() => boardStore.getState().setChaosArmed(false));
  });
});
