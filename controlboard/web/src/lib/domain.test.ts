import { describe, expect, it } from "vitest";
import type { Alert, Experiment, ScenarioDef, ScenarioParam, TimelineEntry, TuningSetting } from "../api/types";
import { service } from "../test/live";
import {
  categoryMap,
  coerceParam,
  confirmedCount,
  defaultParams,
  experimentProgress,
  filterTimeline,
  groupAlerts,
  groupScenarios,
  isActive,
  isApplying,
  isLossy,
  newlyFiring,
  startBlocker,
} from "./domain";

const alert = (fp: string, status: Alert["status"], severity: Alert["severity"], startsAt: string, endsAt?: string): Alert => ({
  fingerprint: fp,
  name: fp,
  status,
  severity,
  component: "kafka",
  summary: "",
  description: "",
  startsAt,
  labels: {},
  ...(endsAt ? { endsAt } : {}),
});

const exp = (status: Experiment["status"], patch: Partial<Experiment> = {}): Experiment => ({
  id: "e",
  scenario: "black-friday",
  name: "BF",
  status,
  params: {},
  createdAt: "2026-01-15T10:00:00Z",
  startsAt: "2026-01-15T10:00:00Z",
  endsAt: "2026-01-15T10:01:40Z",
  autoRecover: true,
  affected: [],
  observations: [],
  ...patch,
});

const scenario = (id: string, category: ScenarioDef["category"], destructive = false): ScenarioDef => ({
  id,
  name: id,
  description: "",
  category,
  severity: "high",
  destructive,
  defaultDurationSec: 60,
  params: [],
  expectedSymptoms: [],
  dashboards: [],
});

describe("alerts", () => {
  it("shouldGroupAlertsBySeverityAndRecencyWhenShowingThePanel", () => {
    // GIVEN: firing warning + critical, a pending info, and two resolved alerts (one without end time)
    const alerts = [
      alert("warn", "firing", "warning", "2026-01-15T10:00:05Z"),
      alert("crit-old", "firing", "critical", "2026-01-15T10:00:00Z"),
      alert("crit-new", "firing", "critical", "2026-01-15T10:00:09Z"),
      alert("pend", "pending", "info", "2026-01-15T10:00:00Z"),
      alert("res-a", "resolved", "warning", "2026-01-15T09:00:00Z", "2026-01-15T09:30:00Z"),
      alert("res-b", "resolved", "warning", "2026-01-15T09:40:00Z"),
    ];
    // WHEN: they are grouped
    const g = groupAlerts(alerts);
    // THEN: critical alerts come first (newest first), resolved ones sorted by when they ended
    expect(g.firing.map((a) => a.fingerprint)).toEqual(["crit-new", "crit-old", "warn"]);
    expect(g.pending.map((a) => a.fingerprint)).toEqual(["pend"]);
    expect(g.resolved.map((a) => a.fingerprint)).toEqual(["res-b", "res-a"]);
  });

  it("shouldReportOnlyNewlyFiringAlertsWhenTheListChanges", () => {
    // GIVEN: one alert already firing and one pending
    const prev = [alert("a", "firing", "critical", "t"), alert("b", "pending", "warning", "t")];
    // WHEN: the pending one starts firing too
    const next = [alert("a", "firing", "critical", "t"), alert("b", "firing", "warning", "t")];
    // THEN: only it should beep
    expect(newlyFiring(prev, next).map((a) => a.fingerprint)).toEqual(["b"]);
  });
});

describe("experiments", () => {
  const start = Date.parse("2026-01-15T10:00:00Z");

  it("shouldCountDownToTheStartWhenAnExperimentIsScheduled", () => {
    // GIVEN: an experiment scheduled to start in 30 s
    const e = exp("scheduled", { startsAt: "2026-01-15T10:00:30Z" });
    // WHEN: progress is computed now
    // THEN: it waits 30 s and has no progress yet
    expect(experimentProgress(e, start)).toEqual({ fraction: 0, remainingSec: 30, overdue: false });
    expect(experimentProgress(e, start + 60000).remainingSec).toBe(0);
  });

  it("shouldShowProgressAndRemainingTimeWhenAnExperimentRuns", () => {
    // GIVEN: a 100 s experiment that started 25 s ago
    const e = exp("running");
    // WHEN: progress is computed
    // THEN: it is a quarter done with 75 s left
    expect(experimentProgress(e, start + 25000)).toEqual({ fraction: 0.25, remainingSec: 75, overdue: false });
    expect(experimentProgress(e, start - 5000).fraction).toBe(0);
  });

  it("shouldFlagOverdueWhenAManualRecoveryExperimentPassedItsEnd", () => {
    // GIVEN: a running experiment without auto-recover, 10 s past its end
    const e = exp("running", { autoRecover: false });
    // WHEN: progress is computed
    // THEN: it is complete and overdue
    expect(experimentProgress(e, start + 110000)).toEqual({ fraction: 1, remainingSec: 0, overdue: true });
  });

  it("shouldTreatFinishedExperimentsAsCompleteWhenComputingProgress", () => {
    // GIVEN: a recovering experiment
    // WHEN: progress is computed
    // THEN: the bar is full
    expect(experimentProgress(exp("recovering"), start)).toEqual({ fraction: 1, remainingSec: 0, overdue: false });
    expect(isActive(exp("recovering"))).toBe(true);
    expect(isActive(exp("aborted"))).toBe(false);
  });

  it("shouldCountConfirmedObservationsWhenComparingRequestedAndObserved", () => {
    // GIVEN: two observations, one confirmed by metrics
    const e = exp("running", {
      observations: [
        { label: "Broker 1 stopped", requested: "down", observed: "down", confirmed: true, at: "" },
        { label: "URP", requested: "> 0", observed: "0", confirmed: false, at: "" },
      ],
    });
    // WHEN: they are counted
    // THEN: one is confirmed
    expect(confirmedCount(e)).toBe(1);
  });
});

describe("services", () => {
  it("shouldShowApplyingWhenTheServiceHasNotCaughtUpWithTheDesiredGeneration", () => {
    // GIVEN: payment received generation 5 but its pods still run 4
    // WHEN: it is checked
    // THEN: it is applying; once equal it is not
    expect(isApplying(service("payment", { desiredGeneration: 5, appliedGeneration: 4 }))).toBe(true);
    expect(isApplying(service("payment", { desiredGeneration: 5, appliedGeneration: 5 }))).toBe(false);
  });
});

describe("scenarios", () => {
  it("shouldGroupScenariosInCategoryOrderWhenRenderingTheLauncher", () => {
    // GIVEN: scenarios from three categories in random order
    const list = [scenario("crash", "broker"), scenario("bf", "traffic"), scenario("poison", "data"), scenario("flash", "traffic")];
    // WHEN: they are grouped
    const groups = groupScenarios(list);
    // THEN: traffic comes first, empty categories are skipped
    expect(groups.map((g) => [g.category, g.scenarios.map((s) => s.id)])).toEqual([
      ["traffic", ["bf", "flash"]],
      ["broker", ["crash"]],
      ["data", ["poison"]],
    ]);
    expect(categoryMap(list)).toEqual({ crash: "broker", bf: "traffic", poison: "data", flash: "traffic" });
  });

  it("shouldCoerceFormInputsIntoTypedParamsWhenTheOperatorEditsThem", () => {
    // GIVEN: number, broker, boolean and select parameters
    const num: ScenarioParam = { key: "rate", label: "Rate", type: "number", min: 100, max: 2000, default: 1000 };
    const free: ScenarioParam = { key: "x", label: "X", type: "number", default: 3 };
    const brk: ScenarioParam = { key: "broker", label: "Broker", type: "broker", default: 1 };
    const flag: ScenarioParam = { key: "random", label: "Random", type: "boolean", default: false };
    const sel: ScenarioParam = { key: "svc", label: "Service", type: "service", default: "payment" };
    // WHEN: raw inputs are coerced
    // THEN: numbers are clamped, garbage falls back to the default, booleans and strings are typed
    expect(coerceParam(num, "5000")).toBe(2000);
    expect(coerceParam(num, "5")).toBe(100);
    expect(coerceParam(num, "abc")).toBe(1000);
    expect(coerceParam(free, "-7")).toBe(-7);
    expect(coerceParam(brk, "2")).toBe(2);
    expect(coerceParam(flag, true)).toBe(true);
    expect(coerceParam(flag, "true")).toBe(true);
    expect(coerceParam(flag, "false")).toBe(false);
    expect(coerceParam(sel, "inventory")).toBe("inventory");
    expect(defaultParams([num, flag])).toEqual({ rate: 1000, random: false });
  });

  it("shouldBlockDestructiveScenariosUntilArmedAndConfirmedWhenLaunching", () => {
    // GIVEN: a destructive broker crash and a harmless Black Friday
    const crash = scenario("broker-crash", "broker", true);
    // WHEN: the launch is checked in different safety states
    // THEN: it needs armed chaos and the typed id; harmless scenarios are never blocked
    expect(startBlocker(scenario("bf", "traffic"), false, "")).toBeNull();
    expect(startBlocker(crash, false, "broker-crash")).toBe("Arm chaos to run destructive scenarios");
    expect(startBlocker(crash, true, "broker")).toBe('Type "broker-crash" to confirm');
    expect(startBlocker(crash, true, "broker-crash")).toBeNull();
  });
});

describe("tuning and timeline", () => {
  it("shouldFlagLossyProducerSettingsWhenAcksIsNotAll", () => {
    // GIVEN: the acks setting with lossy values, and a setting without any
    const acks = { key: "acks", lossy: ["leader", "none"] } as TuningSetting;
    const linger = { key: "lingerMs" } as TuningSetting;
    // WHEN: values are checked
    // THEN: only weakened guarantees are flagged
    expect(isLossy(acks, "none")).toBe(true);
    expect(isLossy(acks, "all")).toBe(false);
    expect(isLossy(acks, undefined)).toBe(false);
    expect(isLossy(linger, 5)).toBe(false);
  });

  it("shouldFilterTheTimelineByKindAndExperimentWhenTheOperatorNarrowsIt", () => {
    // GIVEN: alert and config entries, one belonging to experiment e1
    const t = (id: string, kind: TimelineEntry["kind"], experimentId?: string): TimelineEntry => ({ id, at: "", kind, severity: "info", title: id, ...(experimentId ? { experimentId } : {}) });
    const entries = [t("a", "alert"), t("c", "config"), t("x", "experiment", "e1")];
    // WHEN: filtering by kinds and by experiment
    // THEN: only matching entries remain
    expect(filterTimeline(entries, new Set(["alert", "experiment"])).map((e) => e.id)).toEqual(["a", "x"]);
    expect(filterTimeline(entries, new Set(["alert", "experiment"]), "e1").map((e) => e.id)).toEqual(["x"]);
  });
});
