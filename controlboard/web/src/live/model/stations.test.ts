import { describe, expect, it } from "vitest";
import type { Experiment } from "../../api/types";
import { service } from "../../test/live";
import { stationRate, stationStatus, weatherFor } from "./stations";

function exp(id: string, scenario: string, affected: string[], status: Experiment["status"] = "running"): Experiment {
  return { id, scenario, name: id, status, params: {}, createdAt: "", startsAt: "", endsAt: "", autoRecover: true, affected, observations: [] };
}

describe("station status", () => {
  it("shouldPreferDownOverPausedWhenAPausedServiceHasNoReplicas", () => {
    // GIVEN: a paused service that was also scaled to zero
    const s = service("payment", { paused: true, replicas: 0, readyReplicas: 0 });
    // WHEN: its station status is derived
    // THEN: it is shown as down
    expect(stationStatus(s)).toBe("down");
  });

  it("shouldShowDownWhenHealthIsDownEvenWithReadyPods", () => {
    // GIVEN: a service reporting health down while pods are ready
    // WHEN: its status is derived
    // THEN: it is down
    expect(stationStatus(service("payment", { health: "down" }))).toBe("down");
  });

  it("shouldShowUnknownWhenTheServiceIsNotReported", () => {
    // GIVEN: no state for the service
    // WHEN: its status is derived
    // THEN: it is unknown
    expect(stationStatus(undefined)).toBe("unknown");
  });
});

describe("station throughput", () => {
  it("shouldSumAllProducedTopicsWhenComputingAStationsRate", () => {
    // GIVEN: payment produced 40 completed, 3 failed and 1 refunded records per second
    const rates = { "payments.completed": 40, "payments.failed": 3, "payments.refunded": 1, "orders.created": 50 };
    // WHEN: the rates are aggregated
    // THEN: payment shows 44/s; analytics (no output) shows everything it reads; unreported topics count as 0
    expect(stationRate("payment", rates)).toBe(44);
    expect(stationRate("analytics", rates)).toBe(94);
    expect(stationRate("shipping", rates)).toBe(0);
  });
});

describe("chaos weather", () => {
  it("shouldPickWeatherByScenarioCategoryWhenExperimentsRun", () => {
    // GIVEN: one running experiment per category, plus a scheduled one
    const cats = { bf: "traffic", net: "network", brk: "broker", svc: "service", k8s: "kubernetes", poison: "data" } as const;
    const exps = [
      exp("1", "bf", ["order-generator"]),
      exp("2", "net", ["payment"]),
      exp("3", "brk", ["broker-1"]),
      exp("4", "svc", ["inventory"]),
      exp("5", "k8s", ["shipping"]),
      exp("6", "poison", ["order-generator"]),
      exp("7", "bf", [], "scheduled"),
    ];
    // WHEN: the weather is derived
    const w = weatherFor(exps, cats);
    // THEN: traffic rushes at the factory, network glitches, brokers get lightning, services smoke, data rains matrix
    expect(w.map((x) => [x.kind, x.targets])).toEqual([
      ["rush", ["order-generator"]],
      ["glitch", ["payment"]],
      ["lightning", ["broker-1"]],
      ["smoke", ["inventory"]],
      ["smoke", ["shipping"]],
      ["matrix", []],
    ]);
  });

  it("shouldShowFogWhenADrillHidesTheScenario", () => {
    // GIVEN: an unsolved incident drill whose scenario is "hidden"
    // WHEN: the weather is derived
    const w = weatherFor([exp("d", "hidden", [])], {});
    // THEN: only a soft fog hints that something is going on
    expect(w).toEqual([{ kind: "fog", targets: [], intensity: 0.6, experimentId: "d" }]);
  });
});
