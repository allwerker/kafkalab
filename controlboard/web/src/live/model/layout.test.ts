import { describe, expect, it } from "vitest";
import { TOPIC_PARTITIONS } from "../../lib/topology";
import { LANE_DEFS, STATION_POS, WORLD, buildLanes, laneId } from "./layout";

describe("layout", () => {
  it("shouldBuildOneLanePerPartitionWhenTheTopicCatalogIsKnown", () => {
    // GIVEN: the frozen topic catalog (orders.created has 6 partitions, payments.failed 3)
    // WHEN: the lanes are built
    const lanes = buildLanes(TOPIC_PARTITIONS);
    // THEN: every bundle gets one lane per partition
    const expected = LANE_DEFS.reduce((n, d) => n + TOPIC_PARTITIONS[d.topic]!, 0);
    expect(lanes).toHaveLength(expected);
    expect(lanes.filter((l) => l.topic === "payments.failed")).toHaveLength(3);
    expect(lanes.find((l) => l.id === laneId("orders.created", "inventory", 5))?.group).toBe("inventory");
  });

  it("shouldFallBackToOneLaneWhenATopicIsMissingFromTheTick", () => {
    // GIVEN: a tick that does not report partition counts at all
    // WHEN: the lanes are built
    const lanes = buildLanes({});
    // THEN: each bundle still has a single lane
    expect(lanes).toHaveLength(LANE_DEFS.length);
  });

  it("shouldKeepAllStationsInsideTheWorldWhenLaidOut", () => {
    // GIVEN: the station layout
    // WHEN: every station position is checked
    // THEN: all lie inside the 1600×900 world
    for (const p of Object.values(STATION_POS)) {
      expect(p.x).toBeGreaterThan(0);
      expect(p.x).toBeLessThan(WORLD.width);
      expect(p.y).toBeGreaterThan(0);
      expect(p.y).toBeLessThan(WORLD.height);
    }
  });
});
