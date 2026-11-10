import { describe, expect, it, vi } from "vitest";
import { Emitter } from "./emitter";
import { CONSUMES, DASHBOARDS, PRODUCER, TOPIC_PARTITIONS, dashboardUrl } from "./topology";

describe("event bus", () => {
  it("shouldStopDeliveringEventsWhenSubscribersUnsubscribe", () => {
    // GIVEN: the live view subscribed to hover events
    const bus = new Emitter<{ hover: string; other: number }>();
    const seen = vi.fn();
    const tooltip = vi.fn();
    const off = bus.on("hover", seen);
    const offTooltip = bus.on("hover", tooltip);
    // WHEN: two hovers are emitted, the view unsubscribes, and a third is emitted
    bus.emit("hover", "payment");
    bus.emit("other", 1);
    bus.emit("hover", "inventory");
    off();
    offTooltip();
    bus.emit("hover", "shipping");
    // THEN: only the first two reached it and no listener is left
    expect(seen.mock.calls).toEqual([["payment"], ["inventory"]]);
    expect(tooltip).toHaveBeenCalledTimes(2);
    expect(bus.listenerCount("hover")).toBe(0);
    expect(bus.listenerCount("other")).toBe(0);
  });
});

describe("topology", () => {
  it("shouldBuildGrafanaDeepLinksWhenADashboardIsOpened", () => {
    // GIVEN: the consumer-lag dashboard and a payment filter
    // WHEN: the link is built against the default and a custom Grafana base
    // THEN: the UID path and template variables are encoded
    expect(dashboardUrl("kafka-lag")).toBe("http://grafana.localhost:8080/d/kafka-lag");
    expect(dashboardUrl("kafka-lag", "http://g/", { consumergroup: "payment" })).toBe("http://g/d/kafka-lag?var-consumergroup=payment");
  });

  it("shouldDescribeTheFrozenEventFlowWhenTheLiveViewIsLaidOut", () => {
    // GIVEN: the contract topology
    // WHEN: producers, consumers and dashboards are inspected
    // THEN: every consumed topic exists, every topic has a producer and all 16 dashboard UIDs are known
    for (const topics of Object.values(CONSUMES)) for (const t of topics) expect(TOPIC_PARTITIONS[t]).toBeGreaterThan(0);
    for (const t of Object.keys(TOPIC_PARTITIONS)) expect(PRODUCER[t]).toBeDefined();
    expect(DASHBOARDS).toHaveLength(16);
  });
});
