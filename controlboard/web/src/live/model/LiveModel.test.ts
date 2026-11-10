import { describe, expect, it } from "vitest";
import { batch, broker, event, service, tick, withLag } from "../../test/live";
import { COLORS, DLQ_POS, STATION_POS, TRUCK_DOCK, WORLD } from "./layout";
import { BATCH_SPREAD_MS, LiveModel, MAX_EFFECTS, MAX_TRUCKS, PILE_CAP, TRUCK_CAPACITY, TRUCK_MAX_LOADING_MS } from "./LiveModel";
import type { Experiment } from "../../api/types";

const fixed = () => 0.5;

function model(maxParcels?: number) {
  return new LiveModel(maxParcels === undefined ? { random: fixed } : { random: fixed, maxParcels });
}

/** Lets the model run for `ms` in 16 ms frames. */
function run(m: LiveModel, ms: number) {
  for (let t = 0; t < ms; t += 16) m.step(16);
}

function experiment(patch: Partial<Experiment>): Experiment {
  return {
    id: "exp-1",
    scenario: "black-friday",
    name: "Black Friday",
    status: "running",
    params: {},
    createdAt: "2026-01-15T10:00:00Z",
    startsAt: "2026-01-15T10:00:00Z",
    endsAt: "2026-01-15T10:03:00Z",
    autoRecover: true,
    affected: [],
    observations: [],
    ...patch,
  };
}

describe("stations", () => {
  it("shouldShowEveryStationAsUnknownWhenNoTickArrivedYet", () => {
    // GIVEN: the board just opened and the backend has not sent a tick yet
    const m = model();
    // WHEN: the scene asks for its stations
    const statuses = m.stations.map((s) => s.status);
    // THEN: all six stations are drawn in the neutral "unknown" state
    expect(statuses).toEqual(["unknown", "unknown", "unknown", "unknown", "unknown", "unknown"]);
    expect(m.hasTick).toBe(false);
  });

  it("shouldReflectServiceHealthWhenTickReportsPausedDownAndDegradedServices", () => {
    // GIVEN: payment is paused, inventory has no ready pod, shipping is degraded and analytics reports unknown health
    const m = model();
    const t = tick({
      services: [
        service("order-generator"),
        service("payment", { paused: true }),
        service("inventory", { readyReplicas: 0 }),
        service("shipping", { health: "degraded" }),
        service("notification", { desiredGeneration: 3, appliedGeneration: 2 }),
        service("analytics", { health: "unknown" }),
      ],
    });
    // WHEN: the tick arrives
    m.applyTick(t);
    // THEN: each station shows the matching visual state, and notification is still applying its new config
    expect(m.station("order-generator").status).toBe("ok");
    expect(m.station("payment").status).toBe("paused");
    expect(m.station("inventory").status).toBe("down");
    expect(m.station("shipping").status).toBe("degraded");
    expect(m.station("analytics").status).toBe("unknown");
    expect(m.station("notification").applying).toBe(true);
    expect(m.station("payment").applying).toBe(false);
  });

  it("shouldShowStationAsUnknownWhenTheServiceIsMissingFromTheTick", () => {
    // GIVEN: a tick that only reports the order generator
    const m = model();
    // WHEN: it is applied
    m.applyTick(tick({ services: [service("order-generator")] }));
    // THEN: the other stations stay unknown with zero replicas
    expect(m.station("payment").status).toBe("unknown");
    expect(m.station("payment").replicas).toBe(0);
    expect(m.station("payment").ready).toBe(0);
  });

  it("shouldSumConsumerLagAndThroughputPerStationWhenTickArrives", () => {
    // GIVEN: payment has 120 records of lag on orders.created and 30 on orders.cancelled, producing 47 records/s
    const m = model();
    const lag = withLag("payment", "orders.created", 1, 120);
    lag.payment!["orders.cancelled"]![0] = 30;
    // WHEN: the tick arrives
    m.applyTick(tick({ lag, rates: { "payments.completed": 45, "payments.failed": 2 } }));
    // THEN: the payment station shows 150 lag and 47/s, the generator has no lag of its own
    expect(m.station("payment").lag).toBe(150);
    expect(m.station("payment").rate).toBe(47);
    expect(m.station("order-generator").lag).toBe(0);
  });

  it("shouldKeepTheLastSixtyOrderRatesForTheAnalyticsScreenWhenManyTicksArrive", () => {
    // GIVEN: seventy seconds of ticks with a rising order rate
    const m = model();
    // WHEN: they are applied one by one
    for (let i = 1; i <= 70; i++) m.applyTick(tick({ kpis: { ...tick().kpis, ordersPerSec: i, revenuePerMin: i * 10 } }));
    // THEN: the big screen keeps only the last minute and shows the latest values
    expect(m.analytics.history).toHaveLength(60);
    expect(m.analytics.history[0]).toBe(11);
    expect(m.analytics.ordersPerSec).toBe(70);
    expect(m.analytics.revenuePerMin).toBe(700);
  });
});

describe("lag piles", () => {
  it("shouldQueueParcelsInFrontOfPaymentWhenThePaymentConsumerHasLag", () => {
    // GIVEN: one parcel stands for 10 orders and payment lags 35 records on partition 2 of orders.created
    const m = model();
    m.applyEvents(batch([], 10));
    // WHEN: the tick with that lag arrives
    m.applyTick(tick({ lag: withLag("payment", "orders.created", 2, 35) }));
    // THEN: four parcels pile up on that partition's lane in front of payment, the other lanes stay empty
    const lane = m.lane("orders.created", "payment", 2)!;
    expect(lane.pile).toBe(4);
    expect(lane.overflow).toBe(false);
    expect(m.lane("orders.created", "payment", 1)!.pile).toBe(0);
    expect(m.lane("orders.created", "inventory", 2)!.pile).toBe(0);
    const slots = m.pileSlots(lane);
    expect(slots).toHaveLength(4);
    const end = lane.path.points[lane.path.points.length - 1]!;
    expect(Math.hypot(slots[0]!.x - end.x, slots[0]!.y - end.y)).toBeLessThan(6);
  });

  it("shouldCapThePileAndFlagOverflowWhenLagExceedsWhatTheLaneCanShow", () => {
    // GIVEN: a Black Friday backlog of 25,000 records on one partition at 1 parcel = 1 order
    const m = model();
    // WHEN: the tick arrives
    m.applyTick(tick({ lag: withLag("inventory", "orders.created", 0, 25000) }));
    // THEN: the pile stops at the cap and is flagged as overflowing
    const lane = m.lane("orders.created", "inventory", 0)!;
    expect(lane.pile).toBe(PILE_CAP);
    expect(lane.overflow).toBe(true);
  });

  it("shouldShrinkPilesWhenTheSamplingScaleGrows", () => {
    // GIVEN: shipping lags 40 records on payments.completed partition 0 at 1 parcel = 1 order
    const m = model();
    m.applyTick(tick({ lag: withLag("shipping", "payments.completed", 0, 40) }));
    expect(m.lane("payments.completed", "shipping", 0)!.pile).toBe(PILE_CAP);
    // WHEN: traffic rises and one parcel now represents 20 orders
    m.applyEvents(batch([], 20));
    // THEN: the same lag is drawn as two parcels and the legend says so
    expect(m.lane("payments.completed", "shipping", 0)!.pile).toBe(2);
    expect(m.scaleLabel).toBe("1 parcel = 20 orders");
  });

  it("shouldUseSingularLabelWhenOneParcelIsOneOrder", () => {
    // GIVEN: quiet traffic where every order is drawn
    const m = model();
    // WHEN: an events batch with scale 1 arrives (a nonsensical scale of 0 is treated as 1)
    m.applyEvents(batch([], 0));
    // THEN: the legend uses the singular
    expect(m.scaleLabel).toBe("1 parcel = 1 order");
  });

  it("shouldTreatMissingLagEntriesAsNoLagWhenTheBackendOmitsAGroup", () => {
    // GIVEN: a tick whose lag map lacks the notification group entirely
    const m = model();
    const t = tick();
    delete t.lag.notification;
    // WHEN: it is applied
    m.applyTick(t);
    // THEN: notification lanes show no pile and the station no lag
    expect(m.lane("orders.shipped", "notification", 0)!.pile).toBe(0);
    expect(m.station("notification").lag).toBe(0);
  });
});

describe("parcels", () => {
  it("shouldSpreadABatchOverAQuarterSecondWhenEventsArrive", () => {
    // GIVEN: two orders created in the same 250 ms batch
    const m = model();
    m.applyEvents(batch([event("created", { partition: 3 }), event("created", { partition: 4 })]));
    // WHEN: the first frame runs
    m.step(1);
    // THEN: only the first order has left the factory; the second follows within the batch window
    expect(m.parcels.size).toBe(2);
    run(m, BATCH_SPREAD_MS);
    expect(m.parcels.size).toBe(4);
  });

  it("shouldSendACreatedOrderToPaymentAndInventoryWhenItIsCreated", () => {
    // GIVEN: an order created on partition 3
    const m = model();
    // WHEN: the event is played
    m.applyEvents(batch([event("created", { partition: 3, orderId: "o-1" })]));
    m.step(1);
    // THEN: a cyan parcel starts on partition 3 towards payment and one towards inventory, and the factory flashes
    const lanes = m.parcels.live.map((p) => p.laneId).sort();
    expect(lanes).toEqual(["orders.created>inventory#3", "orders.created>payment#3"]);
    expect(m.parcels.live.every((p) => p.color === COLORS.cyan && p.orderId === "o-1")).toBe(true);
    expect(m.station("order-generator").pulse).toBeGreaterThan(0.9);
    expect(m.lane("orders.created", "payment", 3)!.activity).toBeGreaterThan(0.95);
  });

  it("shouldStopParcelsAtTheEndOfTheQueueAndAbsorbThemWhenTheyArrive", () => {
    // GIVEN: payment lags on partition 0 so that five parcels already wait in front of it
    const m = model();
    m.applyTick(tick({ lag: withLag("payment", "orders.created", 0, 5) }));
    m.applyEvents(batch([event("created", { partition: 0 })]));
    m.step(1);
    const parcel = m.parcels.live.find((p) => p.laneId === "orders.created>payment#0")!;
    const lane = m.lane("orders.created", "payment", 0)!;
    // WHEN: the parcel travels long enough to reach the queue
    run(m, 4000);
    // THEN: it never passes the queue tail and is absorbed into the pile
    expect(parcel.t).toBeLessThanOrEqual(m.pileTail(lane));
    expect(m.parcels.live).not.toContain(parcel);
  });

  it("shouldDrawPaymentOutcomesWhenOrdersArePaidOrDeclined", () => {
    // GIVEN: one paid and one declined order
    const m = model();
    // WHEN: both events are played
    m.applyEvents(batch([event("paid", { topic: "payments.completed", partition: 2 }), event("payment-failed", { topic: "payments.failed", partition: 1 })]));
    run(m, 300);
    // THEN: a gold coin sparkles and a gold parcel heads to shipping; the decline bursts red on its own lane
    const kinds = m.effects.live.map((e) => e.kind).sort();
    expect(kinds).toEqual(["burst", "coin"]);
    const lanes = m.parcels.live.map((p) => p.laneId).sort();
    expect(lanes).toEqual(["payments.completed>shipping#2", "payments.failed>shipping#1"]);
    expect(m.parcels.live.find((p) => p.kind === "paid")!.color).toBe(COLORS.gold);
  });

  it("shouldDrawInventoryOutcomesWhenStockIsReservedRejectedOrReleased", () => {
    // GIVEN: a reservation, a rejection and a release after a cancellation
    const m = model();
    // WHEN: the events are played
    m.applyEvents(batch([event("reserved", { topic: "inventory.reserved" }), event("rejected", { topic: "inventory.rejected" }), event("released", { topic: "inventory.released" })]));
    run(m, 300);
    // THEN: green and orange parcels travel to shipping, a cross marks the rejection and the release floats a box back
    expect(m.parcels.live.map((p) => p.kind).sort()).toEqual(["rejected", "reserved"]);
    expect(m.effects.live.map((e) => e.kind).sort()).toEqual(["cross", "release"]);
  });

  it("shouldFadeCancelledOrdersWhenShippingCancelsThem", () => {
    // GIVEN: shipping cancels an order
    const m = model();
    m.applyEvents(batch([event("cancelled", { topic: "orders.cancelled", partition: 2, reason: "PAYMENT_FAILED" })]));
    m.step(1);
    const parcel = m.parcels.live[0]!;
    // WHEN: the grey parcel travels towards notification
    run(m, 800);
    // THEN: it is grey and fades as it goes
    expect(parcel.color).toBe(COLORS.grey);
    expect(parcel.fade).toBe(true);
    expect(parcel.alpha).toBeLessThan(1);
  });

  it("shouldFlyACoinBackToTheCustomerWhenAPaymentIsRefunded", () => {
    // GIVEN: a refunded payment
    const m = model();
    // WHEN: it is played
    m.applyEvents(batch([event("refunded", { topic: "payments.refunded" })]));
    m.step(1);
    // THEN: a coin flies from payment back to the order generator and a refund parcel goes to notification
    const coin = m.effects.live.find((e) => e.kind === "coinBack")!;
    expect({ x: coin.x, y: coin.y }).toEqual(STATION_POS.payment);
    expect({ x: coin.tx, y: coin.ty }).toEqual(STATION_POS["order-generator"]);
    expect(m.parcels.live[0]!.laneId).toBe("payments.refunded>notification#0");
  });

  it("shouldSendARadioWaveWhenACustomerIsNotified", () => {
    // GIVEN: a notification was sent
    const m = model();
    // WHEN: it is played
    m.applyEvents(batch([event("notified", { topic: "notifications.sent", partition: 5 })]));
    m.step(1);
    // THEN: the antenna emits a wave and analytics receives the record
    expect(m.effects.live[0]!.kind).toBe("wave");
    expect(m.parcels.live[0]!.laneId).toBe("notifications.sent>analytics#5");
  });

  it("shouldDropAPoisonRecordIntoTheDlqPitWhenItIsDeadLettered", () => {
    // GIVEN: payment dead-letters an undecodable record
    const m = model();
    m.applyEvents(batch([event("dlq", { topic: "orders.dlq", reason: "DECODE_ERROR" })]));
    m.step(1);
    const parcel = m.parcels.live[0]!;
    expect(parcel.state).toBe("falling");
    // WHEN: the record has fallen for a second
    run(m, 1000);
    // THEN: it landed in the pit, the counter went up and a red splash marks the impact
    expect(m.dlqCount).toBe(1);
    expect(m.parcels.size).toBe(0);
    const splash = m.effects.live.find((e) => e.kind === "splash")!;
    expect({ x: splash.x, y: splash.y }).toEqual(DLQ_POS);
  });

  it("shouldWrapUnknownPartitionsOntoExistingLanesWhenTheEventPartitionIsOutOfRange", () => {
    // GIVEN: an order event on partition 7 of a six-partition topic
    const m = model();
    // WHEN: it is played
    m.applyEvents(batch([event("created", { partition: 7 })]));
    m.step(1);
    // THEN: it rides partition 1
    expect(m.parcels.live[0]!.laneId).toBe("orders.created>payment#1");
  });

  it("shouldDropNewParcelsAndCountThemWhenTheParcelCapIsReached", () => {
    // GIVEN: a scene that can show at most three parcels
    const m = model(3);
    // WHEN: two orders are created (four parcels) and a record is dead-lettered
    m.applyEvents(batch([event("created"), event("created"), event("dlq")]));
    run(m, 300);
    // THEN: three parcels are shown and two were dropped
    expect(m.parcels.size).toBe(3);
    expect(m.dropped).toBe(2);
  });

  it("shouldRemoveParcelsWhoseLaneDisappearedWhenPartitionsChange", () => {
    // GIVEN: a parcel travelling on partition 5 of orders.created
    const m = model();
    m.applyEvents(batch([event("created", { partition: 5 })]));
    m.step(1);
    expect(m.parcels.size).toBe(2);
    // WHEN: the topic is shrunk to two partitions
    m.applyTick(tick({ partitions: { ...tick().partitions, "orders.created": 2 } }));
    m.step(16);
    // THEN: the lanes are rebuilt and the orphaned parcels vanish
    expect(m.lanes.filter((l) => l.topic === "orders.created" && l.to === "payment")).toHaveLength(2);
    expect(m.parcels.size).toBe(0);
  });

  it("shouldKeepLagWhenTheSamePartitionLayoutArrivesAgain", () => {
    // GIVEN: payment lags on partition 0
    const m = model();
    m.applyTick(tick({ lag: withLag("payment", "orders.created", 0, 3) }));
    const lanes = m.lanes;
    // WHEN: a tick with an identical partition layout arrives
    m.applyTick(tick({ lag: withLag("payment", "orders.created", 0, 3) }));
    // THEN: the lanes are reused, not rebuilt
    expect(m.lanes).toBe(lanes);
  });
});

describe("trucks", () => {
  it("shouldLoadATruckAndDepartWhenItIsFull", () => {
    // GIVEN: an empty loading dock
    const m = model();
    // WHEN: three orders ship in quick succession
    m.applyEvents(batch([event("shipped", { carrier: "DHL" }), event("shipped"), event("shipped")]));
    run(m, 300);
    // THEN: one truck carries all three and drives off to the right
    expect(m.trucks.size).toBe(1);
    const truck = m.trucks.live[0]!;
    expect(truck.load).toBe(TRUCK_CAPACITY);
    expect(truck.carrier).toBe("DHL");
    expect(truck.state).toBe("departing");
    run(m, 500);
    expect(truck.x).toBeGreaterThan(TRUCK_DOCK.x);
  });

  it("shouldDepartHalfEmptyWhenNoMoreShipmentsArriveForAWhile", () => {
    // GIVEN: a single shipment without a carrier name
    const m = model();
    m.applyEvents(batch([event("shipped")]));
    m.step(1);
    expect(m.trucks.live[0]!.carrier).toBe("");
    // WHEN: no further shipments arrive for longer than the loading window
    run(m, TRUCK_MAX_LOADING_MS + 50);
    // THEN: the truck leaves with one parcel, and later disappears off-screen
    expect(m.trucks.live[0]!.state).toBe("departing");
    run(m, 8000);
    expect(m.trucks.size).toBe(0);
  });

  it("shouldNotSpawnMoreTrucksWhenTheFleetIsOnTheRoad", () => {
    // GIVEN: enough shipments to fill every truck
    const m = model();
    // WHEN: dozens of orders ship within a quarter of a second
    m.applyEvents(batch(Array.from({ length: 6 * MAX_TRUCKS }, () => event("shipped"))));
    run(m, 260);
    // THEN: the fleet never exceeds its size
    expect(m.trucks.size).toBe(MAX_TRUCKS);
  });
});

describe("effects", () => {
  it("shouldExpireEffectsAndCapTheirNumberWhenAPaymentStormHits", () => {
    // GIVEN: far more successful payments in one frame than effects the scene can hold
    const m = model();
    const items = Array.from({ length: MAX_EFFECTS + 10 }, () => event("paid", { topic: "payments.completed" }));
    // WHEN: they all play at once
    m.applyEvents({ scale: 1, items });
    run(m, 300);
    // THEN: the number of effects is capped, and they fade away after their lifetime
    expect(m.effects.size).toBe(MAX_EFFECTS);
    run(m, 1000);
    expect(m.effects.size).toBe(0);
  });

  it("shouldDecayStationPulseAndLaneActivityWhenTrafficStops", () => {
    // GIVEN: an order just left the factory
    const m = model();
    m.applyEvents(batch([event("created")]));
    m.step(1);
    // WHEN: two quiet seconds pass
    run(m, 2000);
    // THEN: the factory stops flashing and the lane glow is gone
    expect(m.station("order-generator").pulse).toBe(0);
    expect(m.lane("orders.created", "payment", 0)!.activity).toBe(0);
  });

  it("shouldClampLongFramesWhenTheTabWasInTheBackground", () => {
    // GIVEN: a fresh scene
    const m = model();
    // WHEN: a 5 s frame and a negative frame are reported
    m.step(5000);
    m.step(-20);
    // THEN: the model only advances by one capped step
    expect(m.time).toBe(250);
  });

  it("shouldUseMathRandomWhenNoRandomSourceIsInjected", () => {
    // GIVEN: a model created with default options
    const m = new LiveModel();
    // WHEN: an order is created
    m.applyEvents(batch([event("created")]));
    m.step(1);
    // THEN: its parcel gets a randomised but bounded speed
    const p = m.parcels.live[0]!;
    expect(p.speed).toBeGreaterThan(0);
  });
});

describe("brokers", () => {
  it("shouldShowADarkBrokerWithSparksWhenABrokerGoesDown", () => {
    // GIVEN: a healthy three-broker cluster
    const m = model();
    m.applyTick(tick());
    expect(m.brokers[1]!.downForMs).toBe(-1);
    // WHEN: broker 1 crashes and the survivors report under-replicated partitions
    m.applyTick(tick({ brokers: [broker(0, { leaderCount: 25, underReplicated: 25 }), broker(1, { up: false, leaderCount: 0 }), broker(2, { leaderCount: 25, underReplicated: 25 })] }));
    run(m, 500);
    // THEN: broker 1 is down and counting, the survivors glow with their URPs and broker 0 keeps the crown
    expect(m.brokers[1]!.up).toBe(false);
    expect(m.brokers[1]!.downForMs).toBeGreaterThanOrEqual(480);
    expect(m.brokers[0]!.underReplicated).toBe(25);
    expect(m.brokers[0]!.controller).toBe(true);
    // WHEN: it comes back
    m.applyTick(tick());
    // THEN: the spark timer resets
    expect(m.brokers[1]!.downForMs).toBe(-1);
  });

  it("shouldAssumeABrokerIsUpWhenTheTickDoesNotMentionIt", () => {
    // GIVEN: a tick that only lists broker 0
    const m = model();
    // WHEN: it is applied
    m.applyTick(tick({ brokers: [broker(0)] }));
    // THEN: brokers 1 and 2 are drawn as up without leaders or crown
    expect(m.brokers[2]).toMatchObject({ up: true, leaders: 0, underReplicated: 0, controller: false });
  });
});

describe("chaos weather", () => {
  it("shouldMarkAffectedStationsAndBuildWeatherWhenExperimentsRun", () => {
    // GIVEN: a running payment-provider outage and a finished Black Friday
    const m = model();
    const exps = [experiment({ id: "e1", scenario: "payment-provider-down", affected: ["payment"] }), experiment({ id: "e2", status: "completed", affected: ["order-generator"] })];
    // WHEN: the experiments are applied
    m.applyExperiments(exps, { "payment-provider-down": "service", "black-friday": "traffic" });
    // THEN: smoke rises over payment only
    expect(m.weather).toEqual([{ kind: "smoke", targets: ["payment"], intensity: 1, experimentId: "e1" }]);
    expect(m.station("payment").affected).toBe(true);
    expect(m.station("order-generator").affected).toBe(false);
  });
});

describe("interaction", () => {
  it("shouldFindTheOrderUnderTheCursorWhenHoveringAParcel", () => {
    // GIVEN: an order parcel travelling to payment
    const m = model();
    m.applyEvents(batch([event("created", { orderId: "abcdef12-3456", partition: 2 })]));
    run(m, 200);
    const p = m.parcels.live.find((x) => x.laneId === "orders.created>payment#2")!;
    // WHEN: the cursor is right on it
    const hit = m.hitTest(p.x + 2, p.y - 2)!;
    // THEN: the tooltip names the order, its event and its partition
    expect(hit.type).toBe("parcel");
    expect(m.describe(hit)).toEqual({ title: "Order abcdef12", lines: ["Order created", "orders.created · partition 2", "Click for the order timeline"] });
  });

  it("shouldIgnoreParcelsBeingAbsorbedWhenHitTesting", () => {
    // GIVEN: a parcel that just reached the queue and is being absorbed
    const m = model();
    m.applyEvents(batch([event("created")]));
    m.step(1);
    const p = m.parcels.live[0]!;
    p.state = "absorbing";
    Object.assign(p, STATION_POS["order-generator"]);
    // WHEN: the cursor is on it
    const hit = m.hitTest(p.x, p.y);
    // THEN: the generator station behind it is found instead
    expect(hit?.type).toBe("station");
  });

  it("shouldDescribeAStationWithLagWhenHoveringAConsumer", () => {
    // GIVEN: payment with 1,234 lag, applying a new config
    const m = model();
    m.applyTick(tick({ lag: withLag("payment", "orders.created", 0, 1234), services: [service("payment", { desiredGeneration: 2 })] }));
    // WHEN: the cursor is over the payment vault
    const hit = m.hitTest(STATION_POS.payment.x + 10, STATION_POS.payment.y);
    // THEN: the tooltip shows status, pods, throughput and lag
    expect(m.describe(hit!)).toEqual({
      title: "Payment",
      lines: ["Status: ok (applying config…)", "Replicas: 2/2 ready", "Throughput: 0 records/s", "Consumer lag: 1,234"],
    });
  });

  it("shouldOmitLagWhenHoveringTheOrderGenerator", () => {
    // GIVEN: a running generator
    const m = model();
    m.applyTick(tick());
    // WHEN: the cursor is over the factory
    const tip = m.describe(m.hitTest(STATION_POS["order-generator"].x, STATION_POS["order-generator"].y)!);
    // THEN: there is no consumer lag line
    expect(tip.lines).toEqual(["Status: ok", "Replicas: 2/2 ready", "Throughput: 50 records/s"]);
  });

  it("shouldDescribeBrokersAndTheDlqPitWhenHoveringThem", () => {
    // GIVEN: broker 0 is the controller and broker 2 is down
    const m = model();
    m.applyTick(tick({ brokers: [broker(0), broker(1), broker(2, { up: false })] }));
    // WHEN: the cursor visits broker 0, broker 2 and the DLQ pit
    const b0 = m.describe(m.hitTest(m.brokers[0]!.x, m.brokers[0]!.y)!);
    const b2 = m.describe(m.hitTest(m.brokers[2]!.x, m.brokers[2]!.y)!);
    const dlq = m.describe(m.hitTest(DLQ_POS.x, DLQ_POS.y)!);
    // THEN: each tooltip explains what is there
    expect(b0.title).toBe("Broker 0 · KRaft controller");
    expect(b0.lines[0]).toBe("Up");
    expect(b2).toEqual({ title: "Broker 2", lines: ["DOWN", "Partition leaders: 17", "Under-replicated: 0"] });
    expect(dlq.title).toBe("Dead-letter queue (orders.dlq)");
  });

  it("shouldFindNothingWhenTheCursorIsOnEmptyFloor", () => {
    // GIVEN: an idle scene
    const m = model();
    // WHEN: the cursor is in the top-left corner
    // THEN: nothing is hit
    expect(m.hitTest(5, 5)).toBeNull();
    expect(m.hitTest(WORLD.width - 5, WORLD.height - 5)).toBeNull();
  });
});
