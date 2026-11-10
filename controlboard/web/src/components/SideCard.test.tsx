import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { broker, link, service } from "../test/fixtures";
import { renderWithApi, seedBoard } from "../test/render";
import { SideCard } from "./SideCard";

describe("SideCard", () => {
  beforeEach(() =>
    seedBoard({
      services: [service({ name: "payment", paused: true }), service({ name: "order-generator", replicas: 1, readyReplicas: 1 })],
      brokers: [broker({ id: 0, isController: true }), broker({ id: 1, up: false, leaderCount: 0 })],
      lag: { payment: { "orders.created": [1200, 10, 0] } },
      rates: { "payments.completed": 40, "payments.failed": 2 },
      links: [link({ url: "http://grafana.example" })],
    }),
  );

  it("shouldShowLagPerPartitionAndDashboardsWhenAConsumerStationIsSelected", () => {
    // GIVEN: payment is paused and partition 0 of orders.created lags 1,200 records
    // WHEN: the operator clicks the payment station
    renderWithApi(<SideCard selection={{ type: "station", id: "payment" }} onClose={() => undefined} />);
    // THEN: replicas, throughput, paused state, per-partition lag and dashboard links are shown
    expect(screen.getByText("2/2 ready")).toBeInTheDocument();
    expect(screen.getByText("42 rec/s")).toBeInTheDocument();
    expect(screen.getByText("yes")).toBeInTheDocument();
    expect(screen.getByLabelText("orders.created lag per partition").children).toHaveLength(3);
    expect(screen.getByText("1.21k")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Consumer lag/ })).toHaveAttribute("href", "http://grafana.example/d/kafka-lag?var-consumergroup=payment");
  });

  it("shouldOmitLagWhenTheGeneratorIsSelected", () => {
    // GIVEN: the order generator consumes nothing
    // WHEN: the operator clicks it
    renderWithApi(<SideCard selection={{ type: "station", id: "order-generator" }} onClose={() => undefined} />);
    // THEN: no lag table or lag dashboard link is shown
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Consumer lag/ })).not.toBeInTheDocument();
  });

  it("shouldShowZeroReplicasWhenTheStationHasNoState", () => {
    // GIVEN: no state is known for shipping yet
    // WHEN: the operator clicks the shipping station
    renderWithApi(<SideCard selection={{ type: "station", id: "shipping" }} onClose={() => undefined} />);
    // THEN: it shows zero ready replicas and empty lag bars
    expect(screen.getByText("0/0 ready")).toBeInTheDocument();
  });

  it("shouldMarkTheControllerWhenTheControllerBrokerIsSelected", async () => {
    // GIVEN: broker 0 is the active KRaft controller
    const onClose = vi.fn();
    renderWithApi(<SideCard selection={{ type: "broker", id: 0 }} onClose={onClose} />);
    // THEN: it is labelled as controller and links to its dashboard
    expect(screen.getByText("controller")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Broker dashboard/ })).toHaveAttribute("href", "http://grafana.example/d/kafka-broker?var-kafka_broker=0");
    // WHEN: the operator closes the card
    await userEvent.click(screen.getByRole("button", { name: "Close" }));
    // THEN: the parent is notified
    expect(onClose).toHaveBeenCalled();
  });

  it("shouldShowDownWhenADeadBrokerIsSelected", () => {
    // GIVEN: broker 1 crashed
    // WHEN: the operator clicks it
    renderWithApi(<SideCard selection={{ type: "broker", id: 1 }} onClose={() => undefined} />);
    // THEN: it is shown as DOWN
    expect(screen.getByText("DOWN")).toBeInTheDocument();
  });

  it("shouldShowUnknownWhenTheBrokerIsNotInTheState", () => {
    // GIVEN: broker 2 never reported
    // WHEN: the operator clicks it
    renderWithApi(<SideCard selection={{ type: "broker", id: 2 }} onClose={() => undefined} />);
    // THEN: its status is unknown
    expect(screen.getByText("unknown")).toBeInTheDocument();
  });
});
