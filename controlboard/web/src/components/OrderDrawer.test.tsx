import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ApiError } from "../api/client";
import type { OrderTimeline } from "../api/types";
import { iso } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard } from "../test/render";
import { OrderDrawer } from "./OrderDrawer";

const shippedOrder: OrderTimeline = {
  orderId: "0c7c2c37-31ca-4f80-bcde-e3d7038ed7f1",
  events: [
    { topic: "orders.created", partition: 3, offset: 5096, orderId: "0c7c2c37", kind: "created", at: iso(0), valueCents: 15409, region: "eu-north" },
    { topic: "payments.completed", partition: 3, offset: 4368, orderId: "0c7c2c37", kind: "paid", at: iso(2), valueCents: 15409 },
    { topic: "orders.shipped", partition: 3, offset: 4216, orderId: "0c7c2c37", kind: "shipped", at: iso(4), carrier: "Hermes" },
    { topic: "notifications.sent", partition: 3, offset: 9, orderId: "0c7c2c37", kind: "notified", at: iso(5), reason: "order-shipped" },
  ],
  traceId: "abc",
  traceUrl: "http://traces.localhost:8080/trace/abc",
  logsUrl: "http://logs.localhost:8080/select/vmui",
};

describe("OrderDrawer", () => {
  beforeEach(() => seedBoard());

  it("shouldShowTheFullOrderJourneyWhenTheOrderIsInTheRecentRing", async () => {
    // GIVEN: a €154.09 order from eu-north was created, paid, shipped by Hermes and notified
    const api = fakeApi({ order: vi.fn().mockResolvedValue(shippedOrder) });
    // WHEN: the operator clicks its parcel and the drawer opens
    renderWithApi(<OrderDrawer orderId={shippedOrder.orderId} onClose={() => undefined} />, api);
    // THEN: value, region, every step and the trace/log links are shown
    expect(await screen.findByText("€154.09")).toBeInTheDocument();
    expect(screen.getByText("eu-north")).toBeInTheDocument();
    expect(screen.getByText("Order shipped")).toBeInTheDocument();
    expect(screen.getByText("via Hermes")).toBeInTheDocument();
    expect(screen.getByText("order-shipped")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Trace/ })).toHaveAttribute("href", shippedOrder.traceUrl);
    expect(screen.getByRole("link", { name: /Logs/ })).toHaveAttribute("href", shippedOrder.logsUrl);
    expect(api.order).toHaveBeenCalledWith(shippedOrder.orderId);
  });

  it("shouldExplainWhenTheOrderAlreadyLeftTheRing", async () => {
    // GIVEN: the order is too old for the backend's recent-orders ring
    const api = fakeApi({ order: vi.fn().mockRejectedValue(new ApiError(404, "not_found", "gone")) });
    // WHEN: the drawer opens
    renderWithApi(<OrderDrawer orderId="old" onClose={() => undefined} />, api);
    // THEN: a friendly explanation is shown
    expect(await screen.findByText(/already left the recent-orders ring/)).toBeInTheDocument();
  });

  it("shouldShowTheErrorMessageWhenTheLookupFailsOtherwise", async () => {
    // GIVEN: the backend is unreachable
    const api = fakeApi({ order: vi.fn().mockRejectedValue(new Error("Backend not reachable")) });
    // WHEN: the drawer opens
    renderWithApi(<OrderDrawer orderId="x" onClose={() => undefined} />, api);
    // THEN: the error message is shown
    expect(await screen.findByText("Backend not reachable")).toBeInTheDocument();
  });

  it("shouldCloseWhenTheOperatorPressesClose", async () => {
    // GIVEN: an order without value, region or trace links is loading
    const onClose = vi.fn();
    const api = fakeApi({ order: vi.fn().mockResolvedValue({ orderId: "p", events: [{ topic: "orders.dlq", partition: 0, offset: 1, orderId: "p", kind: "dlq", at: iso(0) }] }) });
    renderWithApi(<OrderDrawer orderId="p" onClose={onClose} />, api);
    expect(screen.getByText(/Loading order timeline/)).toBeInTheDocument();
    expect(await screen.findByText("Dead-lettered")).toBeInTheDocument();
    // WHEN: the operator closes the drawer
    await userEvent.click(screen.getByRole("button", { name: "Close order" }));
    // THEN: the parent is told to close it; no trace links were offered
    expect(onClose).toHaveBeenCalled();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });
});
