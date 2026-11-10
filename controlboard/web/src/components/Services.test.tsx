import { fireEvent, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ApiError } from "../api/client";
import { boardStore } from "../store/store";
import { service } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { Services } from "./Services";

describe("Services", () => {
  beforeEach(() =>
    seedBoard({
      services: [
        service({ name: "order-generator", replicas: 1, readyReplicas: 1, knobs: { ordersPerSecond: 200 } }),
        service({ name: "payment", desiredGeneration: 5, appliedGeneration: 4, knobs: { processingLatencyMs: 150 } }),
        service({ name: "shipping", paused: true, health: "up" }),
        service({ name: "notification", replicas: 0, readyReplicas: 0, health: "down" }),
      ],
      lag: { payment: { "orders.created": [100, 200], "orders.cancelled": [5] } },
    }),
  );

  it("shouldWaitForStateWhenNoServicesAreKnown", () => {
    // GIVEN: the board has no service state yet
    seedBoard();
    // WHEN: the services tab renders
    renderWithApi(<Services />);
    // THEN: a waiting hint is shown
    expect(screen.getByText(/Waiting for service state/)).toBeInTheDocument();
  });

  it("shouldShowApplyingUntilTheServiceReportsTheDesiredGenerationWhenConfigChanged", () => {
    // GIVEN: payment was told generation 5 but its pods still run generation 4
    // WHEN: the services tab renders
    renderWithApi(<Services />);
    // THEN: payment shows "applying…", lag and its non-default knob
    const card = screen.getByTestId("service-payment");
    expect(within(card).getByText(/applying… 4\/5/)).toBeInTheDocument();
    expect(within(card).getByText("305")).toBeInTheDocument();
    expect(within(card).getByText("processingLatencyMs=150")).toBeInTheDocument();
    expect(within(screen.getByTestId("service-shipping")).getByText("gen 3")).toBeInTheDocument();
    expect(within(screen.getByTestId("service-shipping")).getByText("paused")).toBeInTheDocument();
  });

  it("shouldScaleTheServiceWhenTheOperatorPressesPlusOrMinus", async () => {
    // GIVEN: payment runs two replicas
    const { api } = renderWithApi(<Services />);
    // WHEN: the operator scales up and then down
    await userEvent.click(screen.getByRole("button", { name: "Scale payment up" }));
    await userEvent.click(screen.getByRole("button", { name: "Scale payment down" }));
    // THEN: the backend receives 3, the returned state (3 replicas) is stored, and scaling down asks for 2
    expect(api.scale).toHaveBeenNthCalledWith(1, "payment", 3);
    expect(api.scale).toHaveBeenNthCalledWith(2, "payment", 2);
    expect(boardStore.getState().services.find((s) => s.name === "payment")?.replicas).toBe(3);
  });

  it("shouldNotScaleBelowZeroWhenTheServiceIsAlreadyScaledToZero", () => {
    // GIVEN: notification is scaled to zero
    // WHEN: the services tab renders
    renderWithApi(<Services />);
    // THEN: the scale-down button is disabled
    expect(screen.getByRole("button", { name: "Scale notification down" })).toBeDisabled();
  });

  it("shouldPauseAndResumeConsumersWhenTheOperatorTogglesPause", async () => {
    // GIVEN: payment is consuming and shipping is paused
    const { api } = renderWithApi(<Services />);
    // WHEN: the operator pauses payment and resumes shipping
    await userEvent.click(within(screen.getByTestId("service-payment")).getByRole("button", { name: /Pause/ }));
    await userEvent.click(within(screen.getByTestId("service-shipping")).getByRole("button", { name: /Resume/ }));
    // THEN: the paused knob is set accordingly
    expect(api.putKnobs).toHaveBeenCalledWith("payment", { paused: true });
    expect(api.putKnobs).toHaveBeenCalledWith("shipping", { paused: false });
  });

  it("shouldApplyLatencyAndFailureProbabilityWhenTheQuickEditorIsSubmitted", async () => {
    // GIVEN: the operator wants payment to be slow and to decline 20% of cards
    const { api } = renderWithApi(<Services />);
    // WHEN: they enter 300 ms and 0.2 and apply
    fireEvent.change(screen.getByLabelText("payment latency"), { target: { value: "300" } });
    fireEvent.change(screen.getByLabelText("payment failure probability"), { target: { value: "0.2" } });
    await userEvent.click(screen.getByRole("button", { name: "Apply knobs to payment" }));
    // THEN: both knobs are sent
    expect(api.putKnobs).toHaveBeenCalledWith("payment", { processingLatencyMs: 300, businessFailureProbability: 0.2 });
    expect(toastTitles()).toContain("payment knobs applied");
  });

  it("shouldResetFailureProbabilityToDefaultWhenTheFieldIsEmpty", async () => {
    // GIVEN: shipping has no failure override
    const { api } = renderWithApi(<Services />);
    // WHEN: the operator applies the editor unchanged
    await userEvent.click(screen.getByRole("button", { name: "Apply knobs to shipping" }));
    // THEN: the failure probability is sent as null (= service default)
    expect(api.putKnobs).toHaveBeenCalledWith("shipping", { processingLatencyMs: 0, businessFailureProbability: null });
  });

  it("shouldSetTheOrderRateWhenTheGeneratorEditorIsSubmitted", async () => {
    // GIVEN: the generator runs at an overridden 200 orders/s
    const { api } = renderWithApi(<Services />);
    // WHEN: the operator sets 400 and then clears the field
    const input = screen.getByLabelText("order-generator orders per second");
    expect(input).toHaveValue(200);
    fireEvent.change(input, { target: { value: "400" } });
    await userEvent.click(screen.getByRole("button", { name: "Apply knobs to order-generator" }));
    fireEvent.change(input, { target: { value: "" } });
    await userEvent.click(screen.getByRole("button", { name: "Apply knobs to order-generator" }));
    // THEN: first 400, then back to the default rate (null)
    expect(api.putKnobs).toHaveBeenNthCalledWith(1, "order-generator", { ordersPerSecond: 400 });
    expect(api.putKnobs).toHaveBeenNthCalledWith(2, "order-generator", { ordersPerSecond: null });
  });

  it("shouldResetKnobsToBaselineWhenTheOperatorPressesReset", async () => {
    // GIVEN: payment has a manual latency override
    const { api } = renderWithApi(<Services />);
    // WHEN: the operator resets payment's knobs
    await userEvent.click(screen.getByRole("button", { name: "Reset knobs of payment" }));
    // THEN: the override is deleted on the backend
    expect(api.resetKnobs).toHaveBeenCalledWith("payment");
    expect(toastTitles()).toContain("payment back to baseline");
  });

  it("shouldShowTheBackendErrorWhenScalingIsRejected", async () => {
    // GIVEN: the backend rejects the replica count
    const api = fakeApi({ scale: vi.fn().mockRejectedValue(new ApiError(400, "invalid_replicas", "replicas must be an integer 0..6")) });
    renderWithApi(<Services />, api);
    // WHEN: the operator scales payment up
    await userEvent.click(screen.getByRole("button", { name: "Scale payment up" }));
    // THEN: the error is shown as a toast
    expect(toastTitles()).toContain("Request failed (400)");
  });
});
