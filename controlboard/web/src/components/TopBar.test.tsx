import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ApiError } from "../api/client";
import { boardStore } from "../store/store";
import { kpis } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { TopBar } from "./TopBar";

describe("TopBar", () => {
  beforeEach(() => seedBoard({ simulation: "running", connection: "open" }));

  it("shouldShowDashesWhenNoKpisArrivedYet", () => {
    // GIVEN: the board has not received any tick from the backend yet
    seedBoard({ kpis: null });
    // WHEN: the operator looks at the KPI ticker
    renderWithApi(<TopBar />);
    // THEN: every KPI shows a placeholder instead of a number
    expect(within(screen.getByTestId("kpi-ordersPerSec")).getByText("—")).toBeInTheDocument();
  });

  it("shouldColourKpisRedWhenPaymentsFailAndLagExplodes", () => {
    // GIVEN: payments mostly fail and a huge backlog built up over a few ticks
    const bad = kpis({ paymentSuccessRate: 0.4, totalLag: 50000, e2eP95Ms: 30000 });
    seedBoard({ kpis: bad, kpiHistory: [{ at: 1, kpis: kpis() }, { at: 2, kpis: bad }] });
    // WHEN: the ticker renders
    renderWithApi(<TopBar />);
    // THEN: the affected tiles turn red, healthy ones stay green
    expect(screen.getByTestId("kpi-paymentSuccessRate")).toHaveClass("kpi--bad");
    expect(screen.getByTestId("kpi-totalLag")).toHaveClass("kpi--bad");
    expect(screen.getByTestId("kpi-ordersPerSec")).toHaveClass("kpi--ok");
    expect(within(screen.getByTestId("kpi-paymentSuccessRate")).getByText("40.0%")).toBeInTheDocument();
  });

  it("shouldAskForConfirmationWhenArmingChaos", async () => {
    // GIVEN: chaos is safe (disarmed)
    const { api } = renderWithApi(<TopBar />);
    // WHEN: the operator flips the chaos switch and confirms the warning
    await userEvent.click(screen.getByRole("switch"));
    expect(screen.getByRole("dialog", { name: "Arm chaos?" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Arm chaos" }));
    // THEN: the backend is armed and the switch shows the armed state
    expect(api.armChaos).toHaveBeenCalledWith(true);
    expect(boardStore.getState().chaosArmed).toBe(true);
    expect(screen.getByRole("switch")).toHaveAttribute("aria-checked", "true");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shouldStaySafeWhenTheOperatorCancelsArming", async () => {
    // GIVEN: chaos is disarmed and the confirmation dialog is open
    const { api } = renderWithApi(<TopBar />);
    await userEvent.click(screen.getByRole("switch"));
    // WHEN: the operator chooses to keep the lab safe
    await userEvent.click(screen.getByRole("button", { name: "Keep safe" }));
    // THEN: nothing is armed
    expect(api.armChaos).not.toHaveBeenCalled();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shouldDisarmImmediatelyWhenChaosIsArmed", async () => {
    // GIVEN: chaos is already armed
    seedBoard({ chaosArmed: true });
    const { api } = renderWithApi(<TopBar />);
    // WHEN: the operator flips the switch off
    await userEvent.click(screen.getByRole("switch"));
    // THEN: it disarms without asking
    expect(api.armChaos).toHaveBeenCalledWith(false);
    expect(toastTitles()).toContain("Chaos disarmed");
  });

  it("shouldRevertEverythingWhenPanicIsPressed", async () => {
    // GIVEN: chaos is armed and experiments are running
    seedBoard({ chaosArmed: true });
    const { api } = renderWithApi(<TopBar />);
    // WHEN: the operator hits PANIC
    await userEvent.click(screen.getByRole("button", { name: /PANIC/ }));
    // THEN: the backend reverts everything and chaos is disarmed locally
    expect(api.panic).toHaveBeenCalled();
    expect(boardStore.getState().chaosArmed).toBe(false);
    expect(toastTitles()).toContain("PANIC: baseline restored");
  });

  it("shouldStopTheSimulationWhenItIsRunning", async () => {
    // GIVEN: the simulation is running
    const { api } = renderWithApi(<TopBar />);
    // WHEN: the operator presses stop and then reset
    await userEvent.click(screen.getByRole("button", { name: "Stop simulation" }));
    await userEvent.click(screen.getByRole("button", { name: "Reset simulation" }));
    // THEN: both actions reach the backend
    expect(api.simulation).toHaveBeenCalledWith("stop");
    expect(api.simulation).toHaveBeenCalledWith("reset");
    expect(toastTitles()).toEqual(["Simulation stopped", "Simulation reset"]);
  });

  it("shouldOfferStartWhenTheSimulationIsStopped", async () => {
    // GIVEN: the simulation is stopped
    seedBoard({ simulation: "stopped" });
    const { api } = renderWithApi(<TopBar />);
    // WHEN: the operator presses start
    await userEvent.click(screen.getByRole("button", { name: "Start simulation" }));
    // THEN: the simulation is started
    expect(api.simulation).toHaveBeenCalledWith("start");
  });

  it("shouldShowAnErrorToastWhenTheBackendRejectsPanic", async () => {
    // GIVEN: the backend is unreachable
    const api = fakeApi({ panic: vi.fn().mockRejectedValue(new ApiError(0, "network_error", "down")) });
    renderWithApi(<TopBar />, api);
    // WHEN: the operator hits PANIC
    await userEvent.click(screen.getByRole("button", { name: /PANIC/ }));
    // THEN: an error toast explains the backend is unreachable
    expect(toastTitles()).toContain("Backend unreachable");
  });

  it("shouldShowRetryCountdownWhenTheConnectionIsLost", () => {
    // GIVEN: the WebSocket dropped and reconnects in 4 seconds
    seedBoard({ connection: "reconnecting", retryInMs: 4000 });
    // WHEN: the top bar renders
    renderWithApi(<TopBar />);
    // THEN: the connection pill shows the retry countdown
    expect(screen.getByLabelText("connection reconnecting")).toHaveTextContent("Retry 4s");
  });

  it("shouldShowConnectionStateWhenClosedWithoutRetry", () => {
    // GIVEN: the connection is closed for good
    seedBoard({ connection: "closed" });
    // WHEN: the top bar renders
    renderWithApi(<TopBar />);
    // THEN: the pill names the state
    expect(screen.getByLabelText("connection closed")).toHaveTextContent("closed");
  });
});
