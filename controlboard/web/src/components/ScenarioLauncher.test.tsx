import { fireEvent, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ApiError } from "../api/client";
import type { ScenarioDef } from "../api/types";
import { boardStore } from "../store/store";
import { brokerCrash, experiment, scenario } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { ScenarioLauncher } from "./ScenarioLauncher";

const flashSale: ScenarioDef = scenario({
  id: "flash-sale",
  name: "Flash Sale",
  category: "traffic",
  severity: "medium",
  params: [
    { key: "sku", label: "Popular SKU", type: "select", options: ["SKU-1337", "SKU-0042"], default: "SKU-1337" },
    { key: "randomize", label: "Randomize bursts", type: "boolean", default: false },
  ],
});
const silent: ScenarioDef = scenario({
  id: "silent-failure",
  name: "Silent Failure",
  category: "service",
  params: [{ key: "service", label: "Target service", type: "service", options: ["payment", "shipping"], default: "shipping" }],
});

describe("ScenarioLauncher", () => {
  beforeEach(() => seedBoard({ scenarios: [scenario(), flashSale, silent, brokerCrash()] }));

  it("shouldShowLoadingWhenTheScenarioLibraryIsNotLoadedYet", () => {
    // GIVEN: the scenario list has not arrived from the backend
    seedBoard({ scenarios: [] });
    // WHEN: the launcher renders
    renderWithApi(<ScenarioLauncher />);
    // THEN: a loading hint is shown
    expect(screen.getByText(/Loading scenario library/)).toBeInTheDocument();
  });

  it("shouldGroupScenarioCardsByCategoryWhenTheLibraryIsLoaded", () => {
    // GIVEN: traffic, service and broker scenarios are available
    // WHEN: the launcher renders
    renderWithApi(<ScenarioLauncher />);
    // THEN: each category has its own section with severity indicators
    expect(screen.getByRole("heading", { name: /Traffic/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Services/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Kafka brokers/ })).toBeInTheDocument();
    expect(within(screen.getByTestId("scenario-black-friday")).getByLabelText("severity high")).toBeInTheDocument();
  });

  it("shouldStartAScenarioWithTheConfiguredParametersWhenTheOperatorLaunchesIt", async () => {
    // GIVEN: the operator opens Black Friday
    const { api } = renderWithApi(<ScenarioLauncher />);
    await userEvent.click(screen.getByRole("button", { name: "Launch Black Friday" }));
    // WHEN: they set 1,500 orders/s, a 60 s duration, a 10 s start delay, manual recovery and start
    fireEvent.change(screen.getByLabelText(/Orders per second/, { selector: "input[type=number]" }), { target: { value: "1500" } });
    fireEvent.change(screen.getByLabelText("Duration (s)"), { target: { value: "60" } });
    fireEvent.change(screen.getByLabelText("Start delay (s)"), { target: { value: "10" } });
    await userEvent.click(screen.getByLabelText(/Recover automatically/));
    await userEvent.click(screen.getByRole("button", { name: /Schedule/ }));
    // THEN: the experiment request carries exactly these settings and the dialog closes
    expect(api.startExperiment).toHaveBeenCalledWith({ scenario: "black-friday", params: { ordersPerSecond: 1500 }, durationSec: 60, startDelaySec: 10, autoRecover: false });
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(boardStore.getState().experiments.map((e) => e.id)).toEqual(["exp-0001"]);
    expect(toastTitles()).toContain("Black Friday started");
  });

  it("shouldClampNumberParametersWhenTheSliderGoesOutOfRange", async () => {
    // GIVEN: the Black Friday dialog is open
    const { api } = renderWithApi(<ScenarioLauncher />);
    await userEvent.click(screen.getByRole("button", { name: "Launch Black Friday" }));
    // WHEN: the operator drags the slider and types a value above the maximum
    fireEvent.change(screen.getByLabelText("Orders per second slider"), { target: { value: "700" } });
    expect(screen.getByLabelText(/Orders per second/, { selector: "input[type=number]" })).toHaveValue(700);
    fireEvent.change(screen.getByLabelText(/Orders per second/, { selector: "input[type=number]" }), { target: { value: "99999" } });
    await userEvent.click(screen.getByRole("button", { name: /Start/ }));
    // THEN: the value is clamped to the scenario maximum
    expect(api.startExperiment).toHaveBeenCalledWith(expect.objectContaining({ params: { ordersPerSecond: 2000 } }));
  });

  it("shouldSendSelectAndBooleanParametersWhenTheOperatorChangesThem", async () => {
    // GIVEN: the Flash Sale dialog is open
    const { api } = renderWithApi(<ScenarioLauncher />);
    await userEvent.click(screen.getByRole("button", { name: "Launch Flash Sale" }));
    // WHEN: another SKU is chosen and random bursts are enabled
    await userEvent.selectOptions(screen.getByLabelText("Popular SKU"), "SKU-0042");
    await userEvent.click(screen.getByLabelText("Randomize bursts"));
    await userEvent.click(screen.getByRole("button", { name: /Start/ }));
    // THEN: the request contains the typed parameter values
    expect(api.startExperiment).toHaveBeenCalledWith(expect.objectContaining({ scenario: "flash-sale", params: { sku: "SKU-0042", randomize: true } }));
  });

  it("shouldLockDestructiveScenariosWhenChaosIsDisarmed", () => {
    // GIVEN: chaos is disarmed
    // WHEN: the broker crash card renders
    renderWithApi(<ScenarioLauncher />);
    // THEN: its launch button is disabled
    expect(screen.getByRole("button", { name: "Launch Broker Crash" })).toBeDisabled();
  });

  it("shouldRequireTypingTheScenarioIdWhenStartingADestructiveScenario", async () => {
    // GIVEN: chaos is armed and the operator opens Broker Crash
    seedBoard({ chaosArmed: true, scenarios: [brokerCrash()] });
    const { api } = renderWithApi(<ScenarioLauncher />);
    await userEvent.click(screen.getByRole("button", { name: "Launch Broker Crash" }));
    const start = screen.getByRole("button", { name: /Start/ });
    expect(start).toBeDisabled();
    expect(screen.getByText('Type "broker-crash" to confirm')).toBeInTheDocument();
    // WHEN: they pick broker 2 and type the scenario id
    await userEvent.click(screen.getByRole("button", { name: "Broker 2" }));
    await userEvent.type(screen.getByLabelText("Confirm scenario id"), "broker-crash");
    await userEvent.click(start);
    // THEN: the request includes the confirmation and the chosen broker
    expect(api.startExperiment).toHaveBeenCalledWith(expect.objectContaining({ scenario: "broker-crash", params: { broker: 2 }, confirm: "broker-crash" }));
  });

  it("shouldShowTheConflictInsideTheDialogWhenTheBackendRejectsWith409", async () => {
    // GIVEN: another broker fault is already active on the backend
    seedBoard({ chaosArmed: true, scenarios: [brokerCrash()] });
    const api = fakeApi({ startExperiment: vi.fn().mockRejectedValue(new ApiError(409, "broker_fault_active", "Another broker fault is already active. Stop it first.")) });
    renderWithApi(<ScenarioLauncher />, api);
    await userEvent.click(screen.getByRole("button", { name: "Launch Broker Crash" }));
    await userEvent.type(screen.getByLabelText("Confirm scenario id"), "broker-crash");
    // WHEN: the operator submits
    await userEvent.click(screen.getByRole("button", { name: /Start/ }));
    // THEN: the dialog stays open and explains the conflict
    expect(screen.getByRole("alert")).toHaveTextContent("A broker fault is already active");
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("shouldStopARunningScenarioWhenTheOperatorPressesStopOnItsCard", async () => {
    // GIVEN: Black Friday is currently running
    seedBoard({ scenarios: [scenario()], experiments: [experiment()] });
    const { api } = renderWithApi(<ScenarioLauncher />);
    // WHEN: the operator presses stop on the card
    await userEvent.click(screen.getByRole("button", { name: "Stop Black Friday" }));
    // THEN: the experiment is stopped and reverted
    expect(api.stopExperiment).toHaveBeenCalledWith("exp-0001");
    expect(toastTitles()).toContain("Black Friday stopped");
  });

  it("shouldToastAnErrorWhenStoppingFails", async () => {
    // GIVEN: Black Friday runs but the backend is unreachable
    seedBoard({ scenarios: [scenario()], experiments: [experiment()] });
    const api = fakeApi({ stopExperiment: vi.fn().mockRejectedValue(new Error("offline")) });
    renderWithApi(<ScenarioLauncher />, api);
    // WHEN: the operator presses stop
    await userEvent.click(screen.getByRole("button", { name: "Stop Black Friday" }));
    // THEN: an error toast is shown
    expect(toastTitles()).toContain("Unexpected error");
  });

  it("shouldCloseTheDialogWhenTheOperatorCancels", async () => {
    // GIVEN: the Silent Failure dialog is open with its service selector
    const { api } = renderWithApi(<ScenarioLauncher />);
    await userEvent.click(screen.getByRole("button", { name: "Launch Silent Failure" }));
    await userEvent.selectOptions(screen.getByLabelText("Target service"), "payment");
    // WHEN: the operator cancels
    await userEvent.click(screen.getByRole("button", { name: "Cancel" }));
    // THEN: nothing is started
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(api.startExperiment).not.toHaveBeenCalled();
  });
});
