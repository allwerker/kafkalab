import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { toast } from "../store/toasts";
import { act } from "@testing-library/react";
import { broker, experiment, scenario, service } from "../test/fixtures";
import { renderWithApi, seedBoard } from "../test/render";
import { App } from "./App";

const loadLive = () => Promise.resolve(() => ({ destroy() {} }));

describe("App", () => {
  beforeEach(() => {
    localStorage.clear();
    seedBoard({
      scenarios: [scenario()],
      experiments: [experiment()],
      brokers: [broker({ id: 0 }), broker({ id: 1, up: false }), broker({ id: 2 })],
      services: [service(), service({ name: "shipping", health: "degraded" })],
    });
  });

  it("shouldShowLiveViewAlertsAndScenariosWhenTheBoardOpens", () => {
    // GIVEN: one experiment runs and one of three brokers is down
    // WHEN: the control room opens
    renderWithApi(<App loadLive={loadLive} />);
    // THEN: hero live view, alert panel, the scenarios tab and the cluster summary are visible
    expect(screen.getByLabelText("Live view")).toBeInTheDocument();
    expect(screen.getByLabelText("Alerts")).toBeInTheDocument();
    expect(screen.getByRole("tab", { name: /Scenarios/ })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByTestId("scenario-black-friday")).toBeInTheDocument();
    expect(screen.getByLabelText("Cluster summary")).toHaveTextContent("brokers 2/3");
    expect(screen.getByLabelText("Cluster summary")).toHaveTextContent("services healthy 1/2");
    expect(screen.getByRole("tab", { name: /Experiments/ })).toHaveTextContent("1");
  });

  it("shouldSwitchTabsAndRememberTheChoiceWhenTheOperatorClicksATab", async () => {
    // GIVEN: the board is open on the scenarios tab
    renderWithApi(<App loadLive={loadLive} />);
    // WHEN: the operator walks through every tab
    await userEvent.click(screen.getByRole("tab", { name: /Experiments/ }));
    expect(screen.getByRole("heading", { name: /Timeline/ })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("tab", { name: /Services/ }));
    expect(screen.getByTestId("service-payment")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("tab", { name: /Tuning Lab/ }));
    await userEvent.click(screen.getByRole("tab", { name: /Incident Drill/ }));
    expect(screen.getByRole("button", { name: /Start drill/ })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("tab", { name: /Launchpad/ }));
    // THEN: the last tab is active and remembered for the next visit
    expect(screen.getByRole("tab", { name: /Launchpad/ })).toHaveAttribute("aria-selected", "true");
    expect(localStorage.getItem("kafka-lab:tab")).toBe('"launchpad"');
  });

  it("shouldShowAndDismissToastsWhenSomethingHappens", async () => {
    // GIVEN: the board is open
    renderWithApi(<App loadLive={loadLive} />);
    // WHEN: an error and an info message arrive
    act(() => {
      toast("error", "Chaos is disarmed", "Arm chaos first.");
      toast("info", "Simulation running");
    });
    // THEN: both are visible, errors as alerts, and they can be dismissed
    expect(screen.getByText("Arm chaos first.")).toBeInTheDocument();
    expect(screen.getAllByRole("status").some((s) => s.textContent?.includes("Simulation running"))).toBe(true);
    await userEvent.click(screen.getAllByRole("button", { name: "Dismiss" })[0]!);
    expect(screen.queryByText("Arm chaos first.")).not.toBeInTheDocument();
  });

  it("shouldUseDefaultCountsWhenNoClusterStateArrivedYet", () => {
    // GIVEN: no broker or service state is known yet
    seedBoard();
    // WHEN: the board opens
    renderWithApi(<App loadLive={loadLive} />);
    // THEN: the summary assumes the planned 3 brokers and 6 services
    expect(screen.getByLabelText("Cluster summary")).toHaveTextContent("brokers 0/3");
    expect(screen.getByLabelText("Cluster summary")).toHaveTextContent("services healthy 0/6");
  });
});
