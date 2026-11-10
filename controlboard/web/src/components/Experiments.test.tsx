import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { experiment, iso, T0 } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { Experiments } from "./Experiments";

describe("Experiments", () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(T0);
    seedBoard();
  });
  afterEach(() => vi.useRealTimers());

  it("shouldInviteToLaunchWhenNoExperimentIsActive", () => {
    // GIVEN: no experiment exists
    // WHEN: the experiments view renders
    renderWithApi(<Experiments />);
    // THEN: an empty state points to the scenarios tab
    expect(screen.getByText(/No experiment running/)).toBeInTheDocument();
  });

  it("shouldShowCountdownAndRequestedVsObservedWhenAnExperimentRuns", () => {
    // GIVEN: Black Friday runs for another 110 s; the order rate is confirmed, lag growth not yet
    seedBoard({
      experiments: [
        experiment({
          observations: [
            { label: "Order rate", requested: "1,000 orders/s", observed: "980 orders/s", confirmed: true, at: iso(-5) },
            { label: "payment consumer lag", requested: "grows", observed: "120 records", confirmed: false, at: iso(0) },
          ],
        }),
      ],
    });
    // WHEN: the view renders
    renderWithApi(<Experiments />);
    // THEN: countdown, progress and the observation table are visible
    const card = screen.getByTestId("experiment-exp-0001");
    expect(within(card).getByText(/1m 50s left/)).toBeInTheDocument();
    expect(within(card).getByRole("progressbar")).toHaveAttribute("aria-valuenow", "8");
    expect(within(card).getByText("1/2 confirmed")).toBeInTheDocument();
    expect(within(card).getByLabelText("confirmed")).toBeInTheDocument();
    expect(within(card).getByLabelText("not yet observed")).toBeInTheDocument();
    expect(within(card).getByText("order-generator")).toBeInTheDocument();
  });

  it("shouldShowStartCountdownWhenAnExperimentIsScheduled", () => {
    // GIVEN: an experiment starts in 30 s
    seedBoard({ experiments: [experiment({ status: "scheduled", startsAt: iso(30), endsAt: iso(90) })] });
    // WHEN: the view renders
    renderWithApi(<Experiments />);
    // THEN: the start countdown is shown and observations are pending
    expect(screen.getByText(/starts in 30s/)).toBeInTheDocument();
    expect(screen.getByText(/Waiting for the first observation/)).toBeInTheDocument();
  });

  it("shouldExplainRecoveryWhenTheFaultWasRevertedButKpisAreNotBackYet", () => {
    // GIVEN: the experiment ended and the system is recovering
    seedBoard({ experiments: [experiment({ status: "recovering", endsAt: iso(-1) })] });
    // WHEN: the view renders
    renderWithApi(<Experiments />);
    // THEN: the recovery state is explained
    expect(screen.getByText(/recovering — waiting for KPIs/)).toBeInTheDocument();
  });

  it("shouldAskForManualRecoveryWhenARunWithoutAutoRecoverIsOverdue", () => {
    // GIVEN: an experiment without auto-recovery passed its end time
    seedBoard({ experiments: [experiment({ autoRecover: false, startsAt: iso(-120), endsAt: iso(-20) })] });
    // WHEN: the view renders
    renderWithApi(<Experiments />);
    // THEN: the operator is asked to stop it manually
    expect(screen.getByText(/manual recovery/)).toBeInTheDocument();
  });

  it("shouldHideObservationsWhenAnIncidentDrillIsUnsolved", () => {
    // GIVEN: a hidden drill experiment runs
    seedBoard({ experiments: [experiment({ scenario: "hidden", name: "Incident Drill #1", affected: [] })] });
    // WHEN: the view renders
    renderWithApi(<Experiments />);
    // THEN: no hint about the fault leaks
    expect(screen.getByText(/hidden during the drill/)).toBeInTheDocument();
  });

  it("shouldShowTheReportCardWhenAnExperimentCompleted", () => {
    // GIVEN: Black Friday completed; alerts fired after 14 s and the system recovered after 42 s
    seedBoard({
      experiments: [
        experiment({
          status: "completed",
          endedAt: iso(-1),
          report: { timeToDetectSec: 14, timeToRecoverSec: 42, peakLag: 26434, ordersAffected: 1200, revenueImpactEur: 3456.7, alerts: ["KafkaConsumerLagHigh"] },
        }),
        experiment({ id: "exp-0000", status: "aborted", createdAt: iso(-200), report: { peakLag: 0, ordersAffected: 0, revenueImpactEur: 0, alerts: [] } }),
      ],
    });
    // WHEN: the view renders
    renderWithApi(<Experiments />);
    // THEN: the report shows detection, recovery, peak lag and business impact
    expect(screen.getAllByLabelText("Report for Black Friday")).toHaveLength(2);
    expect(within(screen.getByTestId("experiment-exp-0001")).getByText("14s")).toBeInTheDocument();
    expect(within(screen.getByTestId("experiment-exp-0001")).getByText("42s")).toBeInTheDocument();
    expect(screen.getByText("26.4k")).toBeInTheDocument();
    expect(screen.getByText("KafkaConsumerLagHigh")).toBeInTheDocument();
    const aborted = screen.getByTestId("experiment-exp-0000");
    expect(within(aborted).getByText("not detected")).toBeInTheDocument();
    expect(within(aborted).getByText("No alerts fired")).toBeInTheDocument();
  });

  it("shouldStopTheExperimentWhenTheOperatorPressesStop", async () => {
    // GIVEN: Black Friday is running
    vi.useRealTimers();
    seedBoard({ experiments: [experiment()] });
    const { api } = renderWithApi(<Experiments />);
    // WHEN: the operator presses stop
    await userEvent.click(screen.getByRole("button", { name: "Stop Black Friday" }));
    // THEN: the backend stops it and a confirmation appears
    expect(api.stopExperiment).toHaveBeenCalledWith("exp-0001");
    expect(toastTitles()).toContain("Black Friday stopped");
  });

  it("shouldToastAnErrorWhenStoppingTheExperimentFails", async () => {
    // GIVEN: Black Friday is running but the backend rejects the stop
    vi.useRealTimers();
    seedBoard({ experiments: [experiment()] });
    renderWithApi(<Experiments />, fakeApi({ stopExperiment: vi.fn().mockRejectedValue(new Error("x")) }));
    // WHEN: the operator presses stop
    await userEvent.click(screen.getByRole("button", { name: "Stop Black Friday" }));
    // THEN: the failure is reported
    expect(toastTitles()).toContain("Unexpected error");
  });
});
