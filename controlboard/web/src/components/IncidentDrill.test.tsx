import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { boardStore } from "../store/store";
import { drill, iso } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { IncidentDrill } from "./IncidentDrill";

describe("IncidentDrill", () => {
  beforeEach(() => {
    localStorage.clear();
    seedBoard();
  });

  it("shouldStartADrillWithNameAndDifficultyWhenThePlayerPressesStart", async () => {
    // GIVEN: no drill runs and the leaderboard is empty
    const { api } = renderWithApi(<IncidentDrill />);
    expect(await screen.findByText(/No scores yet/)).toBeInTheDocument();
    // WHEN: Ada picks "Hard" and starts
    await userEvent.type(screen.getByLabelText("Player name"), "Ada");
    await userEvent.click(screen.getByRole("radio", { name: /Hard/ }));
    await userEvent.click(screen.getByRole("button", { name: /Start drill/ }));
    // THEN: the drill starts on the backend and the name is remembered
    expect(api.startDrill).toHaveBeenCalledWith("hard", "Ada");
    expect(boardStore.getState().drill?.status).toBe("running");
    expect(localStorage.getItem("kafka-lab:player")).toBe('"Ada"');
    expect(toastTitles()).toContain("Incident drill started");
  });

  it("shouldPlayAsAnonymousWhenNoNameIsGiven", async () => {
    // GIVEN: the player leaves the name empty
    const { api } = renderWithApi(<IncidentDrill />);
    // WHEN: they start an easy drill
    await userEvent.click(screen.getByRole("button", { name: /Start drill/ }));
    // THEN: the backend gets "anonymous"
    expect(api.startDrill).toHaveBeenCalledWith("easy", "anonymous");
  });

  it("shouldRevealAHintWhenThePlayerAsksForOne", async () => {
    // GIVEN: a drill runs and the backend answers with the first hint
    seedBoard({ drill: drill() });
    const api = fakeApi({ hint: vi.fn().mockResolvedValue(drill({ hintsUsed: 1, hints: ["Compare partitions, not groups."] })) });
    renderWithApi(<IncidentDrill />, api);
    // WHEN: the player asks for a hint
    await userEvent.click(screen.getByRole("button", { name: /Hint \(0\/3/ }));
    // THEN: the hint is shown
    expect(await screen.findByText("Compare partitions, not groups.")).toBeInTheDocument();
  });

  it("shouldStrikeThroughAWrongGuessWhenThePlayerGuessesWrong", async () => {
    // GIVEN: a drill runs; the answer is not "Flash Sale"
    seedBoard({ drill: drill() });
    const api = fakeApi({ guess: vi.fn().mockResolvedValue(drill({ guesses: [{ scenario: "flash-sale", correct: false, at: iso(0) }] })) });
    renderWithApi(<IncidentDrill />, api);
    // WHEN: the player guesses Flash Sale
    await userEvent.click(screen.getByRole("button", { name: "Flash Sale" }));
    // THEN: the candidate is disabled and the player is encouraged to continue
    expect(screen.getByRole("button", { name: /Flash Sale/ })).toBeDisabled();
    expect(toastTitles()).toContain("Not quite — keep investigating");
  });

  it("shouldCelebrateAndShowTheResultWhenThePlayerGuessesRight", async () => {
    // GIVEN: a drill runs with Hot Partition as the hidden fault
    seedBoard({ drill: drill() });
    const solved = drill({ status: "solved", score: 640, scenario: "hot-partition", guesses: [{ scenario: "hot-partition", correct: true, at: iso(0) }] });
    const api = fakeApi({
      guess: vi.fn().mockResolvedValue(solved),
      leaderboard: vi.fn().mockResolvedValue([{ player: "Ada", score: 640, scenario: "hot-partition", durationSec: 95, at: iso(0) }]),
    });
    renderWithApi(<IncidentDrill />, api);
    // WHEN: the player guesses Hot Partition
    await userEvent.click(screen.getByRole("button", { name: "Hot Partition" }));
    // THEN: the score is celebrated and the result and leaderboard are shown
    expect(toastTitles()).toContain("Solved! 640 points");
    expect(screen.getByRole("status")).toHaveTextContent("it was Hot Partition");
    expect(await screen.findByText("Ada")).toBeInTheDocument();
    expect(screen.getByText("1m 35s")).toBeInTheDocument();
  });

  it("shouldEndTheDrillWhenThePlayerGivesUp", async () => {
    // GIVEN: a drill runs
    seedBoard({ drill: drill() });
    const api = fakeApi({ reveal: vi.fn().mockResolvedValue(drill({ status: "revealed", score: 0, scenario: "unknown-x" })) });
    renderWithApi(<IncidentDrill />, api);
    // WHEN: the player gives up
    await userEvent.click(screen.getByRole("button", { name: /Give up/ }));
    // THEN: the answer is revealed (unknown ids fall back to the raw id)
    expect(screen.getByRole("status")).toHaveTextContent("revealed");
    expect(screen.getByRole("status")).toHaveTextContent("unknown-x");
  });

  it("shouldToastAnErrorWhenStartingFails", async () => {
    // GIVEN: a drill is already running on the backend
    const api = fakeApi({ startDrill: vi.fn().mockRejectedValue(new Error("drill_active")) });
    renderWithApi(<IncidentDrill />, api);
    // WHEN: the player starts another
    await userEvent.click(screen.getByRole("button", { name: /Start drill/ }));
    // THEN: the error is reported
    expect(toastTitles()).toContain("Unexpected error");
  });

  it("shouldReportAnErrorWhenAHintRequestFails", async () => {
    // GIVEN: a drill runs but the backend is unavailable
    seedBoard({ drill: drill() });
    renderWithApi(<IncidentDrill />, fakeApi({ hint: vi.fn().mockRejectedValue(new Error("x")) }));
    // WHEN: the player asks for a hint
    await userEvent.click(screen.getByRole("button", { name: /Hint/ }));
    // THEN: the failure is reported
    expect(toastTitles()).toContain("Unexpected error");
  });
});
