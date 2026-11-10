import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { timeline } from "../test/fixtures";
import { renderWithApi, seedBoard } from "../test/render";
import { Timeline } from "./Timeline";

describe("Timeline", () => {
  beforeEach(() =>
    seedBoard({
      timeline: [
        timeline({ id: "t3", kind: "alert", severity: "critical", title: "Firing: KafkaBrokerDown", detail: "Kafka broker 1 is down" }),
        timeline({ id: "t2", kind: "config", severity: "info", title: "payment scaled 2 → 3" }),
        timeline({ id: "t1", kind: "experiment", title: "Black Friday started" }),
      ],
    }),
  );

  it("shouldListEntriesChronologicallyWhenTheTimelineHasEntries", () => {
    // GIVEN: an alert, a config change and an experiment start happened
    // WHEN: the timeline renders
    renderWithApi(<Timeline />);
    // THEN: all entries appear newest first with their details
    const items = screen.getAllByTestId("timeline-entry");
    expect(items.map((i) => i.textContent)).toEqual([expect.stringContaining("KafkaBrokerDown"), expect.stringContaining("scaled"), expect.stringContaining("Black Friday")]);
    expect(screen.getByText("Kafka broker 1 is down")).toBeInTheDocument();
  });

  it("shouldHideAKindWhenTheOperatorTogglesItsFilterOff", async () => {
    // GIVEN: the full timeline is shown
    renderWithApi(<Timeline />);
    // WHEN: the operator turns off config entries
    await userEvent.click(screen.getByRole("button", { name: /config/ }));
    // THEN: the scale change disappears, the rest stays
    expect(screen.queryByText("payment scaled 2 → 3")).not.toBeInTheDocument();
    expect(screen.getAllByTestId("timeline-entry")).toHaveLength(2);
    // WHEN: the operator turns config back on
    await userEvent.click(screen.getByRole("button", { name: /config/ }));
    // THEN: it is visible again
    expect(screen.getByText("payment scaled 2 → 3")).toBeInTheDocument();
  });

  it("shouldShowAnEmptyHintWhenTheFilterMatchesNothing", async () => {
    // GIVEN: only alert, config and experiment entries exist
    renderWithApi(<Timeline />);
    // WHEN: all three kinds are filtered out
    for (const k of ["alert", "config", "experiment"]) await userEvent.click(screen.getByRole("button", { name: new RegExp(k) }));
    // THEN: an empty hint is shown
    expect(screen.getByText(/Nothing in the timeline/)).toBeInTheDocument();
  });
});
