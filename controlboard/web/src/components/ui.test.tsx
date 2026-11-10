import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Meter, Modal, NamedIcon, Panel, SeverityBadge } from "./ui";

describe("ui primitives", () => {
  it("shouldLightFourBarsWhenTheSeverityIsCritical", () => {
    // GIVEN: a critical scenario
    // WHEN: its severity badge renders
    const { container } = render(<SeverityBadge severity="critical" />);
    // THEN: all four bars are lit
    expect(container.querySelectorAll("i.on")).toHaveLength(4);
  });

  it("shouldClampTheMeterWhenProgressExceedsOneHundredPercent", () => {
    // GIVEN: an overdue experiment reports more than 100% progress
    // WHEN: the meter renders
    render(<Meter value={1.7} label="progress" />);
    // THEN: it shows at most 100%
    expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "100");
  });

  it("shouldFallBackToALinkIconWhenTheIconNameIsUnknown", () => {
    // GIVEN: the backend sent an icon name the board does not know
    // WHEN: the icon renders
    const { container } = render(<NamedIcon name="does-not-exist" />);
    // THEN: some icon is still shown
    expect(container.querySelector("svg")).not.toBeNull();
  });

  it("shouldRenderActionsWhenAPanelHasThem", () => {
    // GIVEN: a panel with an action button
    // WHEN: it renders
    render(
      <Panel title="Alerts" actions={<button>Mute</button>}>
        body
      </Panel>,
    );
    // THEN: title, action and body are present
    expect(screen.getByRole("heading", { name: "Alerts" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Mute" })).toBeInTheDocument();
    expect(screen.getByText("body")).toBeInTheDocument();
  });

  it("shouldCloseTheModalWhenEscapeOrTheBackdropIsUsed", async () => {
    // GIVEN: a confirmation dialog is open
    const onClose = vi.fn();
    const { container } = render(
      <Modal title="Confirm" onClose={onClose}>
        <button>OK</button>
      </Modal>,
    );
    // THEN: focus starts on the first action
    expect(screen.getByRole("button", { name: "OK" })).toHaveFocus();
    // WHEN: the operator presses Escape, clicks inside, and clicks the backdrop
    await userEvent.keyboard("{Escape}");
    await userEvent.click(screen.getByRole("button", { name: "OK" }));
    await userEvent.click(container.querySelector(".modal-backdrop")!);
    // THEN: Escape and the backdrop close it, a click inside does not
    expect(onClose).toHaveBeenCalledTimes(2);
  });
});
