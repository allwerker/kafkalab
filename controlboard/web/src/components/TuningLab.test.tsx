import { fireEvent, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { TuningLab } from "./TuningLab";

describe("TuningLab", () => {
  beforeEach(() => seedBoard());

  it("shouldShowLoadingWhenSettingsAreNotLoadedYet", () => {
    // GIVEN: the tuning endpoint has not answered yet
    const api = fakeApi({ tuning: vi.fn().mockReturnValue(new Promise(() => undefined)) });
    // WHEN: the tuning lab renders
    renderWithApi(<TuningLab />, api);
    // THEN: a loading hint is shown
    expect(screen.getByText(/Loading client settings/)).toBeInTheDocument();
  });

  it("shouldFlagLossyValuesWhenTheOperatorPicksAcksLeader", async () => {
    // GIVEN: payment uses acks=all
    renderWithApi(<TuningLab />);
    const acks = await screen.findByLabelText(/Producer acks/);
    // WHEN: the operator switches to acks=leader
    await userEvent.selectOptions(acks, "leader");
    // THEN: the field warns that delivery guarantees are weakened
    expect(screen.getByRole("alert")).toHaveTextContent("Weakens delivery guarantees");
    expect(screen.getByText("lossy configuration")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Apply \(1\)/ })).toBeEnabled();
  });

  it("shouldApplyOnlyChangedSettingsWhenTheFormIsSubmitted", async () => {
    // GIVEN: payment's linger.ms is 5
    const { api } = renderWithApi(<TuningLab />);
    const linger = await screen.findByLabelText(/linger.ms/);
    // WHEN: the operator raises it to 50 and applies
    fireEvent.change(linger, { target: { value: "50" } });
    await userEvent.click(screen.getByRole("button", { name: /Apply/ }));
    // THEN: only the changed key is sent for payment
    expect(api.putTuning).toHaveBeenCalledWith("payment", { lingerMs: 50 });
    expect(toastTitles()).toContain("payment: client tuning applied");
  });

  it("shouldResetEverySettingWhenResetToDefaultsIsPressed", async () => {
    // GIVEN: the operator switched to the shipping service, which runs a non-default linger.ms
    const { api } = renderWithApi(<TuningLab />);
    await userEvent.click(await screen.findByRole("tab", { name: "Shipping" }));
    expect(screen.getByText("non-default")).toBeInTheDocument();
    // WHEN: they reset to defaults
    await userEvent.click(screen.getByRole("button", { name: /Reset to defaults/ }));
    // THEN: every key is sent as null for shipping
    expect(api.putTuning).toHaveBeenCalledWith("shipping", { acks: null, lingerMs: null });
  });

  it("shouldReportAnErrorWhenApplyingFails", async () => {
    // GIVEN: the backend rejects tuning updates
    const api = fakeApi({ putTuning: vi.fn().mockRejectedValue(new Error("nope")) });
    renderWithApi(<TuningLab />, api);
    fireEvent.change(await screen.findByLabelText(/linger.ms/), { target: { value: "10" } });
    // WHEN: the operator applies
    await userEvent.click(screen.getByRole("button", { name: /Apply/ }));
    // THEN: an error toast appears
    await waitFor(() => expect(toastTitles()).toContain("Unexpected error"));
  });
});
