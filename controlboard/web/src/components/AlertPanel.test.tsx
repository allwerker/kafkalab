import { act, fireEvent, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { ApiError } from "../api/client";
import { resetAudio } from "../lib/browser";
import { boardStore } from "../store/store";
import { alert } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard, toastTitles } from "../test/render";
import { AlertPanel } from "./AlertPanel";

function fakeAudio() {
  const node = () => ({ connect: vi.fn().mockReturnThis(), start: vi.fn(), stop: vi.fn(), type: "", frequency: { value: 0 }, gain: { setValueAtTime: vi.fn(), exponentialRampToValueAtTime: vi.fn() } });
  const ctor = vi.fn(function (this: Record<string, unknown>) {
    this.currentTime = 0;
    this.destination = {};
    this.createOscillator = vi.fn(node);
    this.createGain = vi.fn(node);
  });
  vi.stubGlobal("AudioContext", ctor);
  return ctor;
}

function fakeNotification(permission: NotificationPermission, answer: NotificationPermission = permission) {
  const N = vi.fn() as unknown as { permission: NotificationPermission; requestPermission: () => Promise<NotificationPermission> };
  N.permission = permission;
  N.requestPermission = vi.fn().mockImplementation(async () => {
    N.permission = answer;
    return answer;
  });
  vi.stubGlobal("Notification", N);
  return N;
}

describe("AlertPanel", () => {
  beforeEach(() => {
    localStorage.clear();
    resetAudio();
    seedBoard();
  });
  afterEach(() => vi.unstubAllGlobals());

  it("shouldSayAllQuietWhenThereAreNoAlerts", () => {
    // GIVEN: nothing fired in the last 15 minutes
    // WHEN: the operator looks at the alert panel
    renderWithApi(<AlertPanel />);
    // THEN: a calm empty state is shown and all counters are zero
    expect(screen.getByText(/All quiet/)).toBeInTheDocument();
    expect(screen.queryAllByTestId("alert")).toHaveLength(0);
  });

  it("shouldGroupAlertsByStatusWhenFiringPendingAndResolvedExist", () => {
    // GIVEN: one critical alert fires, one is pending and one resolved two minutes ago
    seedBoard({
      alerts: [
        alert({ fingerprint: "a", name: "KafkaBrokerDown", severity: "critical" }),
        alert({ fingerprint: "b", name: "KafkaConsumerLagHigh", status: "pending" }),
        alert({ fingerprint: "c", name: "LabTestAlert", status: "resolved", severity: "info", endsAt: new Date(Date.now() - 120000).toISOString(), dashboardUrl: undefined, runbookUrl: undefined, generatorUrl: undefined }),
      ],
    });
    // WHEN: the panel renders
    renderWithApi(<AlertPanel />);
    // THEN: each alert is listed in its status group with links where available
    expect(screen.getByRole("heading", { name: /Firing/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Pending/ })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: /Resolved/ })).toBeInTheDocument();
    const cards = screen.getAllByTestId("alert");
    expect(cards).toHaveLength(3);
    expect(within(cards[0]!).getByText("KafkaBrokerDown")).toBeInTheDocument();
    expect(within(cards[0]!).getByRole("link", { name: /Runbook/ })).toHaveAttribute("href", "https://runbooks/lag.md");
    expect(within(cards[2]!).getByText(/resolved 2m 00s ago/)).toBeInTheDocument();
    expect(within(cards[2]!).queryByRole("link")).not.toBeInTheDocument();
  });

  it("shouldShowUnreadBadgeUntilTheOperatorHoversThePanelWhenANewAlertFires", () => {
    // GIVEN: a new firing alert the operator has not seen yet
    seedBoard({ alerts: [alert()], unreadAlerts: ["fp-1"] });
    renderWithApi(<AlertPanel />);
    expect(screen.getByLabelText("1 unread alerts")).toBeInTheDocument();
    // WHEN: the operator moves the mouse over the panel
    fireEvent.mouseEnter(screen.getByLabelText("Alerts"));
    // THEN: the alert counts as read and the badge disappears
    expect(boardStore.getState().unreadAlerts).toEqual([]);
    expect(screen.queryByLabelText(/unread alerts/)).not.toBeInTheDocument();
  });

  it("shouldSendATestAlertWhenTheTestButtonIsPressed", async () => {
    // GIVEN: the alert pipeline should be verified end to end
    const { api } = renderWithApi(<AlertPanel />);
    // WHEN: the operator presses "Test"
    await userEvent.click(screen.getByRole("button", { name: /Test/ }));
    // THEN: the backend fires LabTestAlert and the operator is told what to expect
    expect(api.testAlert).toHaveBeenCalled();
    expect(toastTitles()).toContain("Test alert sent");
  });

  it("shouldReportAnErrorWhenTheTestAlertCannotBeSent", async () => {
    // GIVEN: the backend refuses the request
    const api = fakeApi({ testAlert: vi.fn().mockRejectedValue(new ApiError(500, "internal", "boom")) });
    renderWithApi(<AlertPanel />, api);
    // WHEN: the operator presses "Test"
    await userEvent.click(screen.getByRole("button", { name: /Test/ }));
    // THEN: an error toast appears
    expect(toastTitles()).toContain("Request failed (500)");
  });

  it("shouldBeepWhenSoundIsOnAndANewAlertStartsFiring", async () => {
    // GIVEN: the operator enabled the alarm sound (it confirms with a beep)
    const ctor = fakeAudio();
    renderWithApi(<AlertPanel />);
    await userEvent.click(screen.getByRole("button", { name: "Alert sound" }));
    expect(screen.getByRole("button", { name: "Alert sound" })).toHaveAttribute("aria-pressed", "true");
    expect(localStorage.getItem("kafka-lab:sound")).toBe("true");
    const ctx = ctor.mock.instances[0] as unknown as { createOscillator: ReturnType<typeof vi.fn> };
    const before = ctx.createOscillator.mock.calls.length;
    // WHEN: a critical alert starts firing
    act(() => boardStore.setState({ alerts: [alert({ severity: "critical" })] }));
    // THEN: another two-tone beep is played
    expect(ctx.createOscillator.mock.calls.length).toBe(before + 2);
  });

  it("shouldTurnSoundOffWhenItWasOn", async () => {
    // GIVEN: sound was enabled in an earlier session
    localStorage.setItem("kafka-lab:sound", "true");
    renderWithApi(<AlertPanel />);
    // WHEN: the operator toggles sound
    await userEvent.click(screen.getByRole("button", { name: "Alert sound" }));
    // THEN: it is off and remembered as off
    expect(screen.getByRole("button", { name: "Alert sound" })).toHaveAttribute("aria-pressed", "false");
    expect(localStorage.getItem("kafka-lab:sound")).toBe("false");
  });

  it("shouldNotifyTheBrowserWhenNotificationsAreGrantedAndAnAlertFires", async () => {
    // GIVEN: the browser grants notification permission when asked
    const N = fakeNotification("default", "granted");
    renderWithApi(<AlertPanel />);
    await userEvent.click(screen.getByRole("button", { name: "Browser notifications" }));
    expect(N.requestPermission).toHaveBeenCalled();
    // WHEN: a warning alert starts firing
    act(() => boardStore.setState({ alerts: [alert()] }));
    // THEN: a desktop notification is shown for it
    expect(N).toHaveBeenCalledWith("WARNING: KafkaConsumerLagHigh", expect.objectContaining({ body: alert().summary }));
  });

  it("shouldWarnWhenTheBrowserBlocksNotifications", async () => {
    // GIVEN: the browser denies notification permission
    fakeNotification("default", "denied");
    renderWithApi(<AlertPanel />);
    // WHEN: the operator enables notifications
    await userEvent.click(screen.getByRole("button", { name: "Browser notifications" }));
    // THEN: the toggle stays off and a warning explains why
    expect(screen.getByRole("button", { name: "Browser notifications" })).toHaveAttribute("aria-pressed", "false");
    expect(toastTitles()).toContain("Notifications blocked");
  });

  it("shouldDisableNotificationsWhenTheyWereOn", async () => {
    // GIVEN: notifications were enabled earlier
    localStorage.setItem("kafka-lab:notifications", "true");
    renderWithApi(<AlertPanel />);
    // WHEN: the operator toggles them
    await userEvent.click(screen.getByRole("button", { name: "Browser notifications" }));
    // THEN: they are off and remembered as off
    expect(screen.getByRole("button", { name: "Browser notifications" })).toHaveAttribute("aria-pressed", "false");
    expect(localStorage.getItem("kafka-lab:notifications")).toBe("false");
  });
});
