import { afterEach, describe, expect, it, vi } from "vitest";
import { beep, loadPref, notify, requestNotifications, resetAudio, savePref } from "./browser";

afterEach(() => {
  resetAudio();
  vi.unstubAllGlobals();
});

function fakeAudio() {
  const started: number[] = [];
  const param = { value: 0, setValueAtTime: vi.fn(), exponentialRampToValueAtTime: vi.fn() };
  class Ctx {
    currentTime = 1;
    destination = {};
    createOscillator() {
      return { type: "", frequency: { value: 0 }, connect: (g: unknown) => g, start: (t: number) => started.push(t), stop: vi.fn() };
    }
    createGain() {
      return { gain: param, connect: (d: unknown) => d };
    }
  }
  return { Ctx: Ctx as unknown as new () => AudioContext, started };
}

describe("preferences", () => {
  it("shouldRememberTheSoundSettingWhenStorageWorks", () => {
    // GIVEN: working browser storage
    // WHEN: the sound toggle is saved
    savePref("sound", true);
    // THEN: it is loaded on the next visit
    expect(loadPref("sound", false)).toBe(true);
    expect(loadPref("never-set", "fallback")).toBe("fallback");
  });

  it("shouldFallBackToDefaultsWhenStorageIsBlocked", () => {
    // GIVEN: a private window where storage throws
    const blocked = {
      getItem: () => {
        throw new Error("denied");
      },
      setItem: () => {
        throw new Error("denied");
      },
    };
    // WHEN: preferences are saved and loaded
    savePref("tab", "services", blocked);
    // THEN: nothing breaks and the default is used
    expect(loadPref("tab", "scenarios", blocked)).toBe("scenarios");
    expect(loadPref("tab", "scenarios", undefined)).toBe("scenarios");
    expect(() => savePref("tab", "x", undefined)).not.toThrow();
  });
});

describe("alert sound", () => {
  it("shouldPlayATwoToneBeepWhenAnAlertFires", () => {
    // GIVEN: a browser with WebAudio
    const { Ctx, started } = fakeAudio();
    // WHEN: a critical, a warning and an info alert beep
    // THEN: each plays two tones
    expect(beep("critical", Ctx)).toBe(true);
    expect(beep("warning", Ctx)).toBe(true);
    expect(beep("info", Ctx)).toBe(true);
    expect(started).toHaveLength(6);
  });

  it("shouldStaySilentWhenWebAudioIsMissingOrBroken", () => {
    // GIVEN: no AudioContext, and one that throws
    const Broken = class {
      constructor() {
        throw new Error("autoplay blocked");
      }
    } as unknown as new () => AudioContext;
    // WHEN: a beep is requested
    // THEN: it reports failure instead of crashing
    expect(beep("critical", undefined)).toBe(false);
    expect(beep("critical", Broken)).toBe(false);
  });

  it("shouldUseTheBrowserAudioContextWhenNoneIsInjected", () => {
    // GIVEN: a browser exposing AudioContext globally
    const { Ctx, started } = fakeAudio();
    vi.stubGlobal("AudioContext", Ctx);
    // WHEN: a beep plays
    // THEN: the global context is used
    expect(beep("info")).toBe(true);
    expect(started).toHaveLength(2);
  });
});

describe("browser notifications", () => {
  function fakeNotification(permission: NotificationPermission, answer: NotificationPermission | "throw" = "granted") {
    const shown: string[] = [];
    const N = class {
      static permission = permission;
      static requestPermission = vi.fn(async () => {
        if (answer === "throw") throw new Error("no");
        return answer;
      });
      constructor(title: string) {
        shown.push(title);
      }
    };
    return { N: N as never, shown };
  }

  it("shouldAskForPermissionWhenTheOperatorEnablesNotifications", async () => {
    // GIVEN: a browser that has not decided yet and grants on request
    const { N } = fakeNotification("default", "granted");
    // WHEN: permission is requested
    // THEN: it is granted
    await expect(requestNotifications(N)).resolves.toBe(true);
  });

  it("shouldReportDeniedWhenTheUserRefusesOrTheApiFails", async () => {
    // GIVEN: browsers that deny, throw, or lack the API
    // WHEN: permission is requested
    // THEN: notifications stay off
    await expect(requestNotifications(fakeNotification("default", "denied").N)).resolves.toBe(false);
    await expect(requestNotifications(fakeNotification("default", "throw").N)).resolves.toBe(false);
    await expect(requestNotifications(undefined)).resolves.toBe(false);
  });

  it("shouldNotAskAgainWhenPermissionWasAlreadyGranted", async () => {
    // GIVEN: an already granted permission (via the global API)
    const { N } = fakeNotification("granted");
    vi.stubGlobal("Notification", N);
    // WHEN: permission is requested
    // THEN: it resolves immediately
    await expect(requestNotifications()).resolves.toBe(true);
  });

  it("shouldShowANotificationOnlyWhenPermitted", () => {
    // GIVEN: one browser that granted notifications and one that did not
    const granted = fakeNotification("granted");
    const denied = fakeNotification("denied");
    const Throws = class {
      static permission = "granted";
      constructor() {
        throw new Error("blocked");
      }
    } as never;
    // WHEN: an alert notification is shown
    // THEN: only the permitted browser shows it; failures are swallowed
    expect(notify("CRITICAL: KafkaBrokerDown", "Broker 1 is down", granted.N)).toBe(true);
    expect(granted.shown).toEqual(["CRITICAL: KafkaBrokerDown"]);
    expect(notify("x", "y", denied.N)).toBe(false);
    expect(notify("x", "y", undefined)).toBe(false);
    expect(notify("x", "y", Throws)).toBe(false);
  });
});
