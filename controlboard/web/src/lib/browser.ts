/** Thin, failure-tolerant wrappers around browser APIs (storage, WebAudio, notifications). */

export function loadPref<T>(key: string, fallback: T, storage: Pick<Storage, "getItem"> | undefined = globalThis.localStorage): T {
  try {
    const raw = storage?.getItem(`kafka-lab:${key}`);
    return raw == null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function savePref(key: string, value: unknown, storage: Pick<Storage, "setItem"> | undefined = globalThis.localStorage): void {
  try {
    storage?.setItem(`kafka-lab:${key}`, JSON.stringify(value));
  } catch {
    /* private mode or blocked storage: preference is not remembered */
  }
}

type AudioCtor = new () => AudioContext;
let ctx: AudioContext | null = null;

/** Short two-tone alarm beep; severity picks the pitch. Returns false if audio is unavailable. */
export function beep(severity: "critical" | "warning" | "info", Ctor: AudioCtor | undefined = (globalThis as { AudioContext?: AudioCtor }).AudioContext): boolean {
  if (!Ctor) return false;
  try {
    ctx ??= new Ctor();
    const base = severity === "critical" ? 880 : severity === "warning" ? 660 : 520;
    const t0 = ctx.currentTime;
    [0, 0.18].forEach((offset, i) => {
      const osc = ctx!.createOscillator();
      const gain = ctx!.createGain();
      osc.type = "triangle";
      osc.frequency.value = base * (i === 0 ? 1 : 1.25);
      gain.gain.setValueAtTime(0.0001, t0 + offset);
      gain.gain.exponentialRampToValueAtTime(0.15, t0 + offset + 0.02);
      gain.gain.exponentialRampToValueAtTime(0.0001, t0 + offset + 0.16);
      osc.connect(gain).connect(ctx!.destination);
      osc.start(t0 + offset);
      osc.stop(t0 + offset + 0.17);
    });
    return true;
  } catch {
    return false;
  }
}

/** Test hook: forget the shared AudioContext. */
export function resetAudio(): void {
  ctx = null;
}

type NotificationApi = { permission: NotificationPermission; requestPermission(): Promise<NotificationPermission> } & (new (title: string, opts?: NotificationOptions) => unknown);

function notificationApi(): NotificationApi | undefined {
  return (globalThis as { Notification?: NotificationApi }).Notification;
}

/** Asks for notification permission; resolves true when granted. */
export async function requestNotifications(N: NotificationApi | undefined = notificationApi()): Promise<boolean> {
  if (!N) return false;
  if (N.permission === "granted") return true;
  try {
    return (await N.requestPermission()) === "granted";
  } catch {
    return false;
  }
}

export function notify(title: string, body: string, N: NotificationApi | undefined = notificationApi()): boolean {
  if (!N || N.permission !== "granted") return false;
  try {
    new N(title, { body, tag: title });
    return true;
  } catch {
    return false;
  }
}
