/** vi.fn()-based Api double; every call resolves to something harmless unless overridden. */
import { vi } from "vitest";
import type { Api } from "../api/client";
import { drill, experiment, service, tuning } from "./fixtures";

export type FakeApi = { [K in keyof Api]: ReturnType<typeof vi.fn> & Api[K] };

export function fakeApi(over: Partial<Record<keyof Api, unknown>> = {}): FakeApi {
  const base: Record<keyof Api, unknown> = {
    state: vi.fn(),
    scenarios: vi.fn().mockResolvedValue([]),
    startExperiment: vi.fn().mockResolvedValue(experiment()),
    stopExperiment: vi.fn().mockResolvedValue(experiment({ status: "aborted" })),
    armChaos: vi.fn().mockImplementation(async (armed: boolean) => ({ armed })),
    panic: vi.fn().mockResolvedValue({ reverted: 3 }),
    simulation: vi.fn().mockImplementation(async (action: string) => ({ simulation: action === "stop" ? "stopped" : "running" })),
    putKnobs: vi.fn().mockResolvedValue(service()),
    resetKnobs: vi.fn().mockResolvedValue(service()),
    scale: vi.fn().mockResolvedValue(service({ replicas: 3 })),
    timeline: vi.fn().mockResolvedValue([]),
    alerts: vi.fn().mockResolvedValue([]),
    testAlert: vi.fn().mockResolvedValue(undefined),
    links: vi.fn().mockResolvedValue([]),
    tuning: vi.fn().mockResolvedValue(tuning()),
    putTuning: vi.fn().mockResolvedValue(tuning()),
    startDrill: vi.fn().mockResolvedValue(drill()),
    hint: vi.fn().mockResolvedValue(drill()),
    guess: vi.fn().mockResolvedValue(drill()),
    reveal: vi.fn().mockResolvedValue(drill()),
    leaderboard: vi.fn().mockResolvedValue([]),
    order: vi.fn(),
  };
  return { ...base, ...over } as unknown as FakeApi;
}
