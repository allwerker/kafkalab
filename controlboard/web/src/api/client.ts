/** Typed REST client for the controlboard API (docs/contracts/controlboard-api.md). */
import type {
  Alert,
  Difficulty,
  Drill,
  Experiment,
  KnobsPatch,
  LeaderboardEntry,
  Link,
  OrderTimeline,
  ScenarioDef,
  ServiceState,
  StartExperimentRequest,
  State,
  TimelineEntry,
  TuningResponse,
  TuningUpdate,
} from "./types";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

type Fetch = (input: string, init?: RequestInit) => Promise<Response>;

export function createApi(fetchImpl: Fetch = (i, init) => fetch(i, init), base = "") {
  async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const init: RequestInit = { method, headers: { Accept: "application/json" } };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
      init.headers = { ...init.headers, "Content-Type": "application/json" };
    }
    let res: Response;
    try {
      res = await fetchImpl(`${base}${path}`, init);
    } catch (e) {
      throw new ApiError(0, "network_error", `Backend not reachable: ${(e as Error).message}`);
    }
    if (!res.ok) {
      let code = `http_${res.status}`;
      let message = res.statusText || `HTTP ${res.status}`;
      try {
        const err = (await res.json()) as { error?: string; message?: string };
        if (err.error) code = err.error;
        if (err.message) message = err.message;
      } catch {
        /* non-JSON error body */
      }
      throw new ApiError(res.status, code, message);
    }
    if (res.status === 204) return undefined as T;
    return (await res.json()) as T;
  }

  const enc = encodeURIComponent;
  return {
    state: () => request<State>("GET", "/api/state"),
    scenarios: () => request<ScenarioDef[]>("GET", "/api/scenarios"),
    startExperiment: (req: StartExperimentRequest) => request<Experiment>("POST", "/api/experiments", req),
    stopExperiment: (id: string) => request<Experiment>("DELETE", `/api/experiments/${enc(id)}`),
    armChaos: (armed: boolean) => request<{ armed: boolean }>("POST", "/api/chaos/arm", { armed }),
    panic: () => request<{ reverted: number }>("POST", "/api/panic"),
    simulation: (action: "start" | "stop" | "reset") => request<{ simulation: State["simulation"] }>("POST", "/api/simulation", { action }),
    putKnobs: (service: string, knobs: KnobsPatch) => request<ServiceState>("PUT", `/api/services/${enc(service)}/knobs`, knobs),
    resetKnobs: (service: string) => request<ServiceState>("DELETE", `/api/services/${enc(service)}/knobs`),
    scale: (service: string, replicas: number) => request<ServiceState>("POST", `/api/services/${enc(service)}/scale`, { replicas }),
    timeline: (limit = 200) => request<TimelineEntry[]>("GET", `/api/timeline?limit=${limit}`),
    alerts: () => request<Alert[]>("GET", "/api/alerts"),
    testAlert: () => request<void>("POST", "/api/alerts/test"),
    links: () => request<Link[]>("GET", "/api/links"),
    tuning: () => request<TuningResponse>("GET", "/api/tuning"),
    putTuning: (service: string, values: TuningUpdate) => request<TuningResponse>("PUT", `/api/tuning/${enc(service)}`, values),
    startDrill: (difficulty: Difficulty, player: string) => request<Drill>("POST", "/api/drills", { difficulty, player }),
    hint: (id: string) => request<Drill>("POST", `/api/drills/${enc(id)}/hint`),
    guess: (id: string, scenario: string) => request<Drill>("POST", `/api/drills/${enc(id)}/guess`, { scenario }),
    reveal: (id: string) => request<Drill>("POST", `/api/drills/${enc(id)}/reveal`),
    leaderboard: () => request<LeaderboardEntry[]>("GET", "/api/drills/leaderboard"),
    order: (orderId: string) => request<OrderTimeline>("GET", `/api/orders/${enc(orderId)}`),
  };
}

export type Api = ReturnType<typeof createApi>;

/** Default client used by the app (same origin; Vite proxies /api in dev). */
export const api: Api = createApi();
