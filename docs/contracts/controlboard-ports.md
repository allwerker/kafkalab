# Controlboard ports (seam between board and experiment engine)

Source of truth: `controlboard/backend/internal/api/ports.go` (Go doc comments are normative);
wire types: `controlboard/backend/internal/api/types.go` (mirror of `controlboard-api.md`).

```
            HTTP / WebSocket (internal/board, T207/T210)
                 │ calls                     ▲ calls
                 ▼                           │
   api.Experiments + api.DrillAPI       api.Observer
   (internal/experiments, engine)       (board: VictoriaMetrics, alerts, lag)
```

## Ownership
| Package | Owner |
|---|---|
| `internal/api` (types, ports, `fakeexp`) | board (T207). Changes to `ports.go` are agreed between both agents. |
| `internal/board`, `internal/feed`, `internal/alerts`, `internal/observe`, `internal/links`, `internal/hub`, `internal/grafana`, `web/` | board (T207/T210) |
| `internal/experiments`, `internal/k8s`, `internal/drills`, `internal/tuning` | engine |
| `cmd/controlboard/main.go` | board; the engine replaces `fakeexp.New()` with its constructor |

## `api.Experiments` (implemented by the engine)
| Method | Used by | Notes |
|---|---|---|
| `Scenarios()` | `GET /api/scenarios` | catalog in display order |
| `Start(ctx, StartRequest)` | `POST /api/experiments` | errors below |
| `Stop(ctx, id)` | `DELETE /api/experiments/{id}` | `ErrNotFound` |
| `List()` | `State.experiments` | active + last 20, newest first |
| `Arm(ctx, bool)`, `Armed()` | `POST /api/chaos/arm`, `State.chaosArmed` | |
| `Panic(ctx)` | `POST /api/panic` | returns number reverted, disarms |
| `Simulation(ctx, action)`, `SimulationState()` | `POST /api/simulation`, `State.simulation` | `start\|stop\|reset` → `running\|stopped` |
| `Controls()` | `ServiceState.paused/desiredGeneration/knobs` | one entry per commerce service |
| `SetKnobs`, `ResetKnobs` | `PUT/DELETE /api/services/{name}/knobs` | manual override, no expiry; `nil` value removes one key |
| `Scale(ctx, svc, n)` | `POST /api/services/{name}/scale` | board validates 0..6 first |
| `Tuning()`, `SetTuning` | `GET /api/tuning`, `PUT /api/tuning/{service}` | |
| `Drills()` | `/api/drills/**` | `Start`, `Hint`, `Guess`, `Reveal`, `Leaderboard`, `Current` |
| `Subscribe()` | WebSocket `experiment`, `timeline`, `drill` | buffered channel, never blocks the engine |

The HTTP response for knob/scale calls is the board's `ServiceState` (observed replicas from
kube-state-metrics merged with `Controls()`), so the engine never builds `ServiceState` itself.

## Errors (`errors.Is` on wrapped sentinels)
| Sentinel | HTTP | `error` code |
|---|---|---|
| `ErrChaosDisarmed` | 409 | `chaos_disarmed` |
| `ErrBrokerFaultActive` | 409 | `broker_fault_active` |
| `ErrConfirmationRequired` | 400 | `confirmation_required` |
| `ErrUnknownScenario` | 404 | `unknown_scenario` |
| `ErrNotFound` | 404 | `not_found` |
| `ErrUnknownService` | 404 | `unknown_service` |
| `ErrInvalid` | 400 | `invalid_request` |
| `ErrConflict` | 409 | `conflict` |
| `ErrDrillActive` | 409 | `drill_active` |
| `ErrDrillFinished` | 409 | `drill_finished` |
| `ErrUnavailable` | 503 | `unavailable` |
| anything else | 500 | `internal` |

`message` is `err.Error()`, so wrap with a human sentence: `fmt.Errorf("%w: broker 1 is already stopped", api.ErrBrokerFaultActive)`.

## Agreed behaviour (lead + frontend)
- `autoRecover=false`: the fault stays after `endsAt`, status stays `running` until `Stop`.
- `Stop` → status `aborted`.
- Broker params are numbers 0..2 (`options` lists them as strings).
- `Experiment.category` / `severity` (optional) carry the scenario's values; empty during an unsolved drill.
- `ServiceControl.Knobs` = effective flat knobs only, without Tuning Lab client settings.
- `Drill.scenario` = scenario id (after solved/revealed); `candidates` carry id + name.
- Board side: a pending alert that clears is sent as `resolved`; pong is exactly `{"type":"pong"}`.

## Wiring
The engine needs `Observer` at construction and the board needs the engine, so `main.go` does
`b := board.New(...)`, `exp := experiments.New(..., b.Observer())`, `b.SetExperiments(exp)`
before serving (today: `b.SetExperiments(fakeexp.New(nil))`).

## Events (`api.Event`, exactly one field set)
- `Experiment` on every status/observation/report change → WebSocket `experiment`.
- `Timeline` for every new entry the engine writes (experiment, config, recovery, simulation,
  drill, system kinds). The board stores it (GET /api/timeline) and sends WebSocket `timeline`.
  The board itself writes the `alert` entries. `id`/`at` may be left empty; the board fills them.
- `Drill` on every change → WebSocket `drill`.

## `api.Observer` (implemented by the board, passed to the engine's constructor)
`Kpis()`, `Brokers()`, `Services()`, `Alerts()` (active + resolved in the last 15 min),
`TotalLag()`. Values are refreshed every 2 s (VictoriaMetrics + Kafka admin API), alerts on
change (Alertmanager webhook) and every 5 s (vmalert pending). Use it for "requested vs
observed" (`Broker 1 stopped` → `Brokers()[1].Up == false`) and for reports (time to detect =
first alert firing after start, peak lag = max `TotalLag()` during the run).

## Fake
`internal/api/fakeexp` is an in-memory implementation with simple, deterministic behaviour
(experiments run until stopped or their duration passes, drills accept the scenario id
`fake-scenario`). The board's tests and `main.go` use it until the engine is merged.
