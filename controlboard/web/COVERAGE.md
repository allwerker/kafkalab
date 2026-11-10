# Coverage policy — controlboard web UI

`bun run test` (= `make web-test`) runs Vitest with v8 coverage and enforces thresholds per glob
(configured in `vite.config.ts`):

| Glob | Lines | Branches | Why |
|---|---|---|---|
| `src/live/model/**` | 100 % | 100 % | Pure scene model of the live view (lanes, piles, parcels, trucks, brokers, weather, hit testing). |
| `src/api/**` | 100 % | 100 % | REST client, WebSocket reconnect/backoff, contract fixture guard. |
| `src/store/**` | 100 % | 100 % | Reducer, zustand store, connection wiring, toasts. |
| `src/lib/**` | 100 % | 100 % | Pure helpers (formatting, domain rules, browser API wrappers, topology, EventBus emitter). |
| `src/components/**` | ≥ 90 % | – | React components, tested through user interactions with Testing Library. |

## Excluded from coverage

| Path | Reason |
|---|---|
| `src/live/render/**` | Thin Phaser render layer (`LiveScene.ts`, `game.ts`). It only draws what the pure `LiveModel` computes and needs WebGL/canvas, which jsdom does not provide. It is exercised by the browser smoke test (`bun run smoke`), which fails on any console error. |
| `src/main.tsx` | Bootstrap only (mounts `<App/>`, starts the connection). Covered by the smoke test. |
| `src/api/types.ts` | Type declarations copied from the contract; no runtime code. |
| `src/test/**`, `*.test.ts(x)` | Test helpers and tests. |
| `mock/**`, `scripts/**` | Dev tooling (mock backend, fixture recorder, smoke and lint scripts), outside `src/`. The mock's output is guarded by `src/api/fixtures.test.ts`, which checks every recorded fixture field by field against the contract. |

`src/live/EventBus.ts` is a typed instance of `src/lib/emitter.ts` and is covered via the component tests.
