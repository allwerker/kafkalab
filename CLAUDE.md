# Kafka Lab — conventions for humans and agents

Kafka monitoring showcase: commerce simulation (Go + franz-go) on Kafka (Strimzi, KRaft) in k3d,
metrics in VictoriaMetrics, dashboards in Grafana, gamified controlboard (React + Phaser).

## Golden rules
- Everything runs through `make` and `./kctl`. Never use a system kubectl/helm/go; the pinned
  toolchain is in `.tools/` (`make bootstrap`). Versions: `tools/versions.env`.
- Contracts in `docs/contracts/`, `schemas/`, `deploy/topics.yaml`, `pkg/contracts/` are FROZEN.
  Changing them needs the lead's approval (write a change request in the ticket file).
- `pkg/kit` has one owner (the lead). Service agents file change requests instead of editing it.
- One commit per ticket, message `T123: <summary>`.
- Code, identifiers and comments in English.

## Tests (mandatory style)
- Go test names: `func Test_should<ExpectedBehaviour>When<Condition>(t *testing.T)`.
  TypeScript: `it("should<ExpectedBehaviour>When<Condition>", ...)`. Enforced by `make lint`.
- Every test body has `// GIVEN: ...`, `// WHEN: ...`, `// THEN: ...` comments that describe the
  business situation, readable without the code.
  good: `// GIVEN: an order whose payment failed after its stock was reserved`
  bad:  `// GIVEN: setup mocks`
- Prefer business scenarios. Domain packages: pure Go, no kit imports, 100% statement coverage.
- Adapters: real PostgreSQL via `pkg/kit/testkit` (Docker), Kafka via franz-go `kfake`.
- Kit tests run with `-race`. No tests that exist only to raise coverage; unreachable branches
  are deleted or listed in `docs/coverage-exceptions.md` with a reason.

## Layout
- `services/<name>/internal/domain` pure rules, `internal/app` handlers, `internal/adapters` db,
  `main.go` wiring only.
- `pkg/events` generated from `schemas/*.avdl` (`make gen`); never edit by hand.
- `monitoring/bundle` is the portable product: no demo-specific labels, PromQL only.
- Each Helm release has its own file in `deploy/helmfile.d/`; each Make area its own `mk/*.mk`.

## Useful commands
`make help` · `make up` · `make status` · `./kctl lab topics` · `make test` · `make ci`
