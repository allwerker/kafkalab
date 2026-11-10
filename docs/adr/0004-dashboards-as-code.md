# ADR 0004 — Dashboards and alerts as code (monitoring/dashgen)

Status: accepted · 2026-11-10

## Context

The lab ships 16 Grafana dashboards (8 portable bundle, 8 demo) and ~30 alert rules in two
profiles. Hand-edited dashboard JSON drifts quickly (copy-pasted selectors, inconsistent colours,
undocumented panels, broken drill-down links), and nobody reviews 5 000-line JSON diffs. Rules
need a calm production profile and a fast demo profile from one source, with unit tests and
runbooks that cannot fall out of sync.

## Options considered

1. **grafana-foundation-sdk (Go)** — spiked for ~25 minutes with `v0.0.22` (the newest Go module
   tag; the Grafana-version branches like `v12.x+cog-v0.0.x` are not resolvable as Go versions,
   `next+cog-v0.0.x` resolves to a Jan 2026 pseudo-version). Findings:
   - it builds classic dashboards (schemaVersion 42) and has typed builders for every panel kind;
   - partial builders emit invalid defaults: setting a legend writes `showLegend: false`, a tooltip
     gets `mode: ""`, rows get id 0, targets get no `refId` — every panel needs boilerplate to
     undo that;
   - no automatic layout (gridPos per panel by hand), no notion of drill-down links between
     dashboards, no linting hooks;
   - an 86 MB module, ~23 s cold compile for a tiny example.
2. **In-house builder** — a small typed model (`Dashboard`, `Row`, `Panel`, `Target`, `Variable`,
   `Annotation`) rendered to classic JSON (schemaVersion 39 = Grafana 11, migrated by 12/13),
   with automatic row layout, one colour palette and opinionated per-kind defaults.
3. Jsonnet/grafonnet — another language and toolchain, no reuse of the Go test/lint setup.

## Decision

**In-house builder** (`monitoring/dashgen/internal/grafana`, ~500 lines) plus:

- `internal/dash` — every dashboard as Go code. Selectors are written once as tokens (`@B` =
  cluster + broker filter, `@KG` = cluster + group + topic, …) and expanded at build time.
- `internal/rules` — alerting/recording rules with a test scenario per alert; renders the
  production and demo rule files, vmalert-tool/promtool unit tests (exact labels and annotations)
  and `docs/runbooks/<alert>.md`.
- `internal/lint` + `internal/catalog` — `make dashboards-lint` re-reads the committed files and
  fails on: stale generated output, queries that do not parse (Prometheus' own PromQL parser for
  the bundle, the MetricsQL parser for the demo, a structural check for LogsQL), metrics missing
  from `monitoring/bundle/catalog.yaml` (bundle) / metrics.md + `monitoring/demo/catalog.yaml`
  (demo), catalog entries no longer used or absent from the recorded fixtures, panels without
  description, unit or (for stat/gauge/bar gauge/table/state panels) thresholds, bundle panels not
  on the `${datasource}` prometheus variable, drill-down links to unknown dashboard UIDs, alerts
  without severity/component/dashboard/summary/description/runbook.

Generated files are committed (Grafana provisioning and helmfile read them directly, reviewers see
the JSON diff) and regenerated with `make dashboards` (alias `make alerts`).

## Consequences

- Adding a panel is a few lines of Go; consistency (palette: one hue per panel, red only for bad;
  descriptions; units; links keeping `${__url_time_range}`, datasource and cluster) is enforced.
- The builder covers only the panel kinds used (timeseries, stat, gauge, bar gauge, table,
  heatmap, state timeline, status history, pie chart, text, logs, dashboard list); new kinds
  need a few lines in `render.go`.
- Dependencies added to `go.mod`: `github.com/prometheus/prometheus` (PromQL parser; pulls a newer
  grpc/x-sys/genproto through MVS), `github.com/VictoriaMetrics/metricsql`, `go.yaml.in/yaml/v3`.
- Tests: `monitoring/dashgen/...` is under the 100 % coverage gate; rule behaviour is tested by
  `make alerts-test` (vmalert-tool for everything, promtool additionally for the PromQL bundle).
