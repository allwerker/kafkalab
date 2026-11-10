# Coverage exceptions

Target: 100% line and branch coverage where it makes business or technical sense.

## Go: statement coverage instead of branch coverage
The Go toolchain (1.27) measures statement coverage only; there is no maintained branch
coverage tool. The gate (`.testcoverage.yml`, vladopajic/go-test-coverage) requires **100%
statement coverage per file**. Branches are covered in practice because every `if`/`switch` arm
contains statements; conditions combined with `&&`/`||` are covered by business test tables.

## Excluded files
| Path | Reason |
|---|---|
| `*_gen.go` | generated from `schemas/*.avdl` |
| `*/main.go` | wiring only; exercised by the end-to-end tests |
| `pkg/kit/testkit` | test infrastructure |

## Annotated blocks (`// coverage-ignore: <reason>`)
Only for infrastructure failures that cannot be provoked meaningfully in a test, every one with
its reason inline (`force-annotation-comment: true`). Typical cases:
- connection loss in the middle of an open PostgreSQL transaction (the batch is retried anyway);
- client constructors that only fail for invalid static options;
- log-only branches after a failed offset commit (redelivery + deduplication cover correctness);
- the process entry point `app.Run` (signals, `os.Exit`).

List them with `grep -rn "coverage-ignore" pkg services controlboard`.

## Frontend
See `controlboard/web/COVERAGE.md` (the Phaser render layer is excluded; the scene model is at 100%).

## Not measurable
Shell scripts (`kctl`, `tools/*.sh`), Helm templates and dashboard JSON have no coverage metric;
they are covered by smoke tests, `make template`, chart schema validation and dashboard lint.
