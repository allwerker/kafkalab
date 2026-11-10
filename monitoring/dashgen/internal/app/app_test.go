package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kafkalab/monitoring/dashgen/internal/lint"
)

// repoRoot is the repository root seen from this package directory.
const repoRoot = "../../../.."

func Test_shouldFindNoProblemsWhenCheckingTheCommittedMonitoringArtifacts(t *testing.T) {
	// GIVEN: the committed dashboards, rules, tests, runbooks, catalogs and fixtures of the repository
	root := repoRoot

	// WHEN: everything is linted
	problems := Lint(root)

	// THEN: generated files are up to date and every check passes
	if len(problems) > 0 {
		t.Errorf("lint problems (run make dashboards):\n%s", strings.Join(problems, "\n"))
	}
}

func Test_shouldWriteEveryGeneratedFileWhenGeneratingIntoAnEmptyTree(t *testing.T) {
	// GIVEN: an empty directory
	root := t.TempDir()
	var out bytes.Buffer

	// WHEN: dashgen generates into it
	err := Generate(root, &out)

	// THEN: all dashboards, rule files, rule tests and runbooks exist with the generated content
	if err != nil || !strings.Contains(out.String(), "wrote") {
		t.Fatalf("generate failed: %v %s", err, out.String())
	}
	for p, want := range Files() {
		got, err := os.ReadFile(filepath.Join(root, p))
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("%s not written correctly", p)
		}
	}
	if len(RuleFiles()) != 3 {
		t.Errorf("want production, demo and business rule files")
	}
}

func Test_shouldFailWhenTheTargetDirectoryCannotBeCreated(t *testing.T) {
	// GIVEN: a "repository root" that is a regular file
	root := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(root, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	// WHEN: dashgen generates into it
	err := Generate(root, &bytes.Buffer{})

	// THEN: the error is returned
	if err == nil {
		t.Error("want an error")
	}
}

func Test_shouldFailWhenAGeneratedFileCannotBeWritten(t *testing.T) {
	// GIVEN: a tree where the first generated file's path is occupied by a directory
	root := t.TempDir()
	first := filepath.Join(root, sortedKeys(Files())[0])
	if err := os.MkdirAll(first, 0o755); err != nil {
		t.Fatal(err)
	}

	// WHEN: dashgen generates into it
	err := Generate(root, &bytes.Buffer{})

	// THEN: the write error is returned
	if err == nil {
		t.Error("want an error")
	}
}

func Test_shouldReportStaleFilesAndBrokenCatalogWhenTheTreeIsIncomplete(t *testing.T) {
	// GIVEN: a tree without generated files and with an unparsable bundle catalog
	root := t.TempDir()
	write(t, root, BundleCatalog, "metrics: [")

	// WHEN: it is linted
	out := strings.Join(Lint(root), "\n")

	// THEN: missing generated files and the catalog error are reported
	if !strings.Contains(out, "stale or missing") || !strings.Contains(out, "catalog:") {
		t.Errorf("unexpected:\n%s", out)
	}
}

func Test_shouldReportBrokenDemoCatalogWhenTheBundleCatalogIsFine(t *testing.T) {
	// GIVEN: generated files, a valid empty bundle catalog and a broken demo catalog
	root := t.TempDir()
	if err := Generate(root, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	write(t, root, BundleCatalog, "metrics: {}\n")
	write(t, root, DemoCatalog, "metrics: [")

	// WHEN: it is linted
	out := strings.Join(Lint(root), "\n")

	// THEN: the demo catalog error is reported
	if !strings.Contains(out, "catalog:") {
		t.Errorf("unexpected:\n%s", out)
	}
}

func Test_shouldReportUndocumentedMetricsWhenTheCatalogsAreEmpty(t *testing.T) {
	// GIVEN: generated files, a bundle catalog with only an unused entry, no metric contract and a stray non-JSON file
	root := t.TempDir()
	if err := Generate(root, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	write(t, root, BundleCatalog, "metrics:\n  orphan_total: {source: rules, type: counter, meaning: \"Nobody uses this metric.\"}\n")
	write(t, root, DemoCatalog, "metrics: {}\n")
	write(t, root, filepath.Join(BundleDashboards, "README.txt"), "not a dashboard")

	// WHEN: it is linted
	out := strings.Join(Lint(root), "\n")

	// THEN: undocumented metrics, the unused entry and the missing contract file are reported
	for _, want := range []string{"kafka-overview panel", "rule KafkaBrokerDown: metric", "metrics.md", "catalog orphan_total: not used"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}

func write(t *testing.T, root, p, content string) {
	t.Helper()
	full := filepath.Join(root, p)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func Test_shouldRejectRulesWhenTheyBreakTheAlertContract(t *testing.T) {
	// GIVEN: a rule file with a broken expression, an unknown metric, bad labels and no runbook
	data := []byte(`groups:
- name: g
  rules:
  - record: r:ok
    expr: sum(known)
  - alert: Broken
    expr: sum(
  - alert: Sloppy
    expr: r:ok > mystery
    labels: {severity: page, component: kafka, dashboard: kafka-overview}
    annotations: {summary: s}
`)
	p := lint.Profile{Bundle: true, Known: func(n string) bool { return n == "known" }, UIDs: map[string]bool{"kafka-overview": true}}

	// WHEN: the rules are linted
	out := strings.Join(LintRules(data, p, t.TempDir()).Problems, "\n")

	// THEN: each violation is named
	for _, want := range []string{"rule Broken: PromQL", "metric mystery is not documented", "alert Sloppy: labels", "summary, description and runbook_url", "runbook docs/runbooks/Sloppy.md missing"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func Test_shouldReportYAMLErrorWhenTheRuleFileIsCorrupt(t *testing.T) {
	// GIVEN: a corrupt rule file
	data := []byte("groups: [")

	// WHEN: it is linted
	r := LintRules(data, lint.Profile{Known: func(string) bool { return true }}, t.TempDir())

	// THEN: the YAML error is the only problem
	if len(r.Problems) != 1 || !strings.HasPrefix(r.Problems[0], "rules:") {
		t.Errorf("got %v", r.Problems)
	}
}
