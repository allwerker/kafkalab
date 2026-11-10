// Package app wires generation and linting of dashboards, rules, rule tests and runbooks.
package app

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"

	"kafkalab/monitoring/dashgen/internal/catalog"
	"kafkalab/monitoring/dashgen/internal/dash"
	"kafkalab/monitoring/dashgen/internal/lint"
	"kafkalab/monitoring/dashgen/internal/rules"
)

// Repository-relative paths.
const (
	BundleDashboards = "monitoring/bundle/dashboards"
	DemoDashboards   = "monitoring/demo/dashboards"
	BundleAlerts     = "monitoring/bundle/alerts"
	DemoAlerts       = "monitoring/demo/alerts"
	BundleCatalog    = "monitoring/bundle/catalog.yaml"
	DemoCatalog      = "monitoring/demo/catalog.yaml"
	MetricsContract  = "docs/contracts/metrics.md"
	Runbooks         = "docs/runbooks"
)

// RuleFile is one generated rule file with its test file.
type RuleFile struct {
	Path     string
	Content  []byte
	TestPath string
	Test     []byte
	// MetricsQL marks rule files evaluated by vmalert only (not PromQL-portable).
	MetricsQL bool
}

// Files returns every generated file (repository-relative path -> content).
func Files() map[string][]byte {
	out := map[string][]byte{}
	for _, b := range dash.All() {
		dir := BundleDashboards
		if b.Set == dash.Demo {
			dir = DemoDashboards
		}
		out[filepath.Join(dir, b.File())] = b.JSON()
	}
	for _, rf := range RuleFiles() {
		out[rf.Path] = rf.Content
		out[rf.TestPath] = rf.Test
	}
	for name, md := range rules.RunbookFiles(append(rules.KafkaGroups(), rules.BusinessGroups()...)) {
		out[filepath.Join(Runbooks, name)] = md
	}
	return out
}

// RuleFiles returns the generated rule files: the bundle in both profiles and the demo business rules.
func RuleFiles() []RuleFile {
	var out []RuleFile
	for _, p := range []rules.Profile{rules.Production, rules.Demo} {
		name := "kafka-" + string(p)
		header := rules.KafkaHeader + "\nProfile: " + string(p) + "."
		out = append(out, RuleFile{
			Path: filepath.Join(BundleAlerts, name+".yaml"), Content: rules.Render(header, rules.KafkaGroups(), p),
			TestPath: filepath.Join(BundleAlerts, name+".test.yaml"),
			Test:     rules.Tests("Unit tests for "+name+".yaml (GENERATED, run: make alerts-test).", name+".yaml", rules.KafkaGroups(), p),
		})
	}
	out = append(out, RuleFile{
		Path: filepath.Join(DemoAlerts, "business.yaml"), Content: rules.Render(rules.BusinessHeader, rules.BusinessGroups(), rules.Demo),
		TestPath:  filepath.Join(DemoAlerts, "business.test.yaml"),
		Test:      rules.Tests("Unit tests for business.yaml (GENERATED, run: make alerts-test).", "business.yaml", rules.BusinessGroups(), rules.Demo),
		MetricsQL: true,
	})
	return out
}

// Generate writes every generated file below root.
func Generate(root string, w io.Writer) error {
	files := Files()
	for _, p := range sortedKeys(files) {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, files[p], 0o644); err != nil {
			return err
		}
	}
	fmt.Fprintf(w, "dashgen: wrote %d files\n", len(files))
	return nil
}

func sortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Lint checks the files on disk below root and returns all problems.
func Lint(root string) []string {
	var problems []string
	read := func(p string) []byte {
		b, err := os.ReadFile(filepath.Join(root, p))
		if err != nil {
			problems = append(problems, err.Error())
		}
		return b
	}
	// stale generated files
	for p, want := range Files() {
		if got, err := os.ReadFile(filepath.Join(root, p)); err != nil || !bytes.Equal(got, want) {
			problems = append(problems, p+": stale or missing — run make dashboards")
		}
	}
	bc, err := catalog.Parse(read(BundleCatalog))
	if err != nil {
		return append(problems, err.Error())
	}
	problems = append(problems, bc.Validate(os.DirFS(filepath.Join(root, filepath.Dir(BundleCatalog))))...)
	dc, err := catalog.Parse(read(DemoCatalog))
	if err != nil {
		return append(problems, err.Error())
	}
	problems = append(problems, dc.Validate(os.DirFS(filepath.Join(root, filepath.Dir(DemoCatalog))))...)
	contract := catalog.ContractNames(read(MetricsContract))

	uids := map[string]bool{}
	for _, b := range dash.All() {
		uids[b.Dash.UID] = true
	}
	bundle := lint.Profile{Bundle: true, Known: bc.Has, UIDs: uids}
	demo := lint.Profile{Known: func(n string) bool { return bc.Has(n) || dc.Has(n) || contract[n] }, UIDs: uids}
	used := map[string]bool{}
	for _, dir := range []string{BundleDashboards, DemoDashboards} {
		prof := bundle
		if dir == DemoDashboards {
			prof = demo
		}
		entries, _ := os.ReadDir(filepath.Join(root, dir)) // a missing directory shows up as stale files above
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			r := lint.Dashboard(read(filepath.Join(dir, e.Name())), prof)
			problems = append(problems, r.Problems...)
			if prof.Bundle {
				merge(used, r.Used)
			}
		}
	}
	for _, rf := range RuleFiles() {
		prof := bundle
		if rf.MetricsQL {
			prof = demo
		}
		r := LintRules(read(rf.Path), prof, root)
		problems = append(problems, r.Problems...)
		if !rf.MetricsQL {
			merge(used, r.Used)
		}
	}
	for _, n := range bc.Unused(used) {
		problems = append(problems, "catalog "+n+": not used by any bundle dashboard or rule (remove it)")
	}
	return problems
}

func merge(dst, src map[string]bool) {
	for k := range src {
		dst[k] = true
	}
}

type ruleFile struct {
	Groups []struct {
		Name  string `yaml:"name"`
		Rules []struct {
			Record      string            `yaml:"record"`
			Alert       string            `yaml:"alert"`
			Expr        string            `yaml:"expr"`
			Labels      map[string]string `yaml:"labels"`
			Annotations map[string]string `yaml:"annotations"`
		} `yaml:"rules"`
	} `yaml:"groups"`
}

var (
	severities = map[string]bool{"critical": true, "warning": true, "info": true}
	components = map[string]bool{"kafka": true, "consumer": true, "business": true, "platform": true, "chaos": true}
)

// LintRules checks a rule file: expressions parse, metrics are known (or recorded in the same
// file), labels and annotations follow the platform contract and runbooks exist.
func LintRules(data []byte, p lint.Profile, root string) lint.Report {
	rep := lint.Report{Used: map[string]bool{}}
	var f ruleFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		rep.Problems = append(rep.Problems, "rules: "+err.Error())
		return rep
	}
	recorded := map[string]bool{}
	for _, g := range f.Groups {
		for _, r := range g.Rules {
			if r.Record != "" {
				recorded[r.Record] = true
			}
		}
	}
	known := p.Known
	p.Known = func(n string) bool { return recorded[n] || known(n) }
	dialect := lint.PromQL
	if !p.Bundle {
		dialect = lint.MetricsQL
	}
	for _, g := range f.Groups {
		for _, r := range g.Rules {
			name := r.Record + r.Alert
			names, err := lint.Metrics(r.Expr, dialect)
			if err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("rule %s: %v", name, err))
			}
			for _, n := range names {
				rep.Used[n] = true
				if !p.Known(n) {
					rep.Problems = append(rep.Problems, fmt.Sprintf("rule %s: metric %s is not documented", name, n))
				}
			}
			if r.Alert == "" {
				continue
			}
			l, a := r.Labels, r.Annotations
			if !severities[l["severity"]] || !components[l["component"]] || !p.UIDs[l["dashboard"]] {
				rep.Problems = append(rep.Problems, fmt.Sprintf("alert %s: labels severity/component/dashboard invalid: %v", name, l))
			}
			if a["summary"] == "" || a["description"] == "" || a["runbook_url"] != rules.RunbookBase+name+".md" {
				rep.Problems = append(rep.Problems, fmt.Sprintf("alert %s: summary, description and runbook_url are required", name))
			}
			if _, err := os.Stat(filepath.Join(root, rules.RunbookBase+name+".md")); err != nil {
				rep.Problems = append(rep.Problems, fmt.Sprintf("alert %s: runbook %s%s.md missing", name, rules.RunbookBase, name))
			}
		}
	}
	return rep
}
