// Package rules is the single source of the Kafka alerting and recording rules. It renders
// Prometheus rule files in two profiles (production: calm `for:` durations, demo: fast) and the
// matching unit-test files for vmalert-tool / promtool.
package rules

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Profile selects the `for:` durations.
type Profile string

// The two profiles.
const (
	Production Profile = "production"
	Demo       Profile = "demo"
)

// RunbookBase is the path prefix of runbook URLs (docs/runbooks/<alert>.md in this repository).
const RunbookBase = "docs/runbooks/"

// Record is a recording rule.
type Record struct {
	Name string
	Expr string
}

// Alert is an alerting rule with a test scenario.
type Alert struct {
	Name        string
	Expr        string
	ForProd     time.Duration
	ForDemo     time.Duration
	Severity    string // critical|warning|info
	Component   string // kafka|consumer|business|platform|chaos
	Dashboard   string // dashboard uid
	Summary     string
	Description string // may use {{ $labels.<name> }}
	Test        Scenario
}

// Series is one input series of a test scenario. Values may contain %d, replaced by the number
// of samples the scenario needs ("0+10x%d" grows forever, "1x%d" is constant).
type Series struct {
	Series string
	Values string
}

// Scenario is a firing test case: Warmup is the time the expression needs to become true (range
// windows); the alert is evaluated at Warmup + for + 1m. Healthy series are evaluated at the
// same time and must not fire.
type Scenario struct {
	Warmup  time.Duration
	Firing  []Series
	Labels  map[string]string // expected output labels of the expression
	Healthy []Series
}

// Group is a rule group.
type Group struct {
	Name     string
	Interval string
	Records  []Record
	Alerts   []Alert
}

// For returns the `for:` duration of the alert in a profile.
func (a Alert) For(p Profile) time.Duration {
	if p == Production {
		return a.ForProd
	}
	return a.ForDemo
}

// Labels are the static labels of the alert (platform contract).
func (a Alert) Labels() map[string]string {
	return map[string]string{"severity": a.Severity, "component": a.Component, "dashboard": a.Dashboard}
}

// Annotations of the alert (platform contract).
func (a Alert) Annotations() map[string]string {
	return map[string]string{
		"summary": a.Summary, "description": a.Description, "runbook_url": RunbookBase + a.Name + ".md",
	}
}

// ---------------------------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------------------------

type ruleOut struct {
	Record      string            `yaml:"record,omitempty"`
	Alert       string            `yaml:"alert,omitempty"`
	Expr        string            `yaml:"expr"`
	For         string            `yaml:"for,omitempty"`
	Labels      map[string]string `yaml:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty"`
}

type groupOut struct {
	Name     string    `yaml:"name"`
	Interval string    `yaml:"interval,omitempty"`
	Rules    []ruleOut `yaml:"rules"`
}

type fileOut struct {
	Groups []groupOut `yaml:"groups"`
}

// Duration formats a duration the Prometheus way (1h30m, 45s, 0s).
func Duration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	s := d.String()
	s = strings.Replace(s, "m0s", "m", 1)
	s = strings.Replace(s, "h0m", "h", 1)
	return s
}

// Render renders the groups as a Prometheus rule file for a profile.
func Render(header string, groups []Group, p Profile) []byte {
	var f fileOut
	for _, g := range groups {
		out := groupOut{Name: g.Name, Interval: g.Interval}
		for _, r := range g.Records {
			out.Rules = append(out.Rules, ruleOut{Record: r.Name, Expr: r.Expr})
		}
		for _, a := range g.Alerts {
			r := ruleOut{Alert: a.Name, Expr: a.Expr, Labels: a.Labels(), Annotations: a.Annotations()}
			if d := a.For(p); d > 0 {
				r.For = Duration(d)
			}
			out.Rules = append(out.Rules, r)
		}
		f.Groups = append(f.Groups, out)
	}
	return marshal(header, f)
}

func marshal(header string, v any) []byte {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimSpace(header), "\n") {
		b.WriteString("# " + l + "\n")
	}
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	_ = enc.Encode(v) // plain structs of strings cannot fail to encode
	return []byte(b.String())
}

// ---------------------------------------------------------------------------------------------
// Unit tests (vmalert-tool unittest / promtool test rules format)
// ---------------------------------------------------------------------------------------------

type testSeries struct {
	Series string `yaml:"series"`
	Values string `yaml:"values"`
}

type expAlert struct {
	ExpLabels      map[string]string `yaml:"exp_labels"`
	ExpAnnotations map[string]string `yaml:"exp_annotations"`
}

type alertTest struct {
	EvalTime  string     `yaml:"eval_time"`
	Alertname string     `yaml:"alertname"`
	ExpAlerts []expAlert `yaml:"exp_alerts"`
}

type testCase struct {
	Name        string       `yaml:"name"`
	Interval    string       `yaml:"interval"`
	InputSeries []testSeries `yaml:"input_series"`
	AlertTests  []alertTest  `yaml:"alert_rule_test"`
}

type testFile struct {
	RuleFiles          []string   `yaml:"rule_files"`
	EvaluationInterval string     `yaml:"evaluation_interval"`
	Tests              []testCase `yaml:"tests"`
}

const step = 15 * time.Second

var labelTmpl = regexp.MustCompile(`\{\{\s*\$labels\.(\w+)\s*\}\}`)

// Expand substitutes {{ $labels.x }} in an annotation template.
func Expand(tmpl string, labels map[string]string) string {
	return labelTmpl.ReplaceAllStringFunc(tmpl, func(m string) string {
		return labels[labelTmpl.FindStringSubmatch(m)[1]]
	})
}

func series(ss []Series, samples int) []testSeries {
	out := []testSeries{}
	for _, s := range ss {
		v := s.Values
		if strings.Contains(v, "%d") {
			v = fmt.Sprintf(v, samples)
		}
		out = append(out, testSeries{Series: s.Series, Values: v})
	}
	return out
}

// Tests renders the unit-test file for the alerts of the groups in a profile. Every alert gets a
// firing case (exact labels and annotations) and, if it has healthy series, a silent case.
func Tests(header, ruleFile string, groups []Group, p Profile) []byte {
	f := testFile{RuleFiles: []string{ruleFile}, EvaluationInterval: Duration(step)}
	for _, g := range groups {
		for _, a := range g.Alerts {
			eval := a.Test.Warmup + a.For(p) + time.Minute
			samples := int(eval/step) + 20
			labels := map[string]string{"alertname": a.Name}
			for k, v := range a.Test.Labels {
				labels[k] = v
			}
			for k, v := range a.Labels() {
				labels[k] = v
			}
			ann := map[string]string{}
			for k, v := range a.Annotations() {
				ann[k] = Expand(v, labels)
			}
			f.Tests = append(f.Tests, testCase{
				Name: a.Name + " fires", Interval: Duration(step),
				InputSeries: series(a.Test.Firing, samples),
				AlertTests: []alertTest{{EvalTime: Duration(eval), Alertname: a.Name, ExpAlerts: []expAlert{
					{ExpLabels: withoutName(labels), ExpAnnotations: ann},
				}}},
			})
			if len(a.Test.Healthy) > 0 {
				f.Tests = append(f.Tests, testCase{
					Name: a.Name + " stays silent when healthy", Interval: Duration(step),
					InputSeries: series(a.Test.Healthy, samples),
					AlertTests:  []alertTest{{EvalTime: Duration(eval), Alertname: a.Name, ExpAlerts: []expAlert{}}},
				})
			}
		}
	}
	return marshal(header, f)
}

func withoutName(l map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range l {
		if k != "alertname" {
			out[k] = v
		}
	}
	return out
}

// Names lists the alert names of the groups, sorted.
func Names(groups []Group) []string {
	var out []string
	for _, g := range groups {
		for _, a := range g.Alerts {
			out = append(out, a.Name)
		}
	}
	sort.Strings(out)
	return out
}
