// Package lint validates generated dashboards and rules: every query parses (strict PromQL for
// the bundle, MetricsQL for the demo, a structural check for LogsQL), every metric is known,
// every panel is documented and has a unit and thresholds where they carry meaning.
package lint

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/metricsql"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// Dialect is the query language of a target.
type Dialect int

// Supported query languages.
const (
	PromQL Dialect = iota
	MetricsQL
	LogsQL
)

var grafanaVar = regexp.MustCompile(`\$\{?(\w+)(?::\w+)?\}?`)

// Expand replaces Grafana template variables with parseable placeholders: interval-like built-ins
// become durations, everything else the label value "x".
func Expand(expr string) string {
	return grafanaVar.ReplaceAllStringFunc(expr, func(m string) string {
		name := grafanaVar.FindStringSubmatch(m)[1]
		switch name {
		case "__rate_interval", "__interval", "__range", "__auto":
			return "5m"
		case "__range_s", "__interval_ms", "__range_ms":
			return "300"
		}
		return "x"
	})
}

// Metrics parses a query and returns the metric names it selects, sorted and unique.
func Metrics(expr string, d Dialect) ([]string, error) {
	expanded := Expand(expr)
	set := map[string]bool{}
	switch d {
	case PromQL:
		e, err := parser.NewParser(parser.Options{}).ParseExpr(expanded)
		if err != nil {
			return nil, fmt.Errorf("PromQL: %w", err)
		}
		parser.Inspect(e, func(n parser.Node, _ []parser.Node) error {
			if vs, ok := n.(*parser.VectorSelector); ok {
				set[selectorName(vs)] = true
			}
			return nil
		})
	case MetricsQL:
		e, err := metricsql.Parse(expanded)
		if err != nil {
			return nil, fmt.Errorf("MetricsQL: %w", err)
		}
		metricsql.VisitAll(e, func(n metricsql.Expr) {
			if me, ok := n.(*metricsql.MetricExpr); ok {
				for _, lfs := range me.LabelFilterss {
					for _, lf := range lfs {
						if lf.Label == "__name__" && !lf.IsRegexp && !lf.IsNegative {
							set[lf.Value] = true
						}
					}
				}
			}
		})
	default:
		return nil, CheckLogsQL(expanded)
	}
	delete(set, "")
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

func selectorName(vs *parser.VectorSelector) string {
	if vs.Name != "" {
		return vs.Name
	}
	for _, m := range vs.LabelMatchers {
		if m.Name == "__name__" && m.Type == labels.MatchEqual {
			return m.Value
		}
	}
	return ""
}

// CheckLogsQL performs a structural check of a LogsQL query: non-empty, balanced quotes, brackets.
func CheckLogsQL(q string) error {
	if strings.TrimSpace(q) == "" {
		return fmt.Errorf("LogsQL: empty query")
	}
	var stack []rune
	pairs := map[rune]rune{')': '(', ']': '[', '}': '{'}
	inQuote := false
	for _, r := range q {
		switch {
		case r == '"':
			inQuote = !inQuote
		case inQuote:
		case r == '(' || r == '[' || r == '{':
			stack = append(stack, r)
		case r == ')' || r == ']' || r == '}':
			if len(stack) == 0 || stack[len(stack)-1] != pairs[r] {
				return fmt.Errorf("LogsQL: unbalanced %q", r)
			}
			stack = stack[:len(stack)-1]
		}
	}
	if inQuote || len(stack) > 0 {
		return fmt.Errorf("LogsQL: unbalanced quotes or brackets")
	}
	return nil
}
