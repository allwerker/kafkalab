// Package catalog loads the bundle metric catalog (monitoring/bundle/catalog.yaml), validates it
// against recorded scrape fixtures and reads the application metric contract (metrics.md).
package catalog

import (
	"bufio"
	"bytes"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Sources a metric can come from. "rules" are recording rules and ALERTS (no fixture).
var Sources = map[string]bool{
	"jmx": true, "kminion": true, "node": true, "cadvisor": true, "ksm": true, "kubelet": true,
	"vmagent": true, "rules": true, "victoriametrics": true, "postgres": true, "go": true,
}

// Metric is one catalog entry.
type Metric struct {
	Source  string   `yaml:"source"`
	Type    string   `yaml:"type"`
	Labels  []string `yaml:"labels,omitempty"`
	MBean   string   `yaml:"mbean,omitempty"`
	Meaning string   `yaml:"meaning"`
}

// Catalog maps metric names to their documentation.
type Catalog struct {
	// Fixtures maps a source to glob patterns of recorded scrapes (relative to the catalog file).
	Fixtures map[string][]string `yaml:"fixtures"`
	Metrics  map[string]Metric   `yaml:"metrics"`
}

// Parse reads a catalog document.
func Parse(data []byte) (*Catalog, error) {
	var c Catalog
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	return &c, nil
}

// Has reports whether the metric is in the catalog.
func (c *Catalog) Has(name string) bool {
	_, ok := c.Metrics[name]
	return ok
}

var types = map[string]bool{"counter": true, "gauge": true, "histogram": true, "summary": true, "info": true, "untyped": true}

// Validate checks every entry (source, type, meaning) and that metrics of a source with fixtures
// appear in at least one fixture file. fsys is rooted at the catalog's directory.
func (c *Catalog) Validate(fsys fs.FS) []string {
	var problems []string
	seen := map[string]map[string]bool{}
	for src, globs := range c.Fixtures {
		seen[src] = map[string]bool{}
		for _, g := range globs {
			files, _ := fs.Glob(fsys, g) // pattern syntax is checked by the empty result below
			if len(files) == 0 {
				problems = append(problems, fmt.Sprintf("fixtures: %s pattern %q matches no file", src, g))
			}
			for _, f := range files {
				data, err := fs.ReadFile(fsys, f)
				if err != nil {
					problems = append(problems, fmt.Sprintf("fixtures: %v", err))
					continue
				}
				for n := range Names(data) {
					seen[src][n] = true
				}
			}
		}
	}
	for _, name := range c.sortedNames() {
		m := c.Metrics[name]
		switch {
		case !Sources[m.Source]:
			problems = append(problems, fmt.Sprintf("catalog %s: unknown source %q", name, m.Source))
		case !types[m.Type]:
			problems = append(problems, fmt.Sprintf("catalog %s: unknown type %q", name, m.Type))
		case len(strings.TrimSpace(m.Meaning)) < 15:
			problems = append(problems, fmt.Sprintf("catalog %s: meaning missing or too short", name))
		case seen[m.Source] != nil && !seen[m.Source][name]:
			problems = append(problems, fmt.Sprintf("catalog %s: not found in any %s fixture (%s)", name, m.Source, strings.Join(c.Fixtures[m.Source], ", ")))
		}
	}
	return problems
}

// Unused returns catalog entries not contained in used (the catalog documents exactly what the
// bundle uses).
func (c *Catalog) Unused(used map[string]bool) []string {
	var out []string
	for _, n := range c.sortedNames() {
		if !used[n] {
			out = append(out, n)
		}
	}
	return out
}

func (c *Catalog) sortedNames() []string {
	names := make([]string, 0, len(c.Metrics))
	for n := range c.Metrics {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Names returns the metric names of a Prometheus text exposition.
func Names(data []byte) map[string]bool {
	out := map[string]bool{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.IndexAny(line, "{ "); i > 0 {
			out[line[:i]] = true
		}
	}
	return out
}

var (
	mdRow    = regexp.MustCompile("^\\|\\s*(`[^|]*)\\|")
	mdName   = regexp.MustCompile("`([a-z_][a-z0-9_]*)`")
	histType = regexp.MustCompile(`\|\s*histogram\s*\|`)
)

// ContractNames extracts the metric names of docs/contracts/metrics.md. Abbreviations like
// "`kafka_client_connects_total` / `_connect_errors_total`" expand with the first name's
// two-segment prefix; histograms add _bucket, _sum and _count.
func ContractNames(md []byte) map[string]bool {
	out := map[string]bool{}
	for _, line := range strings.Split(string(md), "\n") {
		m := mdRow.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		var names []string
		prefix := ""
		for _, n := range mdName.FindAllStringSubmatch(m[1], -1) {
			name := n[1]
			if strings.HasPrefix(name, "_") {
				name = prefix + name
			} else if parts := strings.SplitN(name, "_", 3); len(parts) == 3 {
				prefix = parts[0] + "_" + parts[1]
			}
			names = append(names, name)
		}
		hist := histType.MatchString(line)
		for _, n := range names {
			out[n] = true
			if hist {
				for _, s := range []string{"_bucket", "_sum", "_count"} {
					out[n+s] = true
				}
			}
		}
	}
	return out
}
