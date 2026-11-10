package catalog

import (
	"strings"
	"testing"
	"testing/fstest"
)

const scrape = `# HELP up whatever
# TYPE up gauge
kafka_server_replicamanager_leadercount 21
kafka_server_brokertopicmetrics_bytesin_total{topic="orders"} 5

`

func Test_shouldAcceptCatalogWhenEveryEntryIsDocumentedAndRecorded(t *testing.T) {
	// GIVEN: a catalog whose JMX metrics appear in a recorded broker scrape and whose rule entry has no fixture
	c, err := Parse([]byte(`
fixtures:
  jmx: [fixtures/jmx-*.prom]
metrics:
  kafka_server_replicamanager_leadercount: {source: jmx, type: gauge, meaning: "Partitions this broker leads."}
  "kafka:lag:deriv": {source: rules, type: gauge, meaning: "Recording rule: lag growth."}
`))
	fsys := fstest.MapFS{"fixtures/jmx-0.prom": {Data: []byte(scrape)}}

	// WHEN: it is validated against the fixtures
	problems := c.Validate(fsys)

	// THEN: nothing is reported and lookups work
	if err != nil || len(problems) > 0 || !c.Has("kafka:lag:deriv") || c.Has("nope") {
		t.Errorf("unexpected: %v %v", err, problems)
	}
}

func Test_shouldReportEveryDefectWhenCatalogEntriesAreWrong(t *testing.T) {
	// GIVEN: entries with an unknown source, an unknown type, no meaning, one missing from the
	// fixtures, a fixture pattern without files and a fixture path that is a directory
	c, _ := Parse([]byte(`
fixtures:
  jmx: [fixtures/jmx-*.prom, fixtures/dir]
  kminion: [fixtures/kminion.prom]
metrics:
  a: {source: snmp, type: gauge, meaning: "Something long enough."}
  b: {source: jmx, type: rate, meaning: "Something long enough."}
  c: {source: jmx, type: gauge, meaning: "short"}
  d: {source: jmx, type: gauge, meaning: "Recorded nowhere at all."}
`))
	fsys := fstest.MapFS{
		"fixtures/jmx-0.prom": {Data: []byte(scrape)},
		"fixtures/dir/x":      {Data: []byte("")},
	}

	// WHEN: it is validated
	out := strings.Join(c.Validate(fsys), "\n")

	// THEN: every defect is named
	for _, want := range []string{
		`a: unknown source "snmp"`, `b: unknown type "rate"`, "c: meaning missing", "d: not found in any jmx fixture",
		`kminion pattern "fixtures/kminion.prom" matches no file`, "fixtures/dir",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func Test_shouldFailToParseWhenTheCatalogIsNotYAML(t *testing.T) {
	// GIVEN: a catalog file with broken YAML
	data := []byte("metrics: [")

	// WHEN: it is parsed
	_, err := Parse(data)

	// THEN: the error mentions the catalog
	if err == nil || !strings.Contains(err.Error(), "catalog") {
		t.Errorf("want catalog error, got %v", err)
	}
}

func Test_shouldListUnusedEntriesWhenDashboardsNoLongerUseThem(t *testing.T) {
	// GIVEN: a catalog with two metrics of which the dashboards use one
	c, _ := Parse([]byte("metrics:\n  b: {source: jmx}\n  a: {source: jmx}\n"))

	// WHEN: unused entries are computed
	unused := c.Unused(map[string]bool{"a": true})

	// THEN: only the unused one is listed
	if strings.Join(unused, ",") != "b" {
		t.Errorf("got %v", unused)
	}
}

func Test_shouldExpandAbbreviationsAndHistogramsWhenReadingTheMetricContract(t *testing.T) {
	// GIVEN: contract rows with an abbreviated counter family, a histogram and a non-metric table
	md := []byte("| Metric | Type |\n|---|---|\n" +
		"| `kafka_client_connects_total` / `_connect_errors_total` | counter | `node` |\n" +
		"| `lab_batch_duration_seconds` | histogram | `topic` |\n" +
		"| `lab_x` / `lab_y_total` | counter |\n" +
		"| Port | Use |\n")

	// WHEN: the names are extracted
	names := ContractNames(md)

	// THEN: abbreviations get the family prefix, histograms their series suffixes, labels are ignored
	for _, n := range []string{"kafka_client_connects_total", "kafka_client_connect_errors_total", "lab_batch_duration_seconds_bucket", "lab_batch_duration_seconds_count", "lab_x", "lab_y_total"} {
		if !names[n] {
			t.Errorf("missing %s in %v", n, names)
		}
	}
	if names["node"] || names["topic"] {
		t.Error("label columns must not be read as metrics")
	}
}
