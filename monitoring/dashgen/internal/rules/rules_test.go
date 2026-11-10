package rules

import (
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func sample() []Group {
	return []Group{{
		Name: "g", Records: []Record{{"r:x", "sum(x)"}},
		Alerts: []Alert{{
			Name: "XHigh", Expr: "r:x > 1", ForProd: 10 * time.Minute, ForDemo: 0,
			Severity: "warning", Component: "kafka", Dashboard: "kafka-overview",
			Summary: "x on {{ $labels.kafka_broker }}", Description: "x is high on {{$labels.kafka_broker}}.",
			Test: Scenario{
				Warmup:  time.Minute,
				Firing:  []Series{{`x{kafka_broker="0"}`, "5x%d"}},
				Labels:  map[string]string{"kafka_broker": "0"},
				Healthy: []Series{{`x{kafka_broker="0"}`, "0 0 0"}},
			},
		}},
	}}
}

func Test_shouldWriteCalmAndFastForDurationsWhenRenderingTheTwoProfiles(t *testing.T) {
	// GIVEN: an alert that waits 10 minutes in production and fires immediately in the demo
	groups := sample()

	// WHEN: both profiles are rendered
	prod, demo := string(Render("hdr", groups, Production)), string(Render("hdr", groups, Demo))

	// THEN: production carries the for: duration, the demo omits it; both keep labels and runbook
	if !strings.Contains(prod, "for: 10m") || strings.Contains(demo, "for:") {
		t.Errorf("for durations wrong:\n%s\n%s", prod, demo)
	}
	for _, want := range []string{"# hdr", "record: r:x", "severity: warning", "runbook_url: docs/runbooks/XHigh.md"} {
		if !strings.Contains(demo, want) {
			t.Errorf("missing %q in\n%s", want, demo)
		}
	}
}

func Test_shouldExpectExactLabelsAndExpandedAnnotationsWhenGeneratingTests(t *testing.T) {
	// GIVEN: an alert with a firing and a healthy scenario
	groups := sample()

	// WHEN: the production test file is generated
	var f testFile
	if err := yaml.Unmarshal(Tests("hdr", "rules.yaml", groups, Production), &f); err != nil {
		t.Fatal(err)
	}

	// THEN: the firing case is evaluated after warm-up + for + 1m with label templates resolved,
	// and the healthy case expects silence at the same time
	if len(f.Tests) != 2 || f.RuleFiles[0] != "rules.yaml" {
		t.Fatalf("got %+v", f)
	}
	fire := f.Tests[0]
	exp := fire.AlertTests[0].ExpAlerts[0]
	if fire.AlertTests[0].EvalTime != "12m" || exp.ExpLabels["severity"] != "warning" || exp.ExpLabels["alertname"] != "" {
		t.Errorf("firing case wrong: %+v", fire)
	}
	if exp.ExpAnnotations["summary"] != "x on 0" || exp.ExpAnnotations["description"] != "x is high on 0." {
		t.Errorf("annotations not expanded: %v", exp.ExpAnnotations)
	}
	if !strings.HasPrefix(fire.InputSeries[0].Values, "5x") || f.Tests[1].InputSeries[0].Values != "0 0 0" || len(f.Tests[1].AlertTests[0].ExpAlerts) != 0 {
		t.Errorf("series or healthy case wrong: %+v", f.Tests)
	}
}

func Test_shouldFormatDurationsLikePrometheusWhenRendering(t *testing.T) {
	// GIVEN: durations of various sizes
	cases := map[time.Duration]string{0: "0s", 15 * time.Second: "15s", time.Minute: "1m", 90 * time.Minute: "1h30m", time.Hour: "1h", 150 * time.Second: "2m30s"}

	for d, want := range cases {
		// WHEN: a duration is formatted
		got := Duration(d)

		// THEN: it reads like a Prometheus duration
		if got != want {
			t.Errorf("%v: got %s, want %s", d, got, want)
		}
	}
}

func Test_shouldDocumentEveryAlertWithARunbookWhenGeneratingRunbooks(t *testing.T) {
	// GIVEN: all bundle and business alerts
	groups := append(KafkaGroups(), BusinessGroups()...)

	// WHEN: runbooks are rendered
	files := RunbookFiles(groups)

	// THEN: every alert has guidance (impact, diagnosis, mitigation) and its expression
	for _, name := range Names(groups) {
		rb, ok := runbooks[name]
		if !ok || rb.Impact == "" || len(rb.Diagnosis) == 0 || len(rb.Mitigation) == 0 {
			t.Errorf("%s: runbook incomplete", name)
		}
		md := string(files[name+".md"])
		if !strings.Contains(md, "## Diagnosis") || !strings.Contains(md, "```promql") || strings.Contains(md, "$labels") {
			t.Errorf("%s: runbook markdown incomplete:\n%s", name, md)
		}
	}
	if len(files) != len(Names(groups)) {
		t.Errorf("want one runbook per alert, got %d for %d", len(files), len(Names(groups)))
	}
}

func Test_shouldFollowThePlatformContractWhenDefiningKafkaAndBusinessAlerts(t *testing.T) {
	// GIVEN: all alert definitions
	groups := append(KafkaGroups(), BusinessGroups()...)
	sev := map[string]bool{"critical": true, "warning": true, "info": true}

	for _, g := range groups {
		for _, a := range g.Alerts {
			// WHEN: an alert is inspected
			calm, fast := a.For(Production), a.For(Demo)

			// THEN: it has a contract severity, a test scenario and a demo profile at least as fast as production
			if !sev[a.Severity] || len(a.Test.Firing) == 0 || fast > calm || a.Summary == "" {
				t.Errorf("%s violates the contract", a.Name)
			}
		}
	}
}
