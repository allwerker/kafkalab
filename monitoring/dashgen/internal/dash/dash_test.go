package dash

import (
	"encoding/json"
	"strings"
	"testing"
)

func Test_shouldProvideEveryContractDashboardWhenListingAll(t *testing.T) {
	// GIVEN: the dashboard UIDs of the platform contract
	want := "kafka-overview kafka-broker kafka-kraft kafka-topics kafka-lag kafka-e2e kafka-jvm kafka-pods " +
		"lab-home lab-business lab-services lab-chaos lab-footprint lab-monitoring lab-postgres lab-logs"

	// WHEN: all dashboards are built
	var got []string
	for _, b := range All() {
		got = append(got, b.Dash.UID)
		if b.File() != b.Dash.UID+".json" || !json.Valid(b.JSON()) {
			t.Errorf("%s: bad file name or JSON", b.Dash.UID)
		}
	}

	// THEN: exactly the contract dashboards exist, bundle first
	if strings.Join(got, " ") != want {
		t.Errorf("got %s", strings.Join(got, " "))
	}
}

func Test_shouldUseOnlyTheDatasourceVariableWhenTheDashboardIsPartOfTheBundle(t *testing.T) {
	// GIVEN: the bundle dashboards
	for _, b := range All() {
		if b.Set != Bundle {
			continue
		}

		// WHEN: a bundle dashboard is rendered
		js := string(b.JSON())

		// THEN: it references no lab datasource UID and no lab-only metric or label value
		for _, forbidden := range []string{`"uid": "vm"`, `"uid": "vm-native"`, "lab_", `kafka-lab"`} {
			if strings.Contains(js, forbidden) {
				t.Errorf("%s contains %s", b.Dash.UID, forbidden)
			}
		}
	}
}

func Test_shouldKeepTimeRangeDatasourceAndClusterWhenBuildingDrillDownLinks(t *testing.T) {
	// GIVEN: a link target and an extra variable, and a lab link into the bundle
	extra := "var-topic=${__field.labels.topic}"

	// WHEN: the URLs are built
	bundleURL, labURL, plainLab := drill("kafka-topics", extra), lab("kafka-lag", ""), lab("lab-chaos", "x=1")

	// THEN: bundle links carry time range, datasource and cluster; lab links into the bundle select the lab cluster
	if bundleURL != "/d/kafka-topics/?${__url_time_range}&${datasource:queryparam}&${kafka_cluster:queryparam}&"+extra {
		t.Errorf("bundle link: %s", bundleURL)
	}
	if !strings.Contains(labURL, "var-kafka_cluster=kafka-lab") || strings.Contains(plainLab, "var-datasource") || !strings.HasSuffix(plainLab, "&x=1") {
		t.Errorf("lab links: %s %s", labURL, plainLab)
	}
	if drill("kafka-e2e", "") != "/d/kafka-e2e/?${__url_time_range}&${datasource:queryparam}&${kafka_cluster:queryparam}" {
		t.Error("drill without extra parameters must not end with &")
	}
}
