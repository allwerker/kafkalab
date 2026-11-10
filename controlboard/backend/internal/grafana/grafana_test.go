package grafana

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"
)

var links = Links{Base: "http://grafana.localhost:8080/"}

func panes(t *testing.T, link string) map[string]any {
	t.Helper()
	u, err := url.Parse(link)
	if err != nil {
		t.Fatal(err)
	}
	var p map[string]map[string]any
	if err := json.Unmarshal([]byte(u.Query().Get("panes")), &p); err != nil {
		t.Fatal(err)
	}
	return p["a"]
}

func Test_shouldLinkTheDashboardByUIDWhenAnAlertNamesOne(t *testing.T) {
	// GIVEN: an alert labelled with the consumer lag dashboard, and one without a dashboard
	// WHEN: the dashboard links are built
	withUID := links.Dashboard("kafka-lag")
	without := links.Dashboard("")

	// THEN: the first opens /d/kafka-lag on the public Grafana, the second has no link
	if withUID != "http://grafana.localhost:8080/d/kafka-lag" || without != "" {
		t.Fatalf("got %q and %q", withUID, without)
	}
}

func Test_shouldOpenTheTraceInTheTracesDatasourceWhenAnOrderHasATraceID(t *testing.T) {
	// GIVEN: an order whose creation carried trace id 4bf92f3577b34da6a3ce929d0e0e4736
	// WHEN: the trace link is built
	link := links.Trace("4bf92f3577b34da6a3ce929d0e0e4736")

	// THEN: Grafana Explore opens the traces datasource with that trace id as query
	a := panes(t, link)
	q := a["queries"].([]any)[0].(map[string]any)
	if !strings.HasPrefix(link, "http://grafana.localhost:8080/explore?") || a["datasource"] != "traces" || q["query"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("got %s", link)
	}
}

func Test_shouldSearchTheLogsForTheOrderIDWhenTheLogsLinkIsBuilt(t *testing.T) {
	// GIVEN: order 7f0c3c1e
	// WHEN: the logs link is built
	a := panes(t, links.OrderLogs("7f0c3c1e"))

	// THEN: Explore searches VictoriaLogs for the quoted order id over the last 6 hours
	q := a["queries"].([]any)[0].(map[string]any)
	if a["datasource"] != "vlogs" || q["expr"] != `"7f0c3c1e"` || a["range"].(map[string]any)["from"] != "now-6h" {
		t.Fatalf("got %+v", a)
	}
}
