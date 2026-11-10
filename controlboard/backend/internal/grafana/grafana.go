// Package grafana builds links into the lab's Grafana (dashboards, Explore for traces and logs).
// Datasource and dashboard UIDs are fixed by docs/contracts/platform.md.
package grafana

import (
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
)

// Datasource UIDs and types (platform.md).
const (
	TracesUID  = "traces"
	TracesType = "jaeger"
	LogsUID    = "vlogs"
	LogsType   = "victoriametrics-logs-datasource"
)

// Links builds URLs relative to Grafana's public base URL.
type Links struct {
	Base string // e.g. http://grafana.localhost:8080
}

// Dashboard returns the URL of a dashboard by UID, empty when uid is empty.
func (l Links) Dashboard(uid string) string {
	if uid == "" {
		return ""
	}
	return strings.TrimRight(l.Base, "/") + "/d/" + url.PathEscape(uid)
}

// Trace returns an Explore link that opens one trace in the traces datasource.
func (l Links) Trace(traceID string) string {
	return l.explore(TracesUID, TracesType, map[string]any{"query": traceID}, "now-1h")
}

// OrderLogs returns an Explore link with the logs that mention an order id.
func (l Links) OrderLogs(orderID string) string {
	return l.explore(LogsUID, LogsType, map[string]any{"expr": strconv.Quote(orderID)}, "now-6h")
}

func (l Links) explore(uid, typ string, query map[string]any, from string) string {
	query["refId"] = "A"
	query["datasource"] = map[string]string{"type": typ, "uid": uid}
	panes := map[string]any{"a": map[string]any{
		"datasource": uid,
		"queries":    []any{query},
		"range":      map[string]string{"from": from, "to": "now"},
	}}
	b, _ := json.Marshal(panes) // only strings and maps: cannot fail
	v := url.Values{"schemaVersion": {"1"}, "orgId": {"1"}, "panes": {string(b)}}
	return strings.TrimRight(l.Base, "/") + "/explore?" + v.Encode()
}
