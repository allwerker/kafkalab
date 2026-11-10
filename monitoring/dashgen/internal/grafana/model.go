// Package grafana is a small, opinionated builder for classic Grafana dashboard JSON
// (schemaVersion 39: Grafana 11+, migrated transparently by 12/13).
//
// It only covers what the Kafka Lab dashboards need. Panels are laid out automatically:
// rows flow left to right and wrap at 24 grid columns. See docs/adr/0004-dashboards-as-code.md.
package grafana

// SchemaVersion is the classic dashboard schema version written into every dashboard.
const SchemaVersion = 39

// Datasource references a Grafana datasource by type and UID (the UID may be a variable).
type Datasource struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

// Well-known datasources of the lab and the portable bundle.
var (
	// PromVar is the `${datasource}` variable of type prometheus used by every bundle panel.
	PromVar = Datasource{Type: "prometheus", UID: "${datasource}"}
	// VM is the lab's default Prometheus-compatible datasource.
	VM = Datasource{Type: "prometheus", UID: "vm"}
	// VMNative is the VictoriaMetrics plugin datasource (MetricsQL).
	VMNative = Datasource{Type: "victoriametrics-metrics-datasource", UID: "vm-native"}
	// VLogs is the VictoriaLogs datasource.
	VLogs = Datasource{Type: "victoriametrics-logs-datasource", UID: "vlogs"}
	// GrafanaDS is Grafana's built-in datasource (annotations by tag).
	GrafanaDS = Datasource{Type: "grafana", UID: "-- Grafana --"}
)

// Target is one query of a panel.
type Target struct {
	Expr    string
	Legend  string
	Instant bool
	Format  string // "" (time_series), "table", "heatmap"
	// QueryType is used by the VictoriaLogs datasource ("instant", "stats", "statsRange").
	QueryType string
}

// Q creates a range query with a legend.
func Q(expr, legend string) Target { return Target{Expr: expr, Legend: legend} }

// I creates an instant query in table format (for tables, bar gauges and pie charts).
func I(expr, legend string) Target {
	return Target{Expr: expr, Legend: legend, Instant: true, Format: "table"}
}

// Step is one threshold step; the first step's value is ignored (base).
type Step struct {
	Color string
	Value float64
}

// Link is a data link or a dashboard link.
type Link struct {
	Title string
	URL   string
	// Dashboard-link only: link all dashboards carrying these tags as a dropdown.
	Tags []string
	// NewTab opens the link in a new browser tab.
	NewTab bool
}

// Mapping maps an exact value to display text and colour.
type Mapping struct {
	Value string
	Text  string
	Color string
}

// Override is a field override (matcher by name or regexp) with raw properties.
type Override struct {
	Matcher    string // "byName", "byRegexp", "byFrameRefID"
	Option     string
	Properties map[string]any
}

// Panel is one visualisation. Use the constructors (TimeSeries, Stat, ...) and the With* helpers.
type Panel struct {
	Kind        string
	Title       string
	Description string
	W, H        int
	Targets     []Target
	Unit        string
	Decimals    *int
	Min, Max    *float64
	// Steps are the thresholds (nil = none). A single neutral step is a deliberate decision:
	// "no good/bad value for this metric".
	Steps          []Step
	ThresholdStyle string // timeseries: "line", "area", "dashed", "line+area"
	ColorMode      string // "shades", "thresholds"
	ColorFixed     string
	Custom         map[string]any
	Options        map[string]any
	Overrides      []Override
	Links          []Link
	Mappings       []Mapping
	Transforms     []map[string]any
	DS             *Datasource
	// NoValue is shown instead of "No data" (e.g. "no CPU limit").
	NoValue string
	// Repeat repeats the panel horizontally for every selected value of this variable.
	Repeat string
}

// Row groups panels. A row with an empty title is laid out without a row header
// (used for the always-visible tiles at the top of a dashboard).
type Row struct {
	Title     string
	Collapsed bool
	Panels    []*Panel
}

// Variable is a template variable.
type Variable struct {
	Name     string
	Label    string
	Kind     string // "datasource", "query", "custom", "interval", "textbox", "constant"
	Query    string
	Multi    bool
	All      bool
	AllValue string
	Default  string
	Hide     bool
	Regex    string
	Sort     int
}

// Annotation is a dashboard annotation source.
type Annotation struct {
	Name   string
	DS     Datasource
	Color  string
	Expr   string   // prometheus query annotations
	Tags   []string // grafana tag annotations
	Title  string
	Text   string
	TagKey string
	Hide   bool
}

// Dashboard is a whole dashboard.
type Dashboard struct {
	UID         string
	Title       string
	Description string
	Tags        []string
	From        string
	Refresh     string
	Variables   []Variable
	Annotations []Annotation
	Links       []Link
	Rows        []*Row
}
