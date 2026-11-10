package grafana

// Palette: every panel picks one hue family and its series are shades of it. Red is reserved for
// "bad" (thresholds, errors); it is never used as a plain series colour.
const (
	Blue   = "blue"
	Purple = "purple"
	Green  = "green"
	Yellow = "yellow"
	Orange = "orange"
	Red    = "red"
	Text   = "text"
)

func newPanel(kind, title, desc, unit string, w, h int, targets []Target) *Panel {
	return &Panel{
		Kind: kind, Title: title, Description: desc, Unit: unit, W: w, H: h, Targets: targets,
		Custom: map[string]any{}, Options: map[string]any{},
	}
}

// TimeSeries is a line chart; series are shades of one hue (blue by default).
func TimeSeries(title, desc, unit string, targets ...Target) *Panel {
	p := newPanel("timeseries", title, desc, unit, 12, 8, targets)
	p.ColorMode, p.ColorFixed = "shades", Blue
	return p
}

// Stat is a single-value tile with a sparkline, coloured by thresholds.
func Stat(title, desc, unit string, targets ...Target) *Panel {
	p := newPanel("stat", title, desc, unit, 4, 4, targets)
	p.ColorMode = "thresholds"
	return p
}

// Gauge is a radial gauge coloured by thresholds.
func Gauge(title, desc, unit string, targets ...Target) *Panel {
	p := newPanel("gauge", title, desc, unit, 4, 5, targets)
	p.ColorMode = "thresholds"
	return p
}

// BarGauge is a horizontal gradient bar per series (top-N lists).
func BarGauge(title, desc, unit string, targets ...Target) *Panel {
	p := newPanel("bargauge", title, desc, unit, 8, 8, targets)
	// one bar per series: instant queries must return series, not a table frame
	for i := range p.Targets {
		p.Targets[i].Format = ""
	}
	p.ColorMode = "thresholds"
	return p
}

// Table renders instant queries as a table.
func Table(title, desc, unit string, targets ...Target) *Panel {
	p := newPanel("table", title, desc, unit, 12, 8, targets)
	p.ColorMode = "thresholds"
	return p
}

// Heatmap renders pre-bucketed data (Prometheus histograms or one series per row).
func Heatmap(title, desc, unit string, targets ...Target) *Panel {
	return newPanel("heatmap", title, desc, unit, 12, 8, targets)
}

// StateTimeline shows discrete states over time (one lane per series).
func StateTimeline(title, desc, unit string, targets ...Target) *Panel {
	p := newPanel("state-timeline", title, desc, unit, 12, 6, targets)
	p.ColorMode = "thresholds"
	return p
}

// TextPanel renders markdown.
func TextPanel(title, desc, markdown string) *Panel {
	p := newPanel("text", title, desc, "", 24, 4, nil)
	p.Options["mode"] = "markdown"
	p.Options["content"] = markdown
	return p
}

// Logs renders log lines of a logs datasource.
func Logs(title, desc string, targets ...Target) *Panel {
	return newPanel("logs", title, desc, "", 24, 12, targets)
}

// DashList lists dashboards carrying one of the tags, keeping time range and variables.
func DashList(title, desc string, tags ...string) *Panel {
	p := newPanel("dashlist", title, desc, "", 8, 10, nil)
	p.Options["tags"] = tags
	return p
}

// Size sets the grid width (1..24) and height.
func (p *Panel) Size(w, h int) *Panel { p.W, p.H = w, h; return p }

// Hue colours the series as shades of one palette colour.
func (p *Panel) Hue(color string) *Panel {
	p.ColorMode, p.ColorFixed = "shades", color
	return p
}

// Thresholds sets the threshold steps: base colour followed by (value, colour) steps.
func (p *Panel) Thresholds(base string, steps ...Step) *Panel {
	p.Steps = append([]Step{{Color: base}}, steps...)
	return p
}

// Neutral declares that the value has no good/bad range: a single neutral threshold.
func (p *Panel) Neutral() *Panel { return p.Thresholds(Blue) }

// Lines draws the thresholds into a timeseries ("line", "dashed", "area", "line+area").
func (p *Panel) Lines(style string) *Panel { p.ThresholdStyle = style; return p }

// Stack stacks series (normal) and fills them.
func (p *Panel) Stack() *Panel {
	p.Custom["stacking"] = map[string]any{"mode": "normal", "group": "A"}
	p.Custom["fillOpacity"] = 35
	return p
}

// Bars draws bars instead of lines.
func (p *Panel) Bars() *Panel {
	p.Custom["drawStyle"] = "bars"
	p.Custom["fillOpacity"] = 70
	p.Custom["lineWidth"] = 1
	return p
}

// SetMin fixes the axis / value minimum.
func (p *Panel) SetMin(v float64) *Panel { p.Min = &v; return p }

// SetMax fixes the axis / value maximum.
func (p *Panel) SetMax(v float64) *Panel { p.Max = &v; return p }

// Dec sets the number of decimals.
func (p *Panel) Dec(n int) *Panel { p.Decimals = &n; return p }

// Link adds a data link (on values, table cells, series).
func (p *Panel) Link(title, url string) *Panel {
	p.Links = append(p.Links, Link{Title: title, URL: url})
	return p
}

// Map adds a value mapping (exact value -> text/colour).
func (p *Panel) Map(value, text, color string) *Panel {
	p.Mappings = append(p.Mappings, Mapping{Value: value, Text: text, Color: color})
	return p
}

// Over adds a field override.
func (p *Panel) Over(matcher, option string, props map[string]any) *Panel {
	p.Overrides = append(p.Overrides, Override{Matcher: matcher, Option: option, Properties: props})
	return p
}

// SeriesColor pins the colour of the series whose display name matches the regexp.
func (p *Panel) SeriesColor(regex, color string) *Panel {
	return p.Over("byRegexp", regex, map[string]any{
		"color": map[string]any{"mode": "fixed", "fixedColor": color},
	})
}

// Opt sets a raw panel option.
func (p *Panel) Opt(key string, v any) *Panel { p.Options[key] = v; return p }

// Transform appends a Grafana transformation.
func (p *Panel) Transform(id string, options map[string]any) *Panel {
	p.Transforms = append(p.Transforms, map[string]any{"id": id, "options": options})
	return p
}

// Organize hides columns and renames others in table panels (Time is always hidden).
func (p *Panel) Organize(rename map[string]string, hide ...string) *Panel {
	excl := map[string]any{"Time": true}
	for _, h := range hide {
		excl[h] = true
	}
	return p.Transform("organize", map[string]any{"excludeByName": excl, "renameByName": rename})
}

// Using sets a panel datasource (default: the dashboard default passed to Render).
func (p *Panel) Using(ds Datasource) *Panel { p.DS = &ds; return p }

// LegendTable shows the legend as a table with the given calculations (e.g. "mean", "max", "lastNotNull").
func (p *Panel) LegendTable(placement string, calcs ...string) *Panel {
	p.Options["legend"] = map[string]any{
		"displayMode": "table", "placement": placement, "showLegend": true, "calcs": calcs,
		"sortBy": sortName(calcs), "sortDesc": true,
	}
	return p
}

func sortName(calcs []string) string {
	names := map[string]string{"lastNotNull": "Last *", "mean": "Mean", "max": "Max"}
	return names[calcs[0]]
}

// RepeatBy repeats the panel for every selected value of the variable.
func (p *Panel) RepeatBy(variable string) *Panel { p.Repeat = variable; return p }
