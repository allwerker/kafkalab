// Package api holds the controlboard's wire types (a mirror of the TypeScript types in
// docs/contracts/controlboard-api.md, JSON camelCase) and the ports between the HTTP layer
// and the experiment engine (ports.go).
package api

import "time"

// Health of a service, broker or link.
const (
	HealthUp       = "up"
	HealthDegraded = "degraded"
	HealthDown     = "down"
	HealthUnknown  = "unknown"
)

// Values is a Record<string, number | boolean | string>.
type Values = map[string]any

// ServiceState is the desired and observed state of one commerce service.
type ServiceState struct {
	Name              string `json:"name"`
	Replicas          int    `json:"replicas"`
	ReadyReplicas     int    `json:"readyReplicas"`
	Paused            bool   `json:"paused"`
	DesiredGeneration int64  `json:"desiredGeneration"`
	AppliedGeneration int64  `json:"appliedGeneration"`
	Health            string `json:"health"`
	Knobs             Values `json:"knobs"`
}

// BrokerState is the observed state of one Kafka node.
type BrokerState struct {
	ID              int  `json:"id"`
	Up              bool `json:"up"`
	IsController    bool `json:"isController"`
	LeaderCount     int  `json:"leaderCount"`
	UnderReplicated int  `json:"underReplicated"`
}

// Kpis are the business key figures shown on the board.
type Kpis struct {
	OrdersPerSec           float64 `json:"ordersPerSec"`
	RevenuePerMin          float64 `json:"revenuePerMin"`
	PaymentSuccessRate     float64 `json:"paymentSuccessRate"`
	ReservationSuccessRate float64 `json:"reservationSuccessRate"`
	E2EP95Ms               float64 `json:"e2eP95Ms"`
	InFlight               float64 `json:"inFlight"`
	DlqPerMin              float64 `json:"dlqPerMin"`
	TotalLag               float64 `json:"totalLag"`
}

// ScenarioParam describes one parameter of a scenario.
type ScenarioParam struct {
	Key     string   `json:"key"`
	Label   string   `json:"label"`
	Type    string   `json:"type"` // number | boolean | select | service | broker
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Step    *float64 `json:"step,omitempty"`
	Unit    string   `json:"unit,omitempty"`
	Options []string `json:"options,omitempty"`
	Default any      `json:"default"`
}

// DashboardLink points to a Grafana dashboard.
type DashboardLink struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

// ScenarioDef is one entry of the scenario catalog.
type ScenarioDef struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	Description        string          `json:"description"`
	Category           string          `json:"category"` // traffic | service | kubernetes | broker | network | data
	Severity           string          `json:"severity"` // low | medium | high | critical
	Destructive        bool            `json:"destructive"`
	DefaultDurationSec int             `json:"defaultDurationSec"`
	Params             []ScenarioParam `json:"params"`
	ExpectedSymptoms   []string        `json:"expectedSymptoms"`
	Dashboards         []DashboardLink `json:"dashboards"`
}

// Experiment statuses.
const (
	StatusScheduled  = "scheduled"
	StatusRunning    = "running"
	StatusRecovering = "recovering"
	StatusCompleted  = "completed"
	StatusAborted    = "aborted"
	StatusFailed     = "failed"
)

// Observation compares what an experiment requested with what is observed.
type Observation struct {
	Label     string    `json:"label"`
	Requested string    `json:"requested"`
	Observed  string    `json:"observed"`
	Confirmed bool      `json:"confirmed"`
	At        time.Time `json:"at"`
}

// Experiment is one run of a scenario.
type Experiment struct {
	ID           string            `json:"id"`
	Scenario     string            `json:"scenario"`
	Name         string            `json:"name"`
	Category     string            `json:"category,omitempty"` // scenario category; "" during an unsolved drill
	Severity     string            `json:"severity,omitempty"` // scenario severity; "" during an unsolved drill
	Status       string            `json:"status"`
	Params       Values            `json:"params"`
	CreatedAt    time.Time         `json:"createdAt"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	EndedAt      *time.Time        `json:"endedAt,omitempty"`
	AutoRecover  bool              `json:"autoRecover"`
	Affected     []string          `json:"affected"`
	Observations []Observation     `json:"observations"`
	Report       *ExperimentReport `json:"report,omitempty"`
}

// ExperimentReport summarises an ended experiment.
type ExperimentReport struct {
	TimeToDetectSec  *float64 `json:"timeToDetectSec,omitempty"`
	TimeToRecoverSec *float64 `json:"timeToRecoverSec,omitempty"`
	PeakLag          float64  `json:"peakLag"`
	OrdersAffected   float64  `json:"ordersAffected"`
	RevenueImpactEur float64  `json:"revenueImpactEur"`
	Alerts           []string `json:"alerts"`
}

// Alert statuses.
const (
	AlertPending  = "pending"
	AlertFiring   = "firing"
	AlertResolved = "resolved"
)

// Alert is one alert from vmalert (pending) or Alertmanager (firing, resolved).
type Alert struct {
	Fingerprint  string            `json:"fingerprint"`
	Name         string            `json:"name"`
	Status       string            `json:"status"`
	Severity     string            `json:"severity"` // critical | warning | info
	Component    string            `json:"component"`
	Summary      string            `json:"summary"`
	Description  string            `json:"description"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       *time.Time        `json:"endsAt,omitempty"`
	Labels       map[string]string `json:"labels"`
	DashboardURL string            `json:"dashboardUrl,omitempty"`
	RunbookURL   string            `json:"runbookUrl,omitempty"`
	GeneratorURL string            `json:"generatorUrl,omitempty"`
}

// Timeline kinds and severities.
const (
	KindExperiment = "experiment"
	KindConfig     = "config"
	KindAlert      = "alert"
	KindRecovery   = "recovery"
	KindSimulation = "simulation"
	KindDrill      = "drill"
	KindSystem     = "system"

	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
	SeveritySuccess  = "success"
)

// TimelineEntry is one line of the board's timeline.
type TimelineEntry struct {
	ID           string    `json:"id"`
	At           time.Time `json:"at"`
	Kind         string    `json:"kind"`
	Severity     string    `json:"severity"`
	Title        string    `json:"title"`
	Detail       string    `json:"detail,omitempty"`
	ExperimentID string    `json:"experimentId,omitempty"`
}

// Link is a tool link with its health.
type Link struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	URL         string `json:"url"`
	Description string `json:"description"`
	Icon        string `json:"icon"`
	Health      string `json:"health"`
}

// State is the full board state (GET /api/state and the WebSocket "state" message).
type State struct {
	Simulation  string         `json:"simulation"` // running | stopped
	ChaosArmed  bool           `json:"chaosArmed"`
	Services    []ServiceState `json:"services"`
	Brokers     []BrokerState  `json:"brokers"`
	Kpis        Kpis           `json:"kpis"`
	Experiments []Experiment   `json:"experiments"`
	Alerts      []Alert        `json:"alerts"`
	Links       []Link         `json:"links"`
	Drill       *Drill         `json:"drill,omitempty"`
}

// TuningSetting describes one Tuning Lab client setting.
type TuningSetting struct {
	Key     string   `json:"key"`
	Label   string   `json:"label"`
	Type    string   `json:"type"` // number | select
	Min     *float64 `json:"min,omitempty"`
	Max     *float64 `json:"max,omitempty"`
	Step    *float64 `json:"step,omitempty"`
	Options []string `json:"options,omitempty"`
	Default any      `json:"default"`
	Lossy   []string `json:"lossy,omitempty"`
	Help    string   `json:"help"`
}

// TuningState is the body of GET /api/tuning.
type TuningState struct {
	Settings []TuningSetting   `json:"settings"`
	Services map[string]Values `json:"services"`
}

// DrillGuess is one guess of the player.
type DrillGuess struct {
	Scenario string    `json:"scenario"`
	Correct  bool      `json:"correct"`
	At       time.Time `json:"at"`
}

// DrillCandidate is a scenario the player can guess.
type DrillCandidate struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Drill is an Incident Drill (hidden scenario the player has to identify).
type Drill struct {
	ID         string           `json:"id"`
	Status     string           `json:"status"`     // running | solved | revealed | expired
	Difficulty string           `json:"difficulty"` // easy | medium | hard
	StartedAt  time.Time        `json:"startedAt"`
	SolvedAt   *time.Time       `json:"solvedAt,omitempty"`
	HintsUsed  int              `json:"hintsUsed"`
	Hints      []string         `json:"hints"`
	Guesses    []DrillGuess     `json:"guesses"`
	Score      *float64         `json:"score,omitempty"`
	Scenario   string           `json:"scenario,omitempty"`
	Candidates []DrillCandidate `json:"candidates"`
}

// LeaderboardEntry is one finished drill.
type LeaderboardEntry struct {
	Player      string    `json:"player"`
	Score       float64   `json:"score"`
	Scenario    string    `json:"scenario"`
	DurationSec float64   `json:"durationSec"`
	At          time.Time `json:"at"`
}

// LiveEvent kinds.
const (
	EventCreated       = "created"
	EventPaid          = "paid"
	EventPaymentFailed = "payment-failed"
	EventRefunded      = "refunded"
	EventReserved      = "reserved"
	EventRejected      = "rejected"
	EventReleased      = "released"
	EventShipped       = "shipped"
	EventCancelled     = "cancelled"
	EventNotified      = "notified"
	EventDLQ           = "dlq"
)

// LiveEvent is a sampled business event for the live view.
type LiveEvent struct {
	Topic      string    `json:"topic"`
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	OrderID    string    `json:"orderId"`
	Kind       string    `json:"kind"`
	At         time.Time `json:"at"`
	ValueCents int64     `json:"valueCents,omitempty"`
	Region     string    `json:"region,omitempty"`
	Reason     string    `json:"reason,omitempty"`
	Carrier    string    `json:"carrier,omitempty"`
}

// OrderTimeline is the body of GET /api/orders/{orderId}.
type OrderTimeline struct {
	OrderID  string      `json:"orderId"`
	Events   []LiveEvent `json:"events"`
	TraceID  string      `json:"traceId,omitempty"`
	TraceURL string      `json:"traceUrl,omitempty"`
	LogsURL  string      `json:"logsUrl,omitempty"`
}

// Error is the body of every 4xx/5xx response.
type Error struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// StartRequest is the body of POST /api/experiments.
type StartRequest struct {
	Scenario      string `json:"scenario"`
	Params        Values `json:"params"`
	DurationSec   int    `json:"durationSec"`
	StartDelaySec int    `json:"startDelaySec,omitempty"`
	AutoRecover   bool   `json:"autoRecover"`
	Confirm       string `json:"confirm,omitempty"`
}

// WebSocket message types.
const (
	MsgHello      = "hello"
	MsgState      = "state"
	MsgTick       = "tick"
	MsgEvents     = "events"
	MsgAlert      = "alert"
	MsgExperiment = "experiment"
	MsgTimeline   = "timeline"
	MsgDrill      = "drill"
	MsgPong       = "pong"
)

// Message is the envelope of every WebSocket message from the server.
type Message struct {
	Type string    `json:"type"`
	Ts   time.Time `json:"ts"`
	Data any       `json:"data,omitempty"`
}

// Hello is the data of the "hello" message.
type Hello struct {
	Version    int       `json:"version"`
	ServerTime time.Time `json:"serverTime"`
}

// Tick is the data of the "tick" message. Lag[group][topic][p] is the lag of partition p.
type Tick struct {
	Kpis       Kpis                          `json:"kpis"`
	Services   []ServiceState                `json:"services"`
	Brokers    []BrokerState                 `json:"brokers"`
	Rates      map[string]float64            `json:"rates"`
	Lag        map[string]map[string][]int64 `json:"lag"`
	Partitions map[string]int                `json:"partitions"`
}

// Events is the data of the "events" message: each item stands for Scale real events.
type Events struct {
	Scale float64     `json:"scale"`
	Items []LiveEvent `json:"items"`
}

// ArmRequest is the body of POST /api/chaos/arm and its response.
type ArmRequest struct {
	Armed bool `json:"armed"`
}

// PanicResponse is the body returned by POST /api/panic.
type PanicResponse struct {
	Reverted int `json:"reverted"`
}

// SimulationRequest is the body of POST /api/simulation.
type SimulationRequest struct {
	Action string `json:"action"` // start | stop | reset
}

// SimulationResponse is returned by POST /api/simulation.
type SimulationResponse struct {
	Simulation string `json:"simulation"`
}

// ScaleRequest is the body of POST /api/services/{name}/scale.
type ScaleRequest struct {
	Replicas int `json:"replicas"`
}

// DrillStartRequest is the body of POST /api/drills.
type DrillStartRequest struct {
	Difficulty string `json:"difficulty"`
	Player     string `json:"player"`
}

// GuessRequest is the body of POST /api/drills/{id}/guess.
type GuessRequest struct {
	Scenario string `json:"scenario"`
}
