package api

import (
	"context"
	"errors"
)

// This file is the seam between the HTTP/WebSocket layer (internal/board, owner: T207/T210)
// and the experiment engine (internal/experiments, owner: the engine ticket).
// See docs/contracts/controlboard-ports.md.
//
//   board ──calls──▶ Experiments (implemented by the engine; fakeexp is the in-memory stand-in)
//   engine ──calls──▶ Observer   (implemented by the board: what metrics, alerts and lag show)
//
// Rules for implementations:
//   - Every method is safe for concurrent use.
//   - Returned slices, maps and pointers are copies: the caller may keep and mutate them.
//   - Errors returned to the HTTP layer wrap one of the sentinel errors below (errors.Is);
//     anything else becomes 500 internal. The error text is shown to the user as "message".
//   - Timestamps are UTC.

// Sentinel errors and the HTTP status / error code they are mapped to.
var (
	ErrChaosDisarmed        = errors.New("chaos is disarmed")                  // 409 chaos_disarmed
	ErrBrokerFaultActive    = errors.New("another broker fault is active")     // 409 broker_fault_active
	ErrConfirmationRequired = errors.New("confirmation required")              // 400 confirmation_required
	ErrUnknownScenario      = errors.New("unknown scenario")                   // 404 unknown_scenario
	ErrNotFound             = errors.New("not found")                          // 404 not_found (experiment or drill id)
	ErrUnknownService       = errors.New("unknown service")                    // 404 unknown_service
	ErrInvalid              = errors.New("invalid request")                    // 400 invalid_request (bad params, knob, value)
	ErrConflict             = errors.New("conflicting state")                  // 409 conflict (e.g. a drill is already running)
	ErrUnavailable          = errors.New("dependency temporarily unavailable") // 503 unavailable (Kafka, Kubernetes API down)
)

// Experiments is everything the HTTP layer needs from the experiment engine.
type Experiments interface {
	// Scenarios returns the scenario catalog in display order.
	Scenarios() []ScenarioDef
	// Start schedules or starts an experiment. A destructive scenario needs Armed() and
	// req.Confirm == req.Scenario (ErrChaosDisarmed, ErrConfirmationRequired); only one broker
	// fault at a time (ErrBrokerFaultActive); unknown id → ErrUnknownScenario; bad params → ErrInvalid.
	Start(ctx context.Context, req StartRequest) (Experiment, error)
	// Stop ends an experiment early and reverts it (ErrNotFound). Stopping an ended one returns it unchanged.
	Stop(ctx context.Context, id string) (Experiment, error)
	// List returns the active experiments plus the last 20 ended ones, newest first.
	List() []Experiment
	// Arm arms or disarms chaos (destructive scenarios).
	Arm(ctx context.Context, armed bool) error
	Armed() bool
	// Panic reverts everything (experiments, manual knobs, scale changes), disarms chaos and
	// returns how many things were reverted.
	Panic(ctx context.Context) (int, error)
	// Simulation applies "start" | "stop" | "reset" to the order generator and returns the
	// resulting state "running" | "stopped" (unknown action → ErrInvalid).
	Simulation(ctx context.Context, action string) (string, error)
	SimulationState() string
	// Controls returns the desired control state of every commerce service (contracts.Services order).
	Controls() []ServiceControl
	// SetKnobs merges a manual override (no expiry) into the service's knobs: keys are
	// schemas/platform.avdl Knobs field names, a nil value removes the override for that key.
	// Unknown service → ErrUnknownService, unknown knob or wrong type → ErrInvalid.
	SetKnobs(ctx context.Context, service string, knobs Values) error
	// ResetKnobs removes every manual override of the service (back to baseline).
	ResetKnobs(ctx context.Context, service string) error
	// Scale sets the desired replicas (0..6, validated by the HTTP layer as well).
	Scale(ctx context.Context, service string, replicas int) error
	// Tuning returns the Tuning Lab catalog and the current per-service settings.
	Tuning() TuningState
	// SetTuning merges values into the service's client settings (nil value = back to default).
	SetTuning(ctx context.Context, service string, values Values) (TuningState, error)
	// Drills returns the Incident Drill API.
	Drills() DrillAPI
	// Subscribe delivers every change. The channel must be buffered; a subscriber that does
	// not keep up loses events (never blocks the engine). cancel closes the channel.
	Subscribe() (events <-chan Event, cancel func())
}

// DrillAPI is the Incident Drill part of the engine.
type DrillAPI interface {
	// Start starts a drill (difficulty easy | medium | hard, else ErrInvalid; one at a time, else ErrConflict).
	Start(ctx context.Context, difficulty, player string) (Drill, error)
	Hint(ctx context.Context, id string) (Drill, error)
	Guess(ctx context.Context, id, scenario string) (Drill, error)
	Reveal(ctx context.Context, id string) (Drill, error)
	// Leaderboard returns the best entries, best first.
	Leaderboard() []LeaderboardEntry
	// Current returns the running drill or the most recently ended one, nil if none.
	Current() *Drill
}

// ServiceControl is the desired control state of one service as published on platform.control.
type ServiceControl struct {
	Service           string
	Paused            bool
	DesiredGeneration int64
	// Knobs holds the non-default knobs only (same keys as SetKnobs).
	Knobs Values
	// Replicas is the desired replica count when the engine changed it, nil if untouched.
	Replicas *int
}

// Event is one change notification; exactly one field is set.
type Event struct {
	Experiment *Experiment
	Timeline   *TimelineEntry
	Drill      *Drill
}

// Observer is what the board observes right now; the engine uses it for observations
// ("requested vs observed") and reports (time to detect, peak lag, ...).
type Observer interface {
	Kpis() Kpis
	Brokers() []BrokerState
	Services() []ServiceState
	// Alerts returns active alerts (pending + firing) and those resolved in the last 15 min.
	Alerts() []Alert
	// TotalLag is the sum of all partition lags of the five consumer groups.
	TotalLag() int64
}
