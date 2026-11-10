package experiments

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/k8s"
)

// A scenario is planned once per experiment: Plan turns validated parameters into knob
// overlays (published on platform.control) and Kubernetes actions, plus the checks that
// compare what was requested with what is observed. The engine applies the plan, observes
// it, and reverts it by recomputing every affected service's snapshot without the experiment
// and undoing the actions from their recorded revert data.

// Scenario is one catalogue entry.
type Scenario struct {
	Def api.ScenarioDef
	// Drill is the Incident Drill difficulty pool ("easy", "medium", "hard"), "" = never drilled.
	Drill string
	// Kube marks scenarios that need the Kubernetes API.
	Kube bool
	Plan func(p PlanInput) (Plan, error)
}

// PlanInput is what a plan is computed from.
type PlanInput struct {
	Params   api.Values
	Duration time.Duration
	Cfg      *Config
}

// Plan is what one experiment does.
type Plan struct {
	// Affected services and brokers ("broker-1") for the Experiment.affected field.
	Affected []string
	// Overlay returns the knob overlay per service at a moment of the run. nil = no knobs.
	Overlay func(elapsed time.Duration) map[string]api.Values
	// StepEvery > 0 re-evaluates Overlay and republishes at this interval (Traffic Tsunami).
	StepEvery time.Duration
	// Actions are applied in order and reverted in reverse order.
	Actions []Action
	// Checks are evaluated while the experiment runs; RecoveryChecks after the revert (all must
	// pass for the experiment to count as recovered).
	Checks         []Check
	RecoveryChecks []Check
	// Broker experiments take the global broker-fault lock.
	Broker bool
	// Intensity (0..1) for lab_experiment_intensity at a moment of the run.
	Intensity func(elapsed time.Duration) float64
}

// Action is a Kubernetes side effect of an experiment.
type Action interface {
	// Apply performs the action and returns the data needed to undo it later, even after a
	// controlboard restart (persisted in PostgreSQL).
	Apply(ctx context.Context, env *Env) (json.RawMessage, error)
	// Tick is called every engine tick while the experiment runs.
	Tick(ctx context.Context, env *Env, elapsed time.Duration) error
	// Revert undoes Apply from its revert data.
	Revert(ctx context.Context, env *Env, data json.RawMessage) error
}

// Env is what actions and checks work with.
type Env struct {
	Kube         *k8s.Client
	Cfg          *Config
	ExperimentID string
	Now          func() time.Time
}

// Check compares one requested effect with what is observed.
type Check struct {
	Label     string
	Requested string
	Eval      func(c *CheckContext) (observed string, ok bool)
}

// CheckContext is the observed world at evaluation time.
type CheckContext struct {
	Ctx      context.Context
	Env      *Env
	Obs      api.Observer
	Start    api.Kpis // KPIs when the experiment started
	StartLag int64
	Now      time.Time
}

// Param constructors keep the catalogue readable.

func f64(v float64) *float64 { return &v }

func numParam(key, label string, min, max, step float64, unit string, def float64) api.ScenarioParam {
	return api.ScenarioParam{Key: key, Label: label, Type: "number", Min: f64(min), Max: f64(max), Step: f64(step), Unit: unit, Default: def}
}

func boolParam(key, label string, def bool) api.ScenarioParam {
	return api.ScenarioParam{Key: key, Label: label, Type: "boolean", Default: def}
}

func selectParam(key, label string, options []string, def string) api.ScenarioParam {
	return api.ScenarioParam{Key: key, Label: label, Type: "select", Options: options, Default: def}
}

func serviceParam(key, label string, options []string, def string) api.ScenarioParam {
	return api.ScenarioParam{Key: key, Label: label, Type: "service", Options: options, Default: def}
}

func brokerParam(def int) api.ScenarioParam {
	return api.ScenarioParam{Key: "broker", Label: "Broker", Type: "broker", Min: f64(0), Max: f64(2), Step: f64(1), Default: float64(def)}
}

// randomizeKey is a common boolean parameter: numeric parameters are varied by ±25 %.
const randomizeKey = "randomize"

func randomizeParam() api.ScenarioParam {
	return boolParam(randomizeKey, "Randomise intensity (±25 %)", false)
}

// resolveParams fills defaults, validates types and ranges and applies the optional
// randomisation (rnd returns values in [0,1)).
func resolveParams(def api.ScenarioDef, in api.Values, rnd func() float64) (api.Values, error) {
	out := api.Values{}
	known := map[string]bool{}
	for _, p := range def.Params {
		known[p.Key] = true
		v, ok := in[p.Key]
		if !ok || v == nil {
			v = p.Default
		}
		nv, err := checkParam(p, v)
		if err != nil {
			return nil, fmt.Errorf("%w: parameter %s: %v", api.ErrInvalid, p.Key, err)
		}
		out[p.Key] = nv
	}
	for k := range in {
		if !known[k] {
			return nil, fmt.Errorf("%w: unknown parameter %q for %s", api.ErrInvalid, k, def.ID)
		}
	}
	if r, _ := out[randomizeKey].(bool); r {
		for _, p := range def.Params {
			if p.Type != "number" {
				continue
			}
			n := out[p.Key].(float64) * (0.75 + 0.5*rnd())
			n = math.Min(math.Max(n, *p.Min), *p.Max)
			if *p.Step >= 1 {
				n = math.Round(n / *p.Step) * *p.Step
			}
			out[p.Key] = n
		}
	}
	return out, nil
}

func checkParam(p api.ScenarioParam, v any) (any, error) {
	switch p.Type {
	case "boolean":
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("want true or false, got %v", v)
		}
		return b, nil
	case "select", "service":
		s, ok := v.(string)
		if !ok || !slices.Contains(p.Options, s) {
			return nil, fmt.Errorf("want one of %v, got %v", p.Options, v)
		}
		return s, nil
	}
	// number, broker
	n, ok := toFloat(v)
	if !ok || math.IsNaN(n) {
		return nil, fmt.Errorf("want a number, got %v", v)
	}
	if n < *p.Min || n > *p.Max {
		return nil, fmt.Errorf("%v is outside %v..%v", n, *p.Min, *p.Max)
	}
	if p.Type == "broker" && n != math.Trunc(n) {
		return nil, fmt.Errorf("broker id must be a whole number, got %v", n)
	}
	return n, nil
}

// Typed accessors for resolved params.
func pNum(p api.Values, key string) float64 { return p[key].(float64) }
func pInt(p api.Values, key string) int     { return int(math.Round(p[key].(float64))) }
func pStr(p api.Values, key string) string  { return p[key].(string) }

// static wraps a fixed overlay.
func static(o map[string]api.Values) func(time.Duration) map[string]api.Values {
	return func(time.Duration) map[string]api.Values { return o }
}

func constant(x float64) func(time.Duration) float64 {
	return func(time.Duration) float64 { return x }
}
