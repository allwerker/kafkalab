package experiments

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

// reportState accumulates what the ExperimentReport is computed from.
type reportState struct {
	StartKpis        api.Kpis   `json:"startKpis"`
	StartLag         int64      `json:"startLag"`
	PeakLag          int64      `json:"peakLag"`
	PeakInFlight     float64    `json:"peakInFlight"`
	DlqOrders        float64    `json:"dlqOrders"`
	RevenueDeltaEur  float64    `json:"revenueDeltaEur"`
	LastSample       time.Time  `json:"lastSample"`
	DetectedAt       *time.Time `json:"detectedAt,omitempty"`
	Alerts           []string   `json:"alerts,omitempty"`
	TimeToRecoverSec *float64   `json:"timeToRecoverSec,omitempty"`
}

func (s *reportState) start(o api.Observer, now time.Time) {
	s.StartKpis = o.Kpis()
	s.StartLag = o.TotalLag()
	s.PeakLag = s.StartLag
	s.PeakInFlight = s.StartKpis.InFlight
	s.LastSample = now
}

// sample integrates the KPIs since the last sample and records alerts that started after
// the experiment did (the first one is the time to detect).
func (s *reportState) sample(o api.Observer, started, now time.Time) {
	k := o.Kpis()
	dt := now.Sub(s.LastSample).Seconds()
	s.LastSample = now
	s.PeakLag = max(s.PeakLag, o.TotalLag())
	s.PeakInFlight = max(s.PeakInFlight, k.InFlight)
	s.DlqOrders += k.DlqPerMin * dt / 60
	s.RevenueDeltaEur += (k.RevenuePerMin - s.StartKpis.RevenuePerMin) * dt / 60
	for _, a := range o.Alerts() {
		if a.Status != api.AlertFiring || a.StartsAt.Before(started) || a.Name == "LabTestAlert" {
			continue
		}
		if s.DetectedAt == nil || a.StartsAt.Before(*s.DetectedAt) {
			at := a.StartsAt
			s.DetectedAt = &at
		}
		if !slices.Contains(s.Alerts, a.Name) {
			s.Alerts = append(s.Alerts, a.Name)
		}
	}
}

// report builds the public report: revenue impact is the shipped revenue compared with the
// rate before the start (negative = lost revenue), orders affected are the extra orders in
// flight at the peak plus the dead-lettered ones.
func (r *run) report() *api.ExperimentReport {
	if r.rec.StartedAt == nil {
		return nil
	}
	s := r.rec.Report
	rep := &api.ExperimentReport{
		TimeToRecoverSec: s.TimeToRecoverSec,
		PeakLag:          float64(s.PeakLag),
		OrdersAffected:   max(0, s.PeakInFlight-s.StartKpis.InFlight) + s.DlqOrders,
		RevenueImpactEur: s.RevenueDeltaEur,
		Alerts:           slices.Clone(s.Alerts),
	}
	if rep.Alerts == nil {
		rep.Alerts = []string{}
	}
	if s.DetectedAt != nil {
		d := s.DetectedAt.Sub(*r.rec.StartedAt).Seconds()
		rep.TimeToDetectSec = &d
	}
	return rep
}

// observe evaluates checks every ObserveEvery and samples the report; it returns whether all
// checks passed (always evaluated when due, otherwise the previous results count).
func (e *Engine) observe(ctx context.Context, r *run, now time.Time, checks []Check) bool {
	if !r.observed.IsZero() && now.Sub(r.observed) < e.cfg.ObserveEvery {
		return false
	}
	r.observed = now
	cc := &CheckContext{Ctx: ctx, Env: r.env, Obs: e.cfg.Observer, Start: r.rec.Report.StartKpis, StartLag: r.rec.Report.StartLag, Now: now}
	obs := make([]api.Observation, 0, len(checks))
	all := true
	for _, c := range checks {
		got, ok := c.Eval(cc)
		all = all && ok
		obs = append(obs, api.Observation{Label: c.Label, Requested: c.Requested, Observed: got, Confirmed: ok, At: now.UTC()})
	}
	e.mu.Lock()
	r.rec.Report.sample(e.cfg.Observer, *r.rec.StartedAt, now)
	if r.rec.Exp.Status == api.StatusRunning {
		r.rec.Exp.Observations = obs
	} else {
		r.rec.Exp.Observations = append(keepRunning(r.rec.Exp.Observations), obs...)
	}
	e.mu.Unlock()
	e.changed(r)
	return all
}

// keepRunning drops the recovery observations of the previous evaluation.
func keepRunning(obs []api.Observation) []api.Observation {
	return slices.DeleteFunc(slices.Clone(obs), func(o api.Observation) bool { return strings.HasPrefix(o.Label, "↺") })
}

// Common checks.

func findService(o api.Observer, name string) (api.ServiceState, bool) {
	for _, s := range o.Services() {
		if s.Name == name {
			return s, true
		}
	}
	return api.ServiceState{}, false
}

func findBroker(o api.Observer, id int) (api.BrokerState, bool) {
	for _, b := range o.Brokers() {
		if b.ID == id {
			return b, true
		}
	}
	return api.BrokerState{}, false
}

// generationCheck: the service's pods applied (at least) generation gen. A service without
// pods counts as applied (it reads the newest snapshot on start).
func generationCheck(svc string, gen int64) Check {
	return Check{Label: "↺ " + svc + " runs the reverted settings", Requested: fmt.Sprintf("generation ≥ %d", gen),
		Eval: func(c *CheckContext) (string, bool) {
			s, ok := findService(c.Obs, svc)
			if !ok || s.ReadyReplicas == 0 {
				return "no running pods", true
			}
			return fmt.Sprintf("generation %d", s.AppliedGeneration), s.AppliedGeneration >= gen
		}}
}

// appliedCheck: the service applied the experiment's snapshot (the engine fills gen lazily).
func appliedCheck(e *Engine, svc string) Check {
	return Check{Label: svc + " applied the experiment settings", Requested: "newest control snapshot",
		Eval: func(c *CheckContext) (string, bool) {
			e.mu.Lock()
			gen := e.published[svc].gen
			e.mu.Unlock()
			s, ok := findService(c.Obs, svc)
			if !ok || s.ReadyReplicas == 0 {
				return "no running pods", false
			}
			return fmt.Sprintf("generation %d of %d", s.AppliedGeneration, gen), s.AppliedGeneration >= gen
		}}
}

func lagRecoveredCheck() Check {
	return Check{Label: "↺ consumer lag back to normal", Requested: "≤ 120 % of the lag before the start (+1000)",
		Eval: func(c *CheckContext) (string, bool) {
			lag := c.Obs.TotalLag()
			return fmt.Sprintf("%d records", lag), float64(lag) <= float64(c.StartLag)*1.2+1000
		}}
}

func lagGrowingCheck() Check {
	return Check{Label: "Consumer lag growing", Requested: "lag above the level before the start",
		Eval: func(c *CheckContext) (string, bool) {
			lag := c.Obs.TotalLag()
			return fmt.Sprintf("%d records (was %d)", lag, c.StartLag), lag > c.StartLag+100
		}}
}

func kpiCheck(label, requested string, value func(api.Kpis) float64, format string, ok func(now, start float64) bool) Check {
	return Check{Label: label, Requested: requested, Eval: func(c *CheckContext) (string, bool) {
		v := value(c.Obs.Kpis())
		return fmt.Sprintf(format, v), ok(v, value(c.Start))
	}}
}

func brokersHealthyCheck() Check {
	return Check{Label: "↺ all brokers up, nothing under-replicated", Requested: "3 brokers up, 0 under-replicated",
		Eval: func(c *CheckContext) (string, bool) {
			up, urp := 0, 0
			for _, b := range c.Obs.Brokers() {
				if b.Up {
					up++
				}
				urp += b.UnderReplicated
			}
			return fmt.Sprintf("%d up, %d under-replicated", up, urp), up >= 3 && urp == 0
		}}
}

func brokerDownCheck(id int) Check {
	return Check{Label: fmt.Sprintf("Broker %d stopped", id), Requested: "broker down",
		Eval: func(c *CheckContext) (string, bool) {
			b, ok := findBroker(c.Obs, id)
			if !ok || !b.Up {
				return "down", true
			}
			return "up", false
		}}
}

func underReplicatedCheck(label string) Check {
	return Check{Label: label, Requested: "under-replicated partitions > 0",
		Eval: func(c *CheckContext) (string, bool) {
			urp := 0
			for _, b := range c.Obs.Brokers() {
				urp += b.UnderReplicated
			}
			return fmt.Sprintf("%d under-replicated", urp), urp > 0
		}}
}

func replicasCheck(svc string, want func(ready int) bool, requested string) Check {
	return Check{Label: svc + " replicas", Requested: requested,
		Eval: func(c *CheckContext) (string, bool) {
			s, _ := findService(c.Obs, svc)
			return fmt.Sprintf("%d ready of %d", s.ReadyReplicas, s.Replicas), want(s.ReadyReplicas)
		}}
}
