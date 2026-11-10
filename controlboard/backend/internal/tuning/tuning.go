// Package tuning is the Tuning Lab: the allowlisted Kafka client settings a player may change
// per service. Values are published as knobs.client in the service's control snapshot; the
// kit rebuilds the business client with them (pkg/kit/kafka.TuningOptions).
package tuning

import (
	"fmt"
	"math"
	"slices"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/events"
)

func f(v float64) *float64 { return &v }

// Settings returns the catalogue in display order.
func Settings() []api.TuningSetting {
	return []api.TuningSetting{
		{Key: "acks", Label: "Producer acks", Type: "select", Options: []string{"all", "leader", "none"}, Default: "all",
			Lossy: []string{"leader", "none"},
			Help:  "all waits for every in-sync replica (idempotent, no loss). leader/none disable idempotence: faster, but records can be lost on broker failure."},
		{Key: "lingerMs", Label: "Producer linger", Type: "number", Min: f(0), Max: f(500), Step: f(1), Default: 5.0,
			Help: "How long the producer waits to fill a batch. Higher = bigger batches, fewer requests, more latency."},
		{Key: "batchMaxBytes", Label: "Producer batch max bytes", Type: "number", Min: f(16 << 10), Max: f(4 << 20), Step: f(1024), Default: float64(1 << 20),
			Help: "Upper bound of one record batch per partition."},
		{Key: "compression", Label: "Compression", Type: "select", Options: []string{"none", "gzip", "snappy", "lz4", "zstd"}, Default: "none",
			Help: "Batch compression: less network and disk, more CPU. Watch throughput and broker bytes in/out."},
		{Key: "fetchMaxBytes", Label: "Consumer fetch max bytes", Type: "number", Min: f(64 << 10), Max: f(64 << 20), Step: f(1024), Default: float64(8 << 20),
			Help: "Maximum bytes per fetch response."},
		{Key: "fetchMaxWaitMs", Label: "Consumer fetch max wait", Type: "number", Min: f(10), Max: f(1000), Step: f(10), Default: 500.0,
			Help: "How long the broker holds a fetch until fetch min bytes are available."},
		{Key: "fetchMinBytes", Label: "Consumer fetch min bytes", Type: "number", Min: f(1), Max: f(1 << 20), Step: f(1), Default: 1.0,
			Help: "The broker answers a fetch once this many bytes are available (or max wait passed)."},
		{Key: "maxPollRecords", Label: "Max poll records", Type: "number", Min: f(10), Max: f(2000), Step: f(10), Default: 500.0,
			Help: "Records per poll = per database transaction batch in the kit runner."},
	}
}

func byKey() map[string]api.TuningSetting {
	m := map[string]api.TuningSetting{}
	for _, s := range Settings() {
		m[s.Key] = s
	}
	return m
}

// Validate checks keys, types and ranges; nil values (back to default) are allowed.
func Validate(v api.Values) error {
	settings := byKey()
	for k, val := range v {
		s, ok := settings[k]
		if !ok {
			return fmt.Errorf("%w: unknown tuning setting %q", api.ErrInvalid, k)
		}
		if val == nil {
			continue
		}
		if s.Type == "select" {
			str, ok := val.(string)
			if !ok || !slices.Contains(s.Options, str) {
				return fmt.Errorf("%w: %s must be one of %v", api.ErrInvalid, k, s.Options)
			}
			continue
		}
		n, ok := val.(float64)
		if i, isInt := val.(int); isInt {
			n, ok = float64(i), true
		}
		if !ok || n != math.Trunc(n) || n < *s.Min || n > *s.Max {
			return fmt.Errorf("%w: %s must be a whole number in %v..%v", api.ErrInvalid, k, *s.Min, *s.Max)
		}
	}
	return nil
}

// Merge returns cur with upd applied (nil removes a setting); the result never contains nil.
func Merge(cur, upd api.Values) api.Values {
	out := api.Values{}
	for k, v := range cur {
		out[k] = v
	}
	for k, v := range upd {
		if v == nil {
			delete(out, k)
		} else {
			out[k] = v
		}
	}
	return out
}

// Lossy returns the settings in v that weaken delivery guarantees.
func Lossy(v api.Values) []string {
	var out []string
	for _, s := range Settings() {
		if val, ok := v[s.Key].(string); ok && slices.Contains(s.Lossy, val) {
			out = append(out, s.Key+"="+val)
		}
	}
	return out
}

// ToClient converts validated values to the snapshot's client settings.
func ToClient(v api.Values) events.ClientTuning {
	var c events.ClientTuning
	str := func(k string) *string {
		if s, ok := v[k].(string); ok {
			return &s
		}
		return nil
	}
	num := func(k string) *int32 {
		switch n := v[k].(type) {
		case float64:
			x := int32(n)
			return &x
		case int:
			x := int32(n)
			return &x
		}
		return nil
	}
	c.Acks = str("acks")
	c.Compression = str("compression")
	c.LingerMs = num("lingerMs")
	c.BatchMaxBytes = num("batchMaxBytes")
	c.FetchMaxBytes = num("fetchMaxBytes")
	c.FetchMaxWaitMs = num("fetchMaxWaitMs")
	c.FetchMinBytes = num("fetchMinBytes")
	c.MaxPollRecords = num("maxPollRecords")
	return c
}
