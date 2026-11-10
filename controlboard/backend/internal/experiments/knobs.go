package experiments

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/pkg/events"
)

// Knob values travel as api.Values keyed by the schemas/platform.avdl field names. The codec
// below is derived from the generated events.Knobs by reflection, so it can never drift from
// the frozen schema. "client" is not a knob here: it belongs to the Tuning Lab.

type knobField struct {
	index int
	name  string
	kind  reflect.Type
	// max is the upper bound of numeric knobs (probabilities 1, rates 10000, ms 60000).
	max float64
}

var knobFields = func() map[string]knobField {
	out := map[string]knobField{}
	t := reflect.TypeOf(events.Knobs{})
	for i := range t.NumField() {
		f := t.Field(i)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "client" {
			continue
		}
		max := 60000.0
		switch {
		case strings.HasSuffix(name, "Probability"), strings.HasSuffix(name, "Ratio"):
			max = 1
		case name == "ordersPerSecond":
			max = 10000
		case name == "paddingBytes":
			max = 900000 // stays below the broker's 1 MiB message limit
		case name == "hotPartition":
			max = 63
		}
		out[name] = knobField{index: i, name: name, kind: f.Type, max: max}
	}
	return out
}()

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	}
	return 0, false
}

// validateKnobs checks names, types and ranges; nil values (remove the override) are allowed.
func validateKnobs(v api.Values) error {
	for name, val := range v {
		f, ok := knobFields[name]
		if !ok {
			return fmt.Errorf("%w: unknown knob %q", api.ErrInvalid, name)
		}
		if val == nil {
			continue
		}
		if err := f.check(val); err != nil {
			return fmt.Errorf("%w: knob %s: %v", api.ErrInvalid, name, err)
		}
	}
	return nil
}

func (f knobField) check(val any) error {
	switch f.kind.Kind() {
	case reflect.Bool:
		if _, ok := val.(bool); !ok {
			return fmt.Errorf("want true or false, got %v", val)
		}
		return nil
	case reflect.Pointer:
		if f.kind.Elem().Kind() == reflect.String {
			if s, ok := val.(string); !ok || s == "" {
				return fmt.Errorf("want a non-empty string, got %v", val)
			}
			return nil
		}
	}
	n, ok := toFloat(val)
	if !ok || math.IsNaN(n) {
		return fmt.Errorf("want a number, got %v", val)
	}
	if n < 0 || n > f.max {
		return fmt.Errorf("%v is outside 0..%v", n, f.max)
	}
	integer := f.kind.Kind() == reflect.Int32 || (f.kind.Kind() == reflect.Pointer && f.kind.Elem().Kind() == reflect.Int32)
	if integer && n != math.Trunc(n) {
		return fmt.Errorf("want a whole number, got %v", n)
	}
	return nil
}

// setKnob writes one validated, non-nil value.
func setKnob(k *events.Knobs, name string, val any) {
	f := knobFields[name]
	fv := reflect.ValueOf(k).Elem().Field(f.index)
	switch f.kind.Kind() {
	case reflect.Bool:
		fv.SetBool(val.(bool))
	case reflect.Int32:
		n, _ := toFloat(val)
		fv.SetInt(int64(n))
	case reflect.Float64:
		n, _ := toFloat(val)
		fv.SetFloat(n)
	default: // pointer
		p := reflect.New(f.kind.Elem())
		switch f.kind.Elem().Kind() {
		case reflect.String:
			p.Elem().SetString(val.(string))
		case reflect.Int32:
			n, _ := toFloat(val)
			p.Elem().SetInt(int64(n))
		default:
			n, _ := toFloat(val)
			p.Elem().SetFloat(n)
		}
		fv.Set(p)
	}
}

// getKnob returns the knob's value and whether it differs from the neutral default.
func getKnob(k events.Knobs, name string) (any, bool) {
	f := knobFields[name]
	fv := reflect.ValueOf(k).Field(f.index)
	switch f.kind.Kind() {
	case reflect.Bool:
		return fv.Bool(), fv.Bool()
	case reflect.Int32:
		return int(fv.Int()), fv.Int() != 0
	case reflect.Float64:
		return fv.Float(), fv.Float() != 0
	}
	if fv.IsNil() {
		return nil, false
	}
	switch e := fv.Elem(); e.Kind() {
	case reflect.String:
		return e.String(), true
	case reflect.Int32:
		return int(e.Int()), true
	default:
		return e.Float(), true
	}
}

// assignKnobs applies manual overrides: every given value replaces the current one.
func assignKnobs(k *events.Knobs, v api.Values) {
	for name, val := range v {
		setKnob(k, name, val)
	}
}

// mergeKnobs layers an experiment overlay onto k so that concurrent experiments add up instead
// of the last one winning: numbers take the maximum, booleans are ORed, strings are replaced.
func mergeKnobs(k *events.Knobs, v api.Values) {
	for name, val := range v {
		cur, set := getKnob(*k, name)
		if !set || val == nil {
			if val != nil {
				setKnob(k, name, val)
			}
			continue
		}
		switch c := cur.(type) {
		case bool:
			setKnob(k, name, c || val.(bool))
		case string:
			setKnob(k, name, val)
		default:
			a, _ := toFloat(c)
			b, _ := toFloat(val)
			setKnob(k, name, math.Max(a, b))
		}
	}
}

// nonDefaultKnobs returns the knobs that differ from the neutral default.
func nonDefaultKnobs(k events.Knobs) api.Values {
	out := api.Values{}
	for name := range knobFields {
		if v, set := getKnob(k, name); set {
			out[name] = v
		}
	}
	return out
}

func copyValues(v api.Values) api.Values {
	out := make(api.Values, len(v))
	for k, x := range v {
		out[k] = x
	}
	return out
}
