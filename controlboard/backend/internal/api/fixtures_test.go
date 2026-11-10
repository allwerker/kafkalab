package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"kafkalab/pkg/kit/testkit"
)

// fixtureTypes maps a fixture file stem (or its prefix, e.g. "state-drill.json") to the Go
// type it must decode into. WebSocket messages are recognised by their envelope instead.
var fixtureTypes = map[string]func() any{
	"state":       func() any { return &State{} },
	"scenarios":   func() any { return &[]ScenarioDef{} },
	"scenario":    func() any { return &ScenarioDef{} },
	"experiments": func() any { return &[]Experiment{} },
	"experiment":  func() any { return &Experiment{} },
	"alerts":      func() any { return &[]Alert{} },
	"alert":       func() any { return &Alert{} },
	"timeline":    func() any { return &[]TimelineEntry{} },
	"links":       func() any { return &[]Link{} },
	"tuning":      func() any { return &TuningState{} },
	"drill":       func() any { return &Drill{} },
	"leaderboard": func() any { return &[]LeaderboardEntry{} },
	"order":       func() any { return &OrderTimeline{} },
	"tick":        func() any { return &Tick{} },
	"events":      func() any { return &Events{} },
	"hello":       func() any { return &Hello{} },
	"error":       func() any { return &Error{} },
	"chaos-arm":   func() any { return &ArmRequest{} },
	"panic":       func() any { return &PanicResponse{} },
	"simulation":  func() any { return &SimulationResponse{} },
	"service":     func() any { return &ServiceState{} },
}

// messageTypes maps a WebSocket message type to the type of its data.
var messageTypes = map[string]func() any{
	MsgHello: func() any { return &Hello{} }, MsgState: func() any { return &State{} }, MsgTick: func() any { return &Tick{} },
	MsgEvents: func() any { return &Events{} }, MsgAlert: func() any { return &Alert{} }, MsgExperiment: func() any { return &Experiment{} },
	MsgTimeline: func() any { return &TimelineEntry{} }, MsgDrill: func() any { return &Drill{} },
}

func strict(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(v)
}

// decodeMessage decodes one WebSocket envelope and its data.
func decodeMessage(b []byte) error {
	var env struct {
		Type string          `json:"type"`
		Ts   string          `json:"ts"`
		Data json.RawMessage `json:"data"`
	}
	if err := strict(b, &env); err != nil {
		return err
	}
	if env.Type == MsgPong {
		return nil
	}
	mk, ok := messageTypes[env.Type]
	if !ok {
		return &json.UnsupportedValueError{Str: "message type " + env.Type}
	}
	return strict(env.Data, mk())
}

func fixtureType(name string) (func() any, bool) {
	stem := strings.ToLower(strings.TrimSuffix(filepath.Base(name), filepath.Ext(name)))
	keys := make([]string, 0, len(fixtureTypes))
	for k := range fixtureTypes {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int { return len(b) - len(a) }) // longest prefix wins
	for _, k := range keys {
		if stem == k || strings.HasPrefix(stem, k+"-") || strings.HasPrefix(stem, k+"_") || strings.HasPrefix(stem, k+".") {
			return fixtureTypes[k], true
		}
	}
	return nil, false
}

func isEnvelope(b []byte) bool {
	var probe map[string]json.RawMessage
	if json.Unmarshal(b, &probe) != nil {
		return false
	}
	_, hasType := probe["type"]
	_, hasData := probe["data"]
	return hasType && (hasData || len(probe) <= 2)
}

// checkFixture returns every problem of one fixture file.
func checkFixture(path string) []error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return []error{err}
	}
	if strings.HasSuffix(path, ".jsonl") {
		var errs []error
		sc := bufio.NewScanner(bytes.NewReader(raw))
		sc.Buffer(make([]byte, 0, 1<<20), 16<<20)
		for line := 1; sc.Scan(); line++ {
			if b := bytes.TrimSpace(sc.Bytes()); len(b) > 0 {
				if err := decodeMessage(b); err != nil {
					errs = append(errs, fmt.Errorf("%s:%d: %w", path, line, err))
				}
			}
		}
		return errs
	}
	if isEnvelope(raw) {
		if err := decodeMessage(raw); err != nil {
			return []error{fmt.Errorf("%s: %w", path, err)}
		}
		return nil
	}
	mk, ok := fixtureType(path)
	if !ok {
		return []error{fmt.Errorf("%s: no Go type known for this fixture name (add it to fixtureTypes)", path)}
	}
	if err := strict(raw, mk()); err != nil {
		return []error{fmt.Errorf("%s: %w", path, err)}
	}
	return nil
}

func Test_shouldDecodeEveryRecordedFixtureWhenTheFrontendFixturesAreChecked(t *testing.T) {
	// GIVEN: the fixtures the frontend recorded in controlboard/api/fixtures
	dir := filepath.Join(testkit.RepoRoot(), "controlboard", "api", "fixtures")
	var files []string
	for _, pattern := range []string{"*.json", "*.jsonl"} {
		m, _ := filepath.Glob(filepath.Join(dir, pattern))
		files = append(files, m...)
	}
	if len(files) == 0 {
		t.Skip("no fixtures recorded yet in " + dir)
	}

	// WHEN: every fixture is decoded into the Go types without unknown fields
	// THEN: none of them uses a field or message the backend does not know
	for _, f := range files {
		for _, err := range checkFixture(f) {
			t.Error(err)
		}
	}
}

func Test_shouldRejectAFixtureWhenItHasAFieldTheBackendDoesNotKnow(t *testing.T) {
	// GIVEN: a recorded state with an unknown field, a message of an unknown type, a pong,
	// an unnamed fixture and a valid tick stream
	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	bad := write("state-broken.json", `{"simulation":"running","weather":"sunny"}`)
	stream := write("session.jsonl", `{"type":"tick","ts":"2026-11-10T12:00:00Z","data":{"kpis":{"ordersPerSec":50},"rates":{}}}`+"\n\n"+`{"type":"pong"}`+"\n"+`{"type":"weather","data":{}}`)
	single := write("hello-message.json", `{"type":"hello","ts":"2026-11-10T12:00:00Z","data":{"version":1,"serverTime":"2026-11-10T12:00:00Z"}}`)
	unnamed := write("misc.json", `[]`)
	broken := write("ws-hello.json", `{"type":"hello","data":{"version":"one"}}`)

	// WHEN: each is checked with a recording test
	var results []bool
	for _, f := range []string{bad, stream, single, unnamed, broken} {
		results = append(results, len(checkFixture(f)) > 0)
	}
	missing := checkFixture(filepath.Join(dir, "gone.json"))

	// THEN: the unknown field, the unknown message type, the unnamed file and the wrong value fail; the valid hello passes
	if want := []bool{true, true, false, true, true}; !slices.Equal(results, want) {
		t.Fatalf("failed %v, want %v", results, want)
	}
	if len(missing) != 1 || isEnvelope([]byte("[")) {
		t.Fatal("helpers misbehave")
	}
}
