package board

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"kafkalab/controlboard/backend/internal/alerts"
	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/api/fakeexp"
	"kafkalab/controlboard/backend/internal/grafana"
	"kafkalab/controlboard/backend/internal/observe"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

type stubFeed struct {
	mu    sync.Mutex
	items []api.LiveEvent
}

func (f *stubFeed) Rates() map[string]float64 { return map[string]float64{"orders.created": 50} }
func (f *stubFeed) Drain() api.Events {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := f.items
	f.items = nil
	return api.Events{Scale: 1, Items: append([]api.LiveEvent{}, items...)}
}
func (f *stubFeed) Order(id string) (api.OrderTimeline, bool) {
	if id != "o-1" {
		return api.OrderTimeline{}, false
	}
	return api.OrderTimeline{OrderID: "o-1", Events: []api.LiveEvent{{OrderID: "o-1", Kind: api.EventCreated, At: t0}}}, true
}

type stubObserved struct{ snap observe.Snapshot }

func (s stubObserved) Snapshot() observe.Snapshot { return s.snap }

type stubLinks struct{}

func (stubLinks) Links() []api.Link {
	return []api.Link{{ID: "grafana", Title: "Grafana", URL: "http://grafana.localhost:8080", Health: api.HealthUp}}
}

// lab is a running lab: payment healthy, inventory with one of two pods ready, shipping scaled
// to zero, notification without kube-state-metrics data, brokers 0-2 with 1 as controller.
func lab() observe.Snapshot {
	return observe.Snapshot{
		Kpis:    api.Kpis{OrdersPerSec: 50, TotalLag: 12},
		Brokers: []api.BrokerState{{ID: 0, Up: true}, {ID: 1, Up: true, IsController: true}, {ID: 2, Up: true}},
		Services: map[string]observe.ServiceObs{
			"order-generator": {Known: true, Replicas: 1, ReadyReplicas: 1, AppliedGeneration: 3},
			"payment":         {Known: true, Replicas: 1, ReadyReplicas: 1},
			"inventory":       {Known: true, Replicas: 2, ReadyReplicas: 1},
			"shipping":        {Known: true, Replicas: 0, ReadyReplicas: 0},
			"analytics":       {Known: true, Replicas: 1, ReadyReplicas: 1},
		},
		Lag: observe.Lag{Total: 12, ByGroup: map[string]map[string][]int64{"payment": {"orders.created": {12, 0, 0}}}, Partitions: map[string]int{"orders.created": 3}},
	}
}

type env struct {
	b      *Board
	exp    *fakeexp.Fake
	feed   *stubFeed
	store  *alerts.Memory
	alerts *alerts.Manager
	srv    *httptest.Server
	fire   error
}

func newEnv(t *testing.T, mod ...func(*Options)) *env {
	t.Helper()
	e := &env{exp: fakeexp.New(func() time.Time { return t0 }), feed: &stubFeed{}, store: &alerts.Memory{}}
	var b *Board
	e.alerts = alerts.New(alerts.Options{Store: e.store, OnChange: func(a api.Alert) { b.AlertChanged(a) },
		OnTimeline: func(te api.TimelineEntry) { b.AddTimeline(te) }, Now: func() time.Time { return t0 }})
	gl := grafana.Links{Base: "http://grafana.localhost:8080"}
	o := Options{Feed: e.feed, Observed: stubObserved{lab()}, Alerts: e.alerts, Links: stubLinks{},
		ParseWebhook: func(body []byte) ([]api.Alert, error) { return alerts.ParseWebhook(bytes.NewReader(body), gl) },
		FireTest:     func(context.Context) error { return e.fire },
		Now:          func() time.Time { return t0 }}
	for _, m := range mod {
		m(&o)
	}
	b = New(o)
	b.SetExperiments(e.exp)
	e.b = b
	mux := http.NewServeMux()
	b.Register(mux)
	e.srv = httptest.NewServer(mux)
	t.Cleanup(e.srv.Close)
	return e
}

func (e *env) do(t *testing.T, method, path, body string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, r)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (e *env) json(t *testing.T, method, path, body string, want int, v any) {
	t.Helper()
	status, b := e.do(t, method, path, body)
	if status != want {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, status, want, b)
	}
	if v != nil {
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatalf("%s %s: %v: %s", method, path, err, b)
		}
	}
}

func errCode(t *testing.T, e *env, method, path, body string, status int) string {
	t.Helper()
	var er api.Error
	e.json(t, method, path, body, status, &er)
	if er.Message == "" {
		t.Fatalf("%s %s: no message", method, path)
	}
	return er.Error
}

func Test_shouldComposeTheFullStateWhenTheBoardIsOpened(t *testing.T) {
	// GIVEN: the running lab with payment paused by hand and shipping scaled to zero by the board
	e := newEnv(t)
	_ = e.exp.SetKnobs(context.Background(), "payment", api.Values{"paused": true})
	_ = e.exp.Scale(context.Background(), "order-generator", 1)

	// WHEN: the browser loads the state
	var s api.State
	e.json(t, "GET", "/api/state", "", 200, &s)

	// THEN: services carry desired and observed values with a health each
	health := map[string]string{}
	for _, svc := range s.Services {
		health[svc.Name] = svc.Health
	}
	want := map[string]string{"order-generator": "up", "payment": "degraded", "inventory": "degraded", "shipping": "down", "notification": "unknown", "analytics": "up"}
	for k, v := range want {
		if health[k] != v {
			t.Fatalf("%s: %s, want %s", k, health[k], v)
		}
	}
	if !s.Services[1].Paused || s.Services[1].DesiredGeneration != 1 || s.Services[0].AppliedGeneration != 3 || s.Services[2].Knobs == nil {
		t.Fatalf("services %+v", s.Services)
	}
	// AND: brokers, KPIs, links and the simulation state are included
	if len(s.Brokers) != 3 || !s.Brokers[1].IsController || s.Kpis.OrdersPerSec != 50 || len(s.Links) != 1 || s.Simulation != "running" || s.ChaosArmed || s.Drill != nil {
		t.Fatalf("state %+v", s)
	}
}

func Test_shouldStartAndStopAnExperimentWhenTheUserRunsAScenario(t *testing.T) {
	// GIVEN: the scenario catalog
	e := newEnv(t)
	var defs []api.ScenarioDef
	e.json(t, "GET", "/api/scenarios", "", 200, &defs)

	// WHEN: the user starts the traffic scenario for 60 s and stops it again
	var started, stopped api.Experiment
	e.json(t, "POST", "/api/experiments", `{"scenario":"fake-scenario","params":{"factor":3},"durationSec":60,"autoRecover":true}`, 200, &started)
	e.json(t, "DELETE", "/api/experiments/"+started.ID, "", 200, &stopped)

	// THEN: it ran and was aborted
	if len(defs) != 2 || started.Status != api.StatusRunning || stopped.Status != api.StatusAborted {
		t.Fatalf("started %+v stopped %+v", started, stopped)
	}
}

func Test_shouldExplainTheRefusalWhenAnExperimentCannotStart(t *testing.T) {
	// GIVEN: chaos is disarmed
	e := newEnv(t)

	// WHEN: a broker stop, an unknown scenario, a broken body, an unconfirmed broker stop and an unknown stop are sent
	disarmed := errCode(t, e, "POST", "/api/experiments", `{"scenario":"fake-broker-stop"}`, 409)
	unknown := errCode(t, e, "POST", "/api/experiments", `{"scenario":"nope"}`, 404)
	broken := errCode(t, e, "POST", "/api/experiments", `{`, 400)
	var armed api.ArmRequest
	e.json(t, "POST", "/api/chaos/arm", `{"armed":true}`, 200, &armed)
	unconfirmed := errCode(t, e, "POST", "/api/experiments", `{"scenario":"fake-broker-stop","confirm":"x"}`, 400)
	e.json(t, "POST", "/api/experiments", `{"scenario":"fake-broker-stop","confirm":"fake-broker-stop"}`, 200, nil)
	busy := errCode(t, e, "POST", "/api/experiments", `{"scenario":"fake-broker-stop","confirm":"fake-broker-stop"}`, 409)
	missing := errCode(t, e, "DELETE", "/api/experiments/exp-404", "", 404)

	// THEN: each refusal carries the contract's error code
	got := []string{disarmed, unknown, broken, unconfirmed, busy, missing}
	want := []string{"chaos_disarmed", "unknown_scenario", "invalid_request", "confirmation_required", "broker_fault_active", "not_found"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("refusal %d: %s, want %s", i, got[i], want[i])
		}
	}
	if !armed.Armed {
		t.Fatal("chaos not armed")
	}
}

func Test_shouldRevertEverythingWhenPanicIsPressed(t *testing.T) {
	// GIVEN: a running experiment, armed chaos and a broken arm request
	e := newEnv(t)
	e.json(t, "POST", "/api/experiments", `{"scenario":"fake-scenario"}`, 200, nil)
	e.json(t, "POST", "/api/chaos/arm", `{"armed":true}`, 200, nil)
	broken := errCode(t, e, "POST", "/api/chaos/arm", `armed`, 400)

	// WHEN: panic is pressed
	var p api.PanicResponse
	e.json(t, "POST", "/api/panic", "", 200, &p)

	// THEN: one thing was reverted and chaos is disarmed
	if p.Reverted != 1 || e.exp.Armed() || broken != "invalid_request" {
		t.Fatalf("got %+v", p)
	}
}

func Test_shouldStopTheSimulationWhenTheUserPressesStop(t *testing.T) {
	// GIVEN: a running simulation
	e := newEnv(t)

	// WHEN: stop, an unknown action and a broken body are sent
	var s api.SimulationResponse
	e.json(t, "POST", "/api/simulation", `{"action":"stop"}`, 200, &s)
	bad := errCode(t, e, "POST", "/api/simulation", `{"action":"explode"}`, 400)
	broken := errCode(t, e, "POST", "/api/simulation", `[`, 400)

	// THEN: the simulation is stopped and the others are invalid
	if s.Simulation != "stopped" || bad != "invalid_request" || broken != "invalid_request" {
		t.Fatalf("got %+v", s)
	}
}

func Test_shouldAnswerWithTheServiceStateWhenKnobsAreOverriddenAndReset(t *testing.T) {
	// GIVEN: the payment service at baseline
	e := newEnv(t)

	// WHEN: it is paused by hand, then reset
	var paused, reset api.ServiceState
	e.json(t, "PUT", "/api/services/payment/knobs", `{"paused":true,"processingLatencyMs":200}`, 200, &paused)
	e.json(t, "DELETE", "/api/services/payment/knobs", "", 200, &reset)

	// THEN: the answer shows the override and then the baseline
	if !paused.Paused || paused.Knobs["processingLatencyMs"] != 200.0 || paused.Health != api.HealthDegraded || reset.Paused || len(reset.Knobs) != 0 {
		t.Fatalf("paused %+v reset %+v", paused, reset)
	}
}

func Test_shouldRefuseKnobsAndScalingWhenServiceOrValuesAreWrong(t *testing.T) {
	// GIVEN: the board
	e := newEnv(t)

	// WHEN: an unknown service, a broken body, too many replicas and a broken scale body are sent
	unknown := errCode(t, e, "PUT", "/api/services/billing/knobs", `{"paused":true}`, 404)
	broken := errCode(t, e, "PUT", "/api/services/payment/knobs", `{`, 400)
	tooMany := errCode(t, e, "POST", "/api/services/payment/scale", `{"replicas":7}`, 400)
	brokenScale := errCode(t, e, "POST", "/api/services/payment/scale", `x`, 400)
	unknownScale := errCode(t, e, "POST", "/api/services/billing/scale", `{"replicas":1}`, 404)

	// THEN: each is refused with the matching code
	if unknown != "unknown_service" || broken != "invalid_request" || tooMany != "invalid_request" || brokenScale != "invalid_request" || unknownScale != "unknown_service" {
		t.Fatalf("%s %s %s %s %s", unknown, broken, tooMany, brokenScale, unknownScale)
	}
}

func Test_shouldReportTheDesiredReplicasWhenAServiceIsScaled(t *testing.T) {
	// GIVEN: inventory with two replicas
	e := newEnv(t)

	// WHEN: it is scaled to 4
	var s api.ServiceState
	e.json(t, "POST", "/api/services/inventory/scale", `{"replicas":4}`, 200, &s)

	// THEN: desired is 4 while only one pod is ready so far
	if s.Replicas != 4 || s.ReadyReplicas != 1 || s.Health != api.HealthDegraded {
		t.Fatalf("got %+v", s)
	}
}

func Test_shouldAnswerUnknownServiceWhenTheEngineAcceptsAServiceTheBoardDoesNotList(t *testing.T) {
	// GIVEN: a board whose request names a service missing from the control list
	e := newEnv(t)
	req := httptest.NewRequest("POST", "/api/services/ghost/scale", nil)
	req.SetPathValue("name", "ghost")

	// WHEN: the merged service state is looked up after a successful engine call
	_, err := e.b.service(req, nil)

	// THEN: it is reported as unknown
	if !errors.Is(err, api.ErrUnknownService) {
		t.Fatalf("got %v", err)
	}
}

func Test_shouldListTheTimelineNewestFirstWhenItIsRequestedWithALimit(t *testing.T) {
	// GIVEN: three timeline entries
	e := newEnv(t)
	for _, title := range []string{"first", "second", "third"} {
		e.b.AddTimeline(api.TimelineEntry{Kind: api.KindSystem, Severity: api.SeverityInfo, Title: title})
	}

	// WHEN: the last two, the default amount and an invalid amount are requested
	var two, all []api.TimelineEntry
	e.json(t, "GET", "/api/timeline?limit=2", "", 200, &two)
	e.json(t, "GET", "/api/timeline", "", 200, &all)
	bad := errCode(t, e, "GET", "/api/timeline?limit=zero", "", 400)

	// THEN: the newest come first, ids and times are filled in
	if len(two) != 2 || two[0].Title != "third" || two[0].ID != "tl-3" || !two[0].At.Equal(t0) || len(all) != 3 || bad != "invalid_request" {
		t.Fatalf("two %+v all %d", two, len(all))
	}
}

func Test_shouldKeepOnlyTheNewest500EntriesWhenTheTimelineOverflows(t *testing.T) {
	// GIVEN: a board
	e := newEnv(t)

	// WHEN: 510 entries are added
	for range 510 {
		e.b.AddTimeline(api.TimelineEntry{Title: "x", ID: "given", At: t0.Add(time.Hour)})
	}

	// THEN: 500 remain, keeping given ids and times
	all := e.b.Timeline(1000)
	if len(all) != TimelineSize || all[0].ID != "given" || !all[0].At.Equal(t0.Add(time.Hour)) {
		t.Fatalf("got %d", len(all))
	}
}

const webhook = `{"version":"4","status":"firing","alerts":[{"status":"firing","labels":{"alertname":"KafkaBrokerDown","severity":"critical","component":"kafka"},
 "annotations":{"summary":"broker 1 is down"},"startsAt":"2026-11-10T11:59:00Z","fingerprint":"f1"}]}`

func Test_shouldListTheAlertWhenAlertmanagerCallsTheWebhook(t *testing.T) {
	// GIVEN: the board without alerts
	e := newEnv(t)

	// WHEN: Alertmanager notifies a broker outage
	status, _ := e.do(t, "POST", "/api/alerts/webhook", webhook)
	var list []api.Alert
	e.json(t, "GET", "/api/alerts", "", 200, &list)

	// THEN: the webhook is acknowledged, the alert is listed and on the timeline
	if status != 204 || len(list) != 1 || list[0].Name != "KafkaBrokerDown" || e.b.Timeline(1)[0].Title != "Alert firing: KafkaBrokerDown" {
		t.Fatalf("status %d list %+v", status, list)
	}
}

func Test_shouldRejectOrRetryTheWebhookWhenItIsBrokenTooLargeOrCannotBeStored(t *testing.T) {
	// GIVEN: the board
	e := newEnv(t)

	// WHEN: a broken body, a body over 1 MiB and a valid body during a database outage arrive
	broken := errCode(t, e, "POST", "/api/alerts/webhook", `{"alerts":[`, 400)
	huge := errCode(t, e, "POST", "/api/alerts/webhook", strings.Repeat(" ", maxBody+1), 400)
	e.store.Err = errors.New("connection refused")
	down := errCode(t, e, "POST", "/api/alerts/webhook", webhook, 500)

	// THEN: broken input is refused, the outage is a 500 so Alertmanager retries
	if broken != "invalid_request" || huge != "invalid_request" || down != "internal" {
		t.Fatalf("%s %s %s", broken, huge, down)
	}
}

func Test_shouldFireTheTestAlertWhenTheUserAsksForIt(t *testing.T) {
	// GIVEN: a reachable Alertmanager, later an unreachable one
	e := newEnv(t)

	// WHEN: the test alert is requested twice
	ok, _ := e.do(t, "POST", "/api/alerts/test", "")
	e.fire = api.ErrUnavailable
	down := errCode(t, e, "POST", "/api/alerts/test", "", 503)

	// THEN: the first is accepted, the second reports Alertmanager as unavailable
	if ok != 204 || down != "unavailable" {
		t.Fatalf("%d %s", ok, down)
	}
}

func Test_shouldServeLinksAndTuningWhenTheToolPagesAreOpened(t *testing.T) {
	// GIVEN: the board
	e := newEnv(t)

	// WHEN: the links and the Tuning Lab are loaded and payment's acks are changed
	var links []api.Link
	var tuning, changed api.TuningState
	e.json(t, "GET", "/api/links", "", 200, &links)
	e.json(t, "GET", "/api/tuning", "", 200, &tuning)
	e.json(t, "PUT", "/api/tuning/payment", `{"acks":"leader"}`, 200, &changed)
	broken := errCode(t, e, "PUT", "/api/tuning/payment", `{`, 400)

	// THEN: everything is served and the change is visible
	if len(links) != 1 || len(tuning.Settings) != 8 || changed.Services["payment"]["acks"] != "leader" || broken != "invalid_request" {
		t.Fatalf("links %+v tuning %+v", links, changed)
	}
}

func Test_shouldPlayADrillWhenThePlayerStartsHintsGuessesAndReveals(t *testing.T) {
	// GIVEN: player ada
	e := newEnv(t)

	// WHEN: she starts a drill, takes a hint, guesses right, and starts and reveals a second one
	var d, hinted, solved, second, revealed api.Drill
	e.json(t, "POST", "/api/drills", `{"difficulty":"easy","player":"ada"}`, 200, &d)
	active := errCode(t, e, "POST", "/api/drills", `{"difficulty":"easy","player":"bob"}`, 409)
	e.json(t, "POST", "/api/drills/"+d.ID+"/hint", "", 200, &hinted)
	e.json(t, "POST", "/api/drills/"+d.ID+"/guess", `{"scenario":"fake-scenario"}`, 200, &solved)
	finished := errCode(t, e, "POST", "/api/drills/"+d.ID+"/hint", "", 409)
	e.json(t, "POST", "/api/drills", `{"difficulty":"hard","player":"ada"}`, 200, &second)
	e.json(t, "POST", "/api/drills/"+second.ID+"/reveal", "", 200, &revealed)
	var board []api.LeaderboardEntry
	e.json(t, "GET", "/api/drills/leaderboard", "", 200, &board)
	badStart := errCode(t, e, "POST", "/api/drills", `{`, 400)
	badGuess := errCode(t, e, "POST", "/api/drills/"+d.ID+"/guess", `{`, 400)

	// THEN: the drill is solved with a score, the second revealed, ada on the leaderboard
	if hinted.HintsUsed != 1 || solved.Status != "solved" || revealed.Status != "revealed" || len(board) != 1 || board[0].Player != "ada" {
		t.Fatalf("solved %+v revealed %+v board %+v", solved, revealed, board)
	}
	if badStart != "invalid_request" || badGuess != "invalid_request" || active != "drill_active" || finished != "drill_finished" {
		t.Fatalf("%s %s", badStart, badGuess)
	}
}

func Test_shouldShowTheOrderTimelineWhenARecentOrderIsOpened(t *testing.T) {
	// GIVEN: order o-1 is in the ring, o-2 is not
	e := newEnv(t)

	// WHEN: both are opened, and an unknown API path is called
	var tl api.OrderTimeline
	e.json(t, "GET", "/api/orders/o-1", "", 200, &tl)
	missing := errCode(t, e, "GET", "/api/orders/o-2", "", 404)
	unknown := errCode(t, e, "GET", "/api/nope", "", 404)

	// THEN: o-1 has its events, the others are not found as JSON errors
	if tl.OrderID != "o-1" || len(tl.Events) != 1 || missing != "not_found" || unknown != "not_found" {
		t.Fatalf("got %+v %s %s", tl, missing, unknown)
	}
}

type failingExp struct{ *fakeexp.Fake }

func (failingExp) Arm(context.Context, bool) error { return errors.New("kafka unreachable") }

func Test_shouldAnswerInternalErrorWhenTheEngineFailsUnexpectedly(t *testing.T) {
	// GIVEN: an engine that cannot publish to Kafka
	e := newEnv(t)
	e.b.SetExperiments(failingExp{e.exp})

	// WHEN: chaos is armed
	code := errCode(t, e, "POST", "/api/chaos/arm", `{"armed":true}`, 500)

	// THEN: the user sees an internal error with the reason
	if code != "internal" {
		t.Fatalf("got %s", code)
	}
}

func Test_shouldExposeWhatTheBoardObservesWhenTheEngineAsks(t *testing.T) {
	// GIVEN: the running lab with one firing alert
	e := newEnv(t)
	e.do(t, "POST", "/api/alerts/webhook", webhook)

	// WHEN: the engine reads the observer
	o := e.b.Observer()

	// THEN: KPIs, brokers, services, alerts and lag are those of the board
	if o.Kpis().OrdersPerSec != 50 || len(o.Brokers()) != 3 || len(o.Services()) != 6 || len(o.Alerts()) != 1 || o.TotalLag() != 12 {
		t.Fatal("observer does not match the board")
	}
}

// closingExp is an engine whose subscription ends immediately (engine shut down).
type closingExp struct{ *fakeexp.Fake }

func (closingExp) Subscribe() (<-chan api.Event, func()) {
	ch := make(chan api.Event)
	close(ch)
	return ch, func() {}
}

type wsMsg struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func readUntil(t *testing.T, c *websocket.Conn, types ...string) map[string]json.RawMessage {
	t.Helper()
	got := map[string]json.RawMessage{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for len(got) < len(types) {
		_, b, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("waiting for %v, got %v: %v", types, keys(got), err)
		}
		var m wsMsg
		_ = json.Unmarshal(b, &m)
		for _, typ := range types {
			if m.Type == typ {
				got[typ] = m.Data
			}
		}
	}
	return got
}

func keys(m map[string]json.RawMessage) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	return out
}

func Test_shouldStreamHelloStateTicksEventsAndChangesWhenABrowserConnects(t *testing.T) {
	// GIVEN: a board with fast intervals and a connected browser
	e := newEnv(t, func(o *Options) {
		o.TickEvery, o.EventsEvery, o.StateEvery = 20*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.b.Run(ctx); close(done) }()
	c, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/api/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	first := readUntil(t, c, api.MsgHello, api.MsgState)

	// WHEN: an order arrives, an experiment starts, a drill starts and an alert fires
	e.feed.mu.Lock()
	e.feed.items = []api.LiveEvent{{OrderID: "o-1", Kind: api.EventCreated}}
	e.feed.mu.Unlock()
	streamed := readUntil(t, c, api.MsgTick, api.MsgEvents, api.MsgState)
	_, _ = e.exp.Start(context.Background(), api.StartRequest{Scenario: fakeexp.ScenarioTraffic})
	_, _ = e.exp.Drills().Start(context.Background(), "easy", "ada")
	e.do(t, "POST", "/api/alerts/webhook", webhook)
	changes := readUntil(t, c, api.MsgExperiment, api.MsgTimeline, api.MsgDrill, api.MsgAlert)
	cancel()
	<-done

	// THEN: the browser got the greeting, the periodic messages and every change
	var hello api.Hello
	_ = json.Unmarshal(first[api.MsgHello], &hello)
	var tick api.Tick
	_ = json.Unmarshal(streamed[api.MsgTick], &tick)
	if hello.Version != 1 || tick.Rates["orders.created"] != 50 || tick.Lag["payment"]["orders.created"][0] != 12 || tick.Partitions["orders.created"] != 3 {
		t.Fatalf("hello %+v tick %+v", hello, tick)
	}
	if !strings.Contains(string(changes[api.MsgAlert]), "KafkaBrokerDown") {
		t.Fatalf("alert %s", changes[api.MsgAlert])
	}
}

func Test_shouldKeepStreamingWhenTheEngineClosesItsSubscription(t *testing.T) {
	// GIVEN: an engine that ends its subscription right away
	e := newEnv(t, func(o *Options) { o.TickEvery = 10 * time.Millisecond })
	e.b.SetExperiments(closingExp{e.exp})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.b.Run(ctx); close(done) }()
	c, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(e.srv.URL, "http")+"/api/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()

	// WHEN: the board keeps running
	got := readUntil(t, c, api.MsgTick)
	cancel()
	<-done

	// THEN: ticks still arrive
	if got[api.MsgTick] == nil {
		t.Fatal("no tick")
	}
}

func Test_shouldUseTheContractIntervalsWhenNoneAreConfigured(t *testing.T) {
	// GIVEN: / WHEN: a board created without options
	b := New(Options{})

	// THEN: ticks every second, events every 250 ms, state every 10 s
	if b.o.TickEvery != time.Second || b.o.EventsEvery != 250*time.Millisecond || b.o.StateEvery != 10*time.Second {
		t.Fatalf("got %+v", b.o)
	}
}
