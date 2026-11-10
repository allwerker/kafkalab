package alerts

import (
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

	"github.com/VictoriaMetrics/metrics"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/grafana"
	"kafkalab/pkg/kit/pg"
	"kafkalab/pkg/kit/testkit"
)

var (
	ctx = context.Background()
	t0  = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	gl  = grafana.Links{Base: "http://grafana.localhost:8080"}
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type recorder struct {
	mu       sync.Mutex
	changes  []api.Alert
	timeline []api.TimelineEntry
}

func (r *recorder) change(a api.Alert) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.changes = append(r.changes, a)
}
func (r *recorder) entry(e api.TimelineEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.timeline = append(r.timeline, e)
}

func manager(store Store) (*Manager, *recorder, *clock, *metrics.Set) {
	r, c, set := &recorder{}, &clock{t: t0}, metrics.NewSet()
	return New(Options{Store: store, Grafana: gl, OnChange: r.change, OnTimeline: r.entry, Metrics: set, Now: c.now}), r, c, set
}

const lagWebhook = `{"version":"4","status":"firing","receiver":"controlboard","groupKey":"{}:{}",
 "alerts":[{"status":"firing","labels":{"alertname":"KafkaConsumerLagHigh","severity":"critical","component":"consumer","dashboard":"kafka-lag","group":"payment"},
  "annotations":{"summary":"payment lags behind","description":"lag above 10k for 2m","runbook_url":"http://runbooks/lag"},
  "startsAt":"2026-11-10T11:58:00Z","endsAt":"0001-01-01T00:00:00Z","generatorURL":"http://vmalert/rule","fingerprint":"abc123"}]}`

func parse(t *testing.T, body string) []api.Alert {
	t.Helper()
	list, err := ParseWebhook(strings.NewReader(body), gl)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

func Test_shouldTurnTheAlertmanagerNotificationIntoABoardAlertWhenALagAlertFires(t *testing.T) {
	// GIVEN: Alertmanager notifies that payment's consumer lag is critical
	// WHEN: the webhook body is parsed
	a := parse(t, lagWebhook)[0]

	// THEN: name, severity, component, texts and links come from labels and annotations
	if a.Name != "KafkaConsumerLagHigh" || a.Status != api.AlertFiring || a.Severity != "critical" || a.Component != "consumer" || a.Fingerprint != "abc123" {
		t.Fatalf("got %+v", a)
	}
	if a.Summary != "payment lags behind" || a.Description != "lag above 10k for 2m" || a.RunbookURL != "http://runbooks/lag" ||
		a.DashboardURL != "http://grafana.localhost:8080/d/kafka-lag" || a.GeneratorURL != "http://vmalert/rule" || a.EndsAt != nil || a.Labels["group"] != "payment" {
		t.Fatalf("got %+v", a)
	}
}

func Test_shouldMarkTheAlertResolvedWithItsEndWhenAlertmanagerSendsTheResolution(t *testing.T) {
	// GIVEN: a resolution with an end time, one without, and an alert with an unknown severity and no labels
	body := `{"alerts":[
	 {"status":"resolved","labels":{"alertname":"A","severity":"info"},"startsAt":"2026-11-10T11:00:00Z","endsAt":"2026-11-10T11:05:00Z","fingerprint":"a"},
	 {"status":"resolved","labels":{"alertname":"B"},"startsAt":"2026-11-10T11:00:00Z","fingerprint":"b"},
	 {"status":"firing","startsAt":"2026-11-10T11:00:00Z","fingerprint":"c","labels":null}]}`

	// WHEN: it is parsed
	list := parse(t, body)

	// THEN: the ends are taken over (or the start when missing) and unknown severities count as warning
	if list[0].Status != api.AlertResolved || !list[0].EndsAt.Equal(t0.Add(-55*time.Minute)) || list[0].Severity != "info" {
		t.Fatalf("a %+v", list[0])
	}
	if !list[1].EndsAt.Equal(list[1].StartsAt) || list[1].Severity != "warning" || list[1].DashboardURL != "" {
		t.Fatalf("b %+v", list[1])
	}
	if list[2].Labels == nil || list[2].Name != "" {
		t.Fatalf("c %+v", list[2])
	}
}

func Test_shouldRejectTheNotificationWhenItIsNotAValidWebhook(t *testing.T) {
	// GIVEN: a broken body and an alert without fingerprint
	// WHEN: both are parsed
	_, broken := ParseWebhook(strings.NewReader("{"), gl)
	_, nofp := ParseWebhook(strings.NewReader(`{"alerts":[{"status":"firing","startsAt":"2026-11-10T11:00:00Z"}]}`), gl)

	// THEN: both are invalid requests
	if !errors.Is(broken, api.ErrInvalid) || !errors.Is(nofp, api.ErrInvalid) {
		t.Fatalf("%v / %v", broken, nofp)
	}
}

func Test_shouldAnnounceAFiringAlertOnceWhenAlertmanagerRepeatsIt(t *testing.T) {
	// GIVEN: the board without alerts
	m, r, _, set := manager(&Memory{})

	// WHEN: the lag alert fires, is repeated an hour later and finally resolves
	_ = m.Receive(ctx, parse(t, lagWebhook))
	_ = m.Receive(ctx, parse(t, lagWebhook))
	resolved := strings.ReplaceAll(strings.ReplaceAll(lagWebhook, `"status":"firing"`, `"status":"resolved"`), "0001-01-01T00:00:00Z", "2026-11-10T12:00:00Z")
	_ = m.Receive(ctx, parse(t, resolved))

	// THEN: firing and resolution were announced once each, with timeline entries
	if len(r.changes) != 2 || r.changes[0].Status != api.AlertFiring || r.changes[1].Status != api.AlertResolved {
		t.Fatalf("changes %+v", r.changes)
	}
	if r.timeline[0].Title != "Alert firing: KafkaConsumerLagHigh" || r.timeline[0].Severity != "critical" || r.timeline[1].Severity != api.SeveritySuccess || r.timeline[1].Kind != api.KindAlert {
		t.Fatalf("timeline %+v", r.timeline)
	}
	// AND: every received alert is counted by status
	var sb strings.Builder
	set.WritePrometheus(&sb)
	if !strings.Contains(sb.String(), `lab_alerts_received_total{status="firing"} 2`) || !strings.Contains(sb.String(), `lab_alerts_received_total{status="resolved"} 1`) {
		t.Fatalf("metrics %s", sb.String())
	}
}

func Test_shouldFailTheNotificationWhenTheHistoryCannotBeStored(t *testing.T) {
	// GIVEN: the database is down
	m, r, _, _ := manager(&Memory{Err: errors.New("connection refused")})

	// WHEN: an alert arrives
	err := m.Receive(ctx, parse(t, lagWebhook))

	// THEN: Alertmanager gets an error (and retries), nothing is announced
	if err == nil || len(r.changes) != 0 {
		t.Fatalf("err %v changes %d", err, len(r.changes))
	}
}

func pending(id, name string, labels map[string]string) api.Alert {
	labels["alertname"] = name
	return api.Alert{Fingerprint: "vmalert-" + id, Name: name, Status: api.AlertPending, StartsAt: t0, Labels: labels}
}

func Test_shouldReplaceThePendingAlertWhenTheSameRuleStartsFiring(t *testing.T) {
	// GIVEN: vmalert reports the lag alert for payment as pending, and another for shipping
	m, r, _, _ := manager(&Memory{})
	m.SetPending([]api.Alert{
		pending("1", "KafkaConsumerLagHigh", map[string]string{"group": "payment", "alertgroup": "kafka"}),
		pending("2", "KafkaConsumerLagHigh", map[string]string{"group": "shipping"}),
	})

	// WHEN: the payment one fires via the webhook and vmalert still lists both as pending
	_ = m.Receive(ctx, parse(t, lagWebhook))
	m.SetPending([]api.Alert{
		pending("1", "KafkaConsumerLagHigh", map[string]string{"group": "payment", "alertgroup": "kafka"}),
		pending("2", "KafkaConsumerLagHigh", map[string]string{"group": "shipping"}),
	})
	// AND: vmalert drops the payment one from its pending list
	m.SetPending([]api.Alert{pending("2", "KafkaConsumerLagHigh", map[string]string{"group": "shipping"})})

	// THEN: the board shows payment firing and shipping pending, without a fake resolution
	active := m.Active()
	if len(active) != 2 || active[0].Status != api.AlertFiring || active[1].Fingerprint != "vmalert-2" {
		t.Fatalf("active %+v", active)
	}
	if len(r.changes) != 3 || r.timeline[0].Title != "Alert pending: KafkaConsumerLagHigh" {
		t.Fatalf("changes %+v", r.changes)
	}
}

func Test_shouldAnnounceAResolutionWhenAPendingAlertVanishesWithoutFiring(t *testing.T) {
	// GIVEN: a pending alert for a short lag spike, next to an unrelated resolved alert
	m, r, _, _ := manager(&Memory{})
	_ = m.Receive(ctx, parse(t, `{"alerts":[{"status":"resolved","labels":{"alertname":"Other"},"startsAt":"2026-11-10T11:00:00Z","fingerprint":"o"}]}`))
	m.SetPending([]api.Alert{pending("1", "KafkaConsumerLagHigh", map[string]string{"group": "payment"})})

	// WHEN: the spike is over before the alert fires
	m.SetPending(nil)

	// THEN: the board announces it as resolved and no longer lists it
	if len(r.changes) != 3 || r.changes[2].Status != api.AlertResolved || r.changes[2].EndsAt == nil || len(m.All()) != 1 {
		t.Fatalf("changes %+v", r.changes)
	}
}

func Test_shouldHideAResolvedAlertFromTheStateButKeepItInTheHistoryWhenTwentyMinutesHavePassed(t *testing.T) {
	// GIVEN: an alert resolved at 12:00
	m, _, c, _ := manager(&Memory{})
	resolved := strings.ReplaceAll(strings.ReplaceAll(lagWebhook, `"status":"firing"`, `"status":"resolved"`), "0001-01-01T00:00:00Z", "2026-11-10T12:00:00Z")
	_ = m.Receive(ctx, parse(t, resolved))

	// WHEN: 20 minutes pass
	c.add(20 * time.Minute)

	// THEN: the state no longer shows it, the history still does
	if len(m.Active()) != 0 || len(m.All()) != 1 {
		t.Fatalf("active %d all %d", len(m.Active()), len(m.All()))
	}
}

func Test_shouldForgetResolvedAlertsWhenTheyAreOlderThan24Hours(t *testing.T) {
	// GIVEN: a resolved alert, a resolved alert without end time, and a firing alert, stored a day ago
	store := &Memory{}
	m, _, c, _ := manager(store)
	body := `{"alerts":[
	 {"status":"resolved","labels":{"alertname":"Old"},"startsAt":"2026-11-10T11:00:00Z","endsAt":"2026-11-10T11:05:00Z","fingerprint":"a"},
	 {"status":"firing","labels":{"alertname":"Still"},"startsAt":"2026-11-10T11:00:00Z","fingerprint":"b"}]}`
	_ = m.Receive(ctx, parse(t, body))
	m.alerts["x"] = api.Alert{Fingerprint: "x", Status: api.AlertResolved, StartsAt: t0.Add(-time.Hour)}

	// WHEN: 25 hours pass and the history is pruned
	c.add(25 * time.Hour)
	err := m.Prune(ctx)

	// THEN: only the still firing alert is kept in memory; the store dropped the stale rows
	all := m.All()
	if err != nil || len(all) != 1 || all[0].Name != "Still" {
		t.Fatalf("all %+v err %v", all, err)
	}
	if rows, _ := store.Load(ctx, t0.Add(-48*time.Hour)); len(rows) != 0 {
		t.Fatalf("store kept %d rows", len(rows))
	}
}

func Test_shouldRestoreTheHistoryWhenTheBoardRestarts(t *testing.T) {
	// GIVEN: a board that received an alert, then restarts with the same store
	store := &Memory{}
	first, _, _, _ := manager(store)
	_ = first.Receive(ctx, parse(t, lagWebhook))
	second, _, _, _ := manager(store)

	// WHEN: the new board loads its history; and a board whose database is down tries the same
	err := second.Load(ctx)
	broken, _, _, _ := manager(&Memory{Err: errors.New("down")})

	// THEN: the alert is back; the broken board reports the failure
	if err != nil || len(second.Active()) != 1 || broken.Load(ctx) == nil {
		t.Fatalf("err %v active %d", err, len(second.Active()))
	}
}

func Test_shouldPruneOnScheduleAndLogFailuresWhenTheManagerRuns(t *testing.T) {
	// GIVEN: a manager whose database fails
	m := New(Options{Store: &Memory{Err: errors.New("down")}})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// WHEN: it runs for a few prune intervals
	m.Run(ctx, 10*time.Millisecond)

	// THEN: it stops with the context despite the failures
	if ctx.Err() == nil {
		t.Fatal("run returned early")
	}
}

const vmalertBody = `{"status":"success","data":{"alerts":[
 {"id":"111","name":"KafkaConsumerLagHigh","state":"pending","labels":{"alertname":"KafkaConsumerLagHigh","severity":"warning","dashboard":"kafka-lag"},
  "annotations":{"summary":"lag grows"},"activeAt":"2026-11-10T11:59:00Z"},
 {"id":"222","name":"NoLabels","state":"pending","labels":{},"annotations":{},"activeAt":"2026-11-10T11:59:30Z"},
 {"id":"333","name":"Firing","state":"firing","labels":{"alertname":"Firing"},"activeAt":"2026-11-10T11:00:00Z"}]}}`

func Test_shouldPickOnlyPendingAlertsWhenVMAlertIsPolled(t *testing.T) {
	// GIVEN: vmalert with two pending alerts and one firing
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, vmalertBody) }))
	defer srv.Close()

	// WHEN: the board polls it
	list, err := VMAlert{Base: srv.URL, HTTP: srv.Client(), Grafana: gl}.Pending(ctx)

	// THEN: both pending alerts are returned with vmalert ids, the firing one is left to the webhook
	if err != nil || len(list) != 2 {
		t.Fatalf("got %+v %v", list, err)
	}
	if list[0].Fingerprint != "vmalert-111" || list[0].DashboardURL != "http://grafana.localhost:8080/d/kafka-lag" || !list[0].StartsAt.Equal(t0.Add(-time.Minute)) || list[1].Name != "NoLabels" {
		t.Fatalf("got %+v", list)
	}
	if !strings.HasSuffix(list[0].GeneratorURL, "/vmalert/alert?alert_id=111") {
		t.Fatalf("generator %s", list[0].GeneratorURL)
	}
}

func Test_shouldReportAnErrorWhenVMAlertIsUnreachableOrAnswersGarbage(t *testing.T) {
	// GIVEN: an invalid address, a closed port and an HTML error page
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "<html>", http.StatusBadGateway) }))
	defer garbage.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	// WHEN: each is polled
	_, invalid := VMAlert{Base: "http://[::1", HTTP: http.DefaultClient}.Pending(ctx)
	_, refused := VMAlert{Base: closed.URL, HTTP: http.DefaultClient}.Pending(ctx)
	_, html := VMAlert{Base: garbage.URL, HTTP: http.DefaultClient}.Pending(ctx)

	// THEN: every poll fails
	if invalid == nil || refused == nil || html == nil {
		t.Fatalf("%v / %v / %v", invalid, refused, html)
	}
}

func Test_shouldMergePendingAlertsPeriodicallyWhenPollingRuns(t *testing.T) {
	// GIVEN: vmalert that fails on the first poll and answers afterwards
	var mu sync.Mutex
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, vmalertBody)
	}))
	defer srv.Close()
	m, _, _, _ := manager(&Memory{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	// WHEN: polling runs until the pending alerts show up
	go func() {
		m.PollPending(ctx, VMAlert{Base: srv.URL, HTTP: srv.Client()}, 10*time.Millisecond)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Active()) != 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done

	// THEN: both pending alerts are on the board
	if len(m.Active()) != 2 {
		t.Fatalf("got %d", len(m.Active()))
	}
}

func Test_shouldPostLabTestAlertToAlertmanagerWhenATestAlertIsRequested(t *testing.T) {
	// GIVEN: an Alertmanager that records what it receives
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v2/alerts" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
	}))
	defer srv.Close()

	// WHEN: the board fires the test alert
	err := FireTest(ctx, srv.Client(), srv.URL+"/", "http://board.localhost:8080", t0)

	// THEN: LabTestAlert with severity info arrives, ending two minutes later
	if err != nil || len(got) != 1 {
		t.Fatalf("err %v got %+v", err, got)
	}
	labels := got[0]["labels"].(map[string]any)
	if labels["alertname"] != TestAlertName || labels["severity"] != "info" || got[0]["endsAt"] != "2026-11-10T12:02:00Z" {
		t.Fatalf("got %+v", got[0])
	}
}

func Test_shouldReportAlertmanagerUnavailableWhenTheTestAlertCannotBeDelivered(t *testing.T) {
	// GIVEN: an Alertmanager that rejects alerts, a closed port and an invalid address
	rejecting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "maintenance", http.StatusServiceUnavailable)
	}))
	defer rejecting.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()

	// WHEN: the test alert is fired at each
	rejected := FireTest(ctx, http.DefaultClient, rejecting.URL, "", t0)
	refused := FireTest(ctx, http.DefaultClient, closed.URL, "", t0)
	invalid := FireTest(ctx, http.DefaultClient, "http://[::1", "", t0)

	// THEN: the first two are "unavailable" with the reason, the invalid address fails too
	if !errors.Is(rejected, api.ErrUnavailable) || !strings.Contains(rejected.Error(), "maintenance") || !errors.Is(refused, api.ErrUnavailable) || invalid == nil {
		t.Fatalf("%v / %v / %v", rejected, refused, invalid)
	}
}

func Test_shouldUseDefaultsWhenTheManagerIsCreatedWithoutCallbacks(t *testing.T) {
	// GIVEN: a manager with only a store
	m := New(Options{Store: &Memory{}})

	// WHEN: an alert arrives
	err := m.Receive(ctx, parse(t, lagWebhook))

	// THEN: it is listed although nobody listens for changes
	if err != nil || len(m.All()) != 1 {
		t.Fatalf("err %v", err)
	}
}

func openPG(t *testing.T) PG {
	t.Helper()
	db, err := pg.Open(ctx, pg.Options{URL: testkit.PostgresDSN(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err := db.Migrate(ctx, Migrations()); err != nil {
		t.Fatal(err)
	}
	return PG{DB: db}
}

func Test_shouldKeepOneRowPerAlertOccurrenceWhenAlertmanagerUpdatesItInPostgres(t *testing.T) {
	// GIVEN: the PostgreSQL history and a firing alert
	store := openPG(t)
	a := parse(t, lagWebhook)[0]

	// WHEN: it is stored firing, then resolved
	_ = store.Upsert(ctx, a, t0)
	end := t0
	a.Status, a.EndsAt = api.AlertResolved, &end
	err := store.Upsert(ctx, a, t0.Add(time.Minute))
	rows, lerr := store.Load(ctx, t0.Add(-time.Hour))

	// THEN: one resolved row with all its fields remains
	if err != nil || lerr != nil || len(rows) != 1 || rows[0].Status != api.AlertResolved || rows[0].DashboardURL != a.DashboardURL || !rows[0].EndsAt.Equal(t0) {
		t.Fatalf("rows %+v err %v %v", rows, err, lerr)
	}
}

func Test_shouldDeleteRowsUpdatedBeforeTheCutoffWhenPostgresIsPruned(t *testing.T) {
	// GIVEN: an alert last updated two days ago and one updated now
	store := openPG(t)
	old, fresh := parse(t, lagWebhook)[0], parse(t, lagWebhook)[0]
	fresh.Fingerprint = "fresh"
	_ = store.Upsert(ctx, old, t0.Add(-48*time.Hour))
	_ = store.Upsert(ctx, fresh, t0)

	// WHEN: everything older than 24 h is pruned
	err := store.Prune(ctx, t0.Add(-24*time.Hour))
	rows, _ := store.Load(ctx, time.Time{})

	// THEN: only the fresh one remains
	if err != nil || len(rows) != 1 || rows[0].Fingerprint != "fresh" {
		t.Fatalf("rows %+v err %v", rows, err)
	}
}

func Test_shouldFailLoadingWhenAStoredRowIsCorruptOrTheDatabaseIsGone(t *testing.T) {
	// GIVEN: a history with a row that is not an alert document
	store := openPG(t)
	if _, err := store.DB.Pool.Exec(ctx, `INSERT INTO alerts VALUES ('x', now(), 'firing', NULL, now(), '"not an alert"')`); err != nil {
		t.Fatal(err)
	}

	// WHEN: the board loads it, and loads again after the database connection is closed
	_, corrupt := store.Load(ctx, time.Time{})
	store.DB.Close()
	_, gone := store.Load(ctx, time.Time{})

	// THEN: both loads fail
	if corrupt == nil || gone == nil {
		t.Fatalf("%v / %v", corrupt, gone)
	}
}
