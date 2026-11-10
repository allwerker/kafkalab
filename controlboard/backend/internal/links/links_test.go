package links

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kafkalab/controlboard/backend/internal/api"
)

func Test_shouldOfferThePlatformToolsWhenNoLinksFileIsConfigured(t *testing.T) {
	// GIVEN: no LINKS_FILE
	// WHEN: the links are loaded and listed before any probe
	list, err := Load("")
	links := NewChecker(list, http.DefaultClient).Links()

	// THEN: Grafana, AKHQ and the other platform tools are offered with unknown health
	if err != nil || len(links) != 8 || links[0].ID != "grafana" || links[0].URL != "http://grafana.localhost:8080" || links[0].Health != api.HealthUnknown {
		t.Fatalf("got %+v %v", links, err)
	}
}

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "links.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func Test_shouldReadTheLinksFromTheMountedFileWhenOneIsConfigured(t *testing.T) {
	// GIVEN: a ConfigMap with a single runbook link
	path := write(t, `[{"id":"runbooks","title":"Runbooks","url":"http://wiki/runbooks","description":"How to react","icon":"book"}]`)

	// WHEN: the links are loaded
	list, err := Load(path)

	// THEN: exactly that link is offered
	if err != nil || len(list) != 1 || list[0].Title != "Runbooks" || list[0].Icon != "book" {
		t.Fatalf("got %+v %v", list, err)
	}
}

func Test_shouldRefuseToStartWhenTheLinksFileIsMissingBrokenOrIncomplete(t *testing.T) {
	// GIVEN: a missing file, a file that is not JSON, and a link without url
	missing := filepath.Join(t.TempDir(), "nope.json")
	broken := write(t, "links: [grafana]")
	incomplete := write(t, `[{"id":"grafana"}]`)

	// WHEN: each is loaded
	_, e1 := Load(missing)
	_, e2 := Load(broken)
	_, e3 := Load(incomplete)

	// THEN: each fails with an error
	if e1 == nil || e2 == nil || e3 == nil {
		t.Fatalf("%v / %v / %v", e1, e2, e3)
	}
}

func Test_shouldReportEachToolsHealthWhenTheLinksAreProbed(t *testing.T) {
	// GIVEN: a healthy tool, a tool answering 500, one that is gone, one with a broken probe url and one without probe
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ok.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer failing.Close()
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	c := NewChecker([]Config{
		{ID: "ok", URL: "x", ProbeURL: ok.URL}, {ID: "failing", URL: "x", ProbeURL: failing.URL},
		{ID: "gone", URL: "x", ProbeURL: gone.URL}, {ID: "broken", URL: "x", ProbeURL: "http://[::1"}, {ID: "none", URL: "x"},
	}, http.DefaultClient)

	// WHEN: the board probes them
	c.Probe(context.Background())

	// THEN: up, degraded, down, unknown and unknown
	want := []string{api.HealthUp, api.HealthDegraded, api.HealthDown, api.HealthUnknown, api.HealthUnknown}
	for i, l := range c.Links() {
		if l.Health != want[i] {
			t.Fatalf("%s: %s, want %s", l.ID, l.Health, want[i])
		}
	}
}

func Test_shouldProbePeriodicallyWhenTheCheckerRuns(t *testing.T) {
	// GIVEN: a healthy tool
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer ok.Close()
	c := NewChecker([]Config{{ID: "ok", URL: "x", ProbeURL: ok.URL}}, ok.Client())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	// WHEN: the checker runs for a few intervals
	go func() { c.Run(ctx, 10*time.Millisecond); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for c.Links()[0].Health != api.HealthUp && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond)
	up := c.Links()[0].Health
	cancel()
	<-done

	// THEN: the tool is up
	if up != api.HealthUp {
		t.Fatal("not probed")
	}
}
