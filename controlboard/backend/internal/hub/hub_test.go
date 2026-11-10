package hub

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/coder/websocket"

	"kafkalab/controlboard/backend/internal/api"
)

type received struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func serve(t *testing.T, h *Hub) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func next(t *testing.T, c *websocket.Conn) received {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var m received
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func waitClients(t *testing.T, h *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for h.Clients() != n {
		if time.Now().After(deadline) {
			t.Fatalf("clients %d, want %d", h.Clients(), n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func Test_shouldGreetANewClientAndForwardBroadcastsWhenItConnects(t *testing.T) {
	// GIVEN: a hub that greets with hello and state, one greeting that cannot be encoded
	set := metrics.NewSet()
	h := New(Options{Metrics: set, OnConnect: func() []api.Message {
		return []api.Message{{Type: api.MsgHello, Data: api.Hello{Version: 1}}, {Type: "broken", Data: math.NaN()}, {Type: api.MsgState, Data: api.State{Simulation: "running"}}}
	}})
	c := dial(t, serve(t, h))

	// WHEN: the client connects and the board broadcasts a tick
	hello, state := next(t, c), next(t, c)
	waitClients(t, h, 1)
	h.Broadcast(api.MsgTick, api.Tick{Rates: map[string]float64{"orders.created": 50}})
	tick := next(t, c)

	// THEN: it got hello, state and the tick; the unencodable greeting was skipped; the gauge counts it
	if hello.Type != api.MsgHello || state.Type != api.MsgState || tick.Type != api.MsgTick || !strings.Contains(string(tick.Data), `"orders.created":50`) {
		t.Fatalf("got %s %s %s", hello.Type, state.Type, tick.Type)
	}
	var sb strings.Builder
	set.WritePrometheus(&sb)
	if !strings.Contains(sb.String(), "lab_ws_clients 1") {
		t.Fatalf("metrics %s", sb.String())
	}
}

func Test_shouldAnswerWithPongWhenTheClientSendsPing(t *testing.T) {
	// GIVEN: a connected client
	h := New(Options{})
	c := dial(t, serve(t, h))

	// WHEN: it sends garbage, an unknown message and a ping
	ctx := context.Background()
	for _, m := range []string{"not json", `{"type":"hello"}`, `{"type":"ping"}`} {
		if err := c.Write(ctx, websocket.MessageText, []byte(m)); err != nil {
			t.Fatal(err)
		}
	}

	// THEN: only the ping is answered, with a pong
	if m := next(t, c); m.Type != api.MsgPong {
		t.Fatalf("got %s", m.Type)
	}
}

func Test_shouldDropOnlyTheSlowClientWhenItsQueueIsFull(t *testing.T) {
	// GIVEN: a slow client with room for one message and a fast client with plenty of room
	h := New(Options{})
	slow := &client{send: make(chan []byte, 1), done: make(chan struct{})}
	fast := &client{send: make(chan []byte, 10), done: make(chan struct{})}
	h.clients[slow], h.clients[fast] = struct{}{}, struct{}{}

	// WHEN: two messages are broadcast
	h.Broadcast(api.MsgTick, nil)
	h.Broadcast(api.MsgTick, nil)

	// THEN: the slow client is disconnected, the fast one keeps both messages
	select {
	case <-slow.done:
	default:
		t.Fatal("slow client not dropped")
	}
	if h.Clients() != 1 || len(fast.send) != 2 || slow.why != websocket.StatusTryAgainLater {
		t.Fatalf("clients %d, fast queue %d", h.Clients(), len(fast.send))
	}
}

func Test_shouldSkipTheBroadcastWhenTheMessageCannotBeEncoded(t *testing.T) {
	// GIVEN: a connected client
	h := New(Options{})
	cl := &client{send: make(chan []byte, 1), done: make(chan struct{})}
	h.clients[cl] = struct{}{}

	// WHEN: a KPI that is not a number (NaN) is broadcast
	h.Broadcast(api.MsgTick, math.NaN())

	// THEN: nothing is sent and the client stays connected
	if len(cl.send) != 0 || h.Clients() != 1 {
		t.Fatal("broken message must not be sent")
	}
}

func Test_shouldCloseEveryConnectionWhenTheHubShutsDown(t *testing.T) {
	// GIVEN: a connected client
	h := New(Options{})
	c := dial(t, serve(t, h))
	waitClients(t, h, 1)

	// WHEN: the board shuts down
	h.Close()

	// THEN: the client sees "going away"
	_, _, err := c.Read(context.Background())
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("got %v", err)
	}
	waitClients(t, h, 0)
}

func Test_shouldForgetTheClientWhenItDisconnects(t *testing.T) {
	// GIVEN: a connected client
	h := New(Options{})
	c := dial(t, serve(t, h))
	waitClients(t, h, 1)

	// WHEN: the browser tab is closed
	_ = c.Close(websocket.StatusNormalClosure, "")

	// THEN: the hub no longer counts it, and a later broadcast finds nobody
	waitClients(t, h, 0)
	h.Broadcast(api.MsgTick, nil)
}

func Test_shouldDisconnectTheClientWhenAWriteDoesNotFinishWithinTheTimeout(t *testing.T) {
	// GIVEN: a hub whose write timeout is so short that no write can finish
	h := New(Options{WriteTimeout: time.Nanosecond, OnConnect: func() []api.Message {
		return []api.Message{{Type: api.MsgHello, Data: api.Hello{Version: 1}}}
	}})

	// WHEN: a client connects and keeps reading while the board keeps broadcasting
	c := dial(t, serve(t, h))
	readErr := make(chan error, 1)
	go func() {
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				readErr <- err
				return
			}
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.Clients() != 0 && time.Now().Before(deadline) {
		h.Broadcast(api.MsgTick, api.Tick{})
		time.Sleep(time.Millisecond)
	}

	// THEN: a write misses its deadline, the hub forgets the client and the connection drops
	if h.Clients() != 0 {
		t.Fatal("slow client still registered")
	}
	if err := <-readErr; err == nil {
		t.Fatal("expected the connection to be dropped")
	}
}

func Test_shouldRefuseThePlainRequestWhenItIsNotAWebSocketUpgrade(t *testing.T) {
	// GIVEN: the hub endpoint
	h := New(Options{})
	rec := httptest.NewRecorder()

	// WHEN: a browser fetches it as a normal page
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/ws", nil))

	// THEN: the upgrade is refused and no client is registered
	if rec.Code < 400 || h.Clients() != 0 {
		t.Fatalf("got %d", rec.Code)
	}
}
