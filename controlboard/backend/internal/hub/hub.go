// Package hub fans board messages out to WebSocket clients. Every client has a bounded queue;
// a client that cannot keep up is disconnected instead of slowing down the others.
package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/coder/websocket"

	"kafkalab/controlboard/backend/internal/api"
)

// DefaultQueue is the number of messages buffered per client.
const DefaultQueue = 256

// Options configures New.
type Options struct {
	// OnConnect returns the messages every new client gets first (hello, state).
	OnConnect func() []api.Message
	Queue     int
	// WriteTimeout bounds one write; a client slower than that is disconnected.
	WriteTimeout time.Duration
	// Origins are allowed cross-origin hosts (e.g. the Vite dev server); same origin always works.
	Origins []string
	Metrics *metrics.Set
	Log     *slog.Logger
	Now     func() time.Time
}

// Hub holds the connected clients.
type Hub struct {
	o       Options
	mu      sync.Mutex
	clients map[*client]struct{}
}

type client struct {
	send chan []byte
	done chan struct{}
	once sync.Once
	why  websocket.StatusCode
}

func (c *client) close(why websocket.StatusCode) {
	c.once.Do(func() {
		c.why = why
		close(c.done)
	})
}

// New returns a hub without clients and registers the lab_ws_clients gauge.
func New(o Options) *Hub {
	if o.Queue <= 0 {
		o.Queue = DefaultQueue
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 5 * time.Second
	}
	if o.OnConnect == nil {
		o.OnConnect = func() []api.Message { return nil }
	}
	if o.Metrics == nil {
		o.Metrics = metrics.NewSet()
	}
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	h := &Hub{o: o, clients: map[*client]struct{}{}}
	o.Metrics.GetOrCreateGauge("lab_ws_clients", func() float64 { return float64(h.Clients()) })
	return h
}

// Clients returns the number of connected clients.
func (h *Hub) Clients() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Message builds an envelope stamped with the current time.
func (h *Hub) Message(typ string, data any) api.Message {
	return api.Message{Type: typ, Ts: h.o.Now().UTC(), Data: data}
}

// Broadcast sends a message to every client without blocking; full queues get dropped.
func (h *Hub) Broadcast(typ string, data any) {
	b, ok := h.encode(h.Message(typ, data))
	if !ok {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		select {
		case c.send <- b:
		default:
			h.o.Log.Warn("dropping slow websocket client", "queue", cap(c.send))
			delete(h.clients, c)
			c.close(websocket.StatusTryAgainLater)
		}
	}
}

// Close disconnects every client (shutdown).
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		delete(h.clients, c)
		c.close(websocket.StatusGoingAway)
	}
}

func (h *Hub) encode(m api.Message) ([]byte, bool) {
	b, err := json.Marshal(m)
	if err != nil {
		h.o.Log.Error("cannot encode websocket message", "type", m.Type, "error", err)
		return nil, false
	}
	return b, true
}

// ServeHTTP upgrades the request and serves the client until it leaves or is dropped.
func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: h.o.Origins})
	if err != nil {
		return // Accept has answered the request already
	}
	defer conn.CloseNow()
	first := h.o.OnConnect()
	c := &client{send: make(chan []byte, h.o.Queue+len(first)), done: make(chan struct{})}
	for _, m := range first {
		if b, ok := h.encode(m); ok {
			c.send <- b
		}
	}
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.clients, c)
		h.mu.Unlock()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.read(ctx, conn, c)
	for {
		select {
		case b := <-c.send:
			wctx, wcancel := context.WithTimeout(ctx, h.o.WriteTimeout)
			err := conn.Write(wctx, websocket.MessageText, b)
			wcancel()
			if err != nil {
				return
			}
		case <-c.done:
			_ = conn.Close(c.why, "bye")
			return
		}
	}
}

// read answers pings and ends the client when the connection breaks.
func (h *Hub) read(ctx context.Context, conn *websocket.Conn, c *client) {
	for {
		_, b, err := conn.Read(ctx)
		if err != nil {
			c.close(websocket.StatusNormalClosure)
			return
		}
		var in struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(b, &in) != nil || in.Type != "ping" {
			continue
		}
		// Writes are safe concurrently with the writer loop; a failing write ends the reader next round.
		pong, _ := h.encode(api.Message{Type: api.MsgPong, Ts: h.o.Now().UTC()})
		wctx, wcancel := context.WithTimeout(ctx, h.o.WriteTimeout)
		_ = conn.Write(wctx, websocket.MessageText, pong)
		wcancel()
	}
}
