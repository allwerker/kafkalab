package board

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"

	"kafkalab/controlboard/backend/internal/api"
)

const maxBody = 1 << 20

// Register adds every REST endpoint and the WebSocket to mux (Go 1.22 patterns).
func (b *Board) Register(mux *http.ServeMux) {
	h := func(pattern string, fn func(*http.Request) (any, error)) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) { b.respond(w, r, fn) })
	}
	exp := func() api.Experiments { return b.o.Exp }

	h("GET /api/state", func(*http.Request) (any, error) { return b.State(), nil })
	h("GET /api/scenarios", func(*http.Request) (any, error) { return exp().Scenarios(), nil })
	h("POST /api/experiments", func(r *http.Request) (any, error) {
		var req api.StartRequest
		if err := decode(r, &req); err != nil {
			return nil, err
		}
		return exp().Start(r.Context(), req)
	})
	h("DELETE /api/experiments/{id}", func(r *http.Request) (any, error) { return exp().Stop(r.Context(), r.PathValue("id")) })
	h("POST /api/chaos/arm", func(r *http.Request) (any, error) {
		var req api.ArmRequest
		if err := decode(r, &req); err != nil {
			return nil, err
		}
		if err := exp().Arm(r.Context(), req.Armed); err != nil {
			return nil, err
		}
		return api.ArmRequest{Armed: exp().Armed()}, nil
	})
	h("POST /api/panic", func(r *http.Request) (any, error) {
		n, err := exp().Panic(r.Context())
		return api.PanicResponse{Reverted: n}, err
	})
	h("POST /api/simulation", func(r *http.Request) (any, error) {
		var req api.SimulationRequest
		if err := decode(r, &req); err != nil {
			return nil, err
		}
		s, err := exp().Simulation(r.Context(), req.Action)
		return api.SimulationResponse{Simulation: s}, err
	})
	h("PUT /api/services/{name}/knobs", func(r *http.Request) (any, error) {
		var knobs api.Values
		if err := decode(r, &knobs); err != nil {
			return nil, err
		}
		return b.service(r, exp().SetKnobs(r.Context(), r.PathValue("name"), knobs))
	})
	h("DELETE /api/services/{name}/knobs", func(r *http.Request) (any, error) {
		return b.service(r, exp().ResetKnobs(r.Context(), r.PathValue("name")))
	})
	h("POST /api/services/{name}/scale", func(r *http.Request) (any, error) {
		var req api.ScaleRequest
		if err := decode(r, &req); err != nil {
			return nil, err
		}
		if req.Replicas < 0 || req.Replicas > 6 {
			return nil, fmt.Errorf("%w: replicas must be between 0 and 6", api.ErrInvalid)
		}
		return b.service(r, exp().Scale(r.Context(), r.PathValue("name"), req.Replicas))
	})
	h("GET /api/timeline", func(r *http.Request) (any, error) {
		limit := 200
		if s := r.URL.Query().Get("limit"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("%w: limit must be a positive number", api.ErrInvalid)
			}
			limit = n
		}
		return b.Timeline(limit), nil
	})
	h("GET /api/alerts", func(*http.Request) (any, error) { return b.o.Alerts.All(), nil })
	h("POST /api/alerts/test", func(r *http.Request) (any, error) { return nil, b.o.FireTest(r.Context()) })
	h("POST /api/alerts/webhook", func(r *http.Request) (any, error) {
		body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBody))
		if err != nil {
			return nil, fmt.Errorf("%w: %v", api.ErrInvalid, err)
		}
		list, err := b.o.ParseWebhook(body)
		if err != nil {
			return nil, err
		}
		return nil, b.o.Alerts.Receive(r.Context(), list)
	})
	h("GET /api/links", func(*http.Request) (any, error) { return b.o.Links.Links(), nil })
	h("GET /api/tuning", func(*http.Request) (any, error) { return exp().Tuning(), nil })
	h("PUT /api/tuning/{service}", func(r *http.Request) (any, error) {
		var values api.Values
		if err := decode(r, &values); err != nil {
			return nil, err
		}
		return exp().SetTuning(r.Context(), r.PathValue("service"), values)
	})
	h("POST /api/drills", func(r *http.Request) (any, error) {
		var req api.DrillStartRequest
		if err := decode(r, &req); err != nil {
			return nil, err
		}
		return exp().Drills().Start(r.Context(), req.Difficulty, req.Player)
	})
	h("POST /api/drills/{id}/hint", func(r *http.Request) (any, error) { return exp().Drills().Hint(r.Context(), r.PathValue("id")) })
	h("POST /api/drills/{id}/guess", func(r *http.Request) (any, error) {
		var req api.GuessRequest
		if err := decode(r, &req); err != nil {
			return nil, err
		}
		return exp().Drills().Guess(r.Context(), r.PathValue("id"), req.Scenario)
	})
	h("POST /api/drills/{id}/reveal", func(r *http.Request) (any, error) { return exp().Drills().Reveal(r.Context(), r.PathValue("id")) })
	h("GET /api/drills/leaderboard", func(*http.Request) (any, error) { return exp().Drills().Leaderboard(), nil })
	h("GET /api/orders/{orderId}", func(r *http.Request) (any, error) {
		t, ok := b.o.Feed.Order(r.PathValue("orderId"))
		if !ok {
			return nil, fmt.Errorf("%w: order %q is not among the recent orders", api.ErrNotFound, r.PathValue("orderId"))
		}
		return t, nil
	})
	mux.Handle("GET /api/ws", b.hub)
	// Anything else under /api is a JSON 404, never the SPA.
	h("/api/", func(r *http.Request) (any, error) {
		return nil, fmt.Errorf("%w: %s %s", api.ErrNotFound, r.Method, r.URL.Path)
	})
}

// service answers a knob/scale call with the service's merged state.
func (b *Board) service(r *http.Request, err error) (any, error) {
	if err != nil {
		return nil, err
	}
	name := r.PathValue("name")
	list := b.services(b.o.Observed.Snapshot())
	i := slices.IndexFunc(list, func(s api.ServiceState) bool { return s.Name == name })
	if i < 0 {
		return nil, fmt.Errorf("%w: %q", api.ErrUnknownService, name)
	}
	return list[i], nil
}

func decode(r *http.Request, v any) error {
	if err := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxBody)).Decode(v); err != nil {
		return fmt.Errorf("%w: body: %v", api.ErrInvalid, err)
	}
	return nil
}

var errorCodes = []struct {
	err    error
	status int
	code   string
}{
	{api.ErrChaosDisarmed, http.StatusConflict, "chaos_disarmed"},
	{api.ErrBrokerFaultActive, http.StatusConflict, "broker_fault_active"},
	{api.ErrConfirmationRequired, http.StatusBadRequest, "confirmation_required"},
	{api.ErrUnknownScenario, http.StatusNotFound, "unknown_scenario"},
	{api.ErrNotFound, http.StatusNotFound, "not_found"},
	{api.ErrUnknownService, http.StatusNotFound, "unknown_service"},
	{api.ErrInvalid, http.StatusBadRequest, "invalid_request"},
	{api.ErrConflict, http.StatusConflict, "conflict"},
	{api.ErrDrillActive, http.StatusConflict, "drill_active"},
	{api.ErrDrillFinished, http.StatusConflict, "drill_finished"},
	{api.ErrUnavailable, http.StatusServiceUnavailable, "unavailable"},
}

func (b *Board) respond(w http.ResponseWriter, r *http.Request, fn func(*http.Request) (any, error)) {
	v, err := fn(r)
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		status, code := http.StatusInternalServerError, "internal"
		for _, e := range errorCodes {
			if errors.Is(err, e.err) {
				status, code = e.status, e.code
				break
			}
		}
		if status == http.StatusInternalServerError {
			b.o.Log.ErrorContext(r.Context(), "request failed", "method", r.Method, "path", r.URL.Path, "error", err)
		}
		writeJSON(w, status, api.Error{Error: code, Message: err.Error()})
		return
	}
	if v == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
