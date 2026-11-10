package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func Test_shouldAttachTraceIDsToLogLinesWhenTheContextCarriesASpan(t *testing.T) {
	// GIVEN: a payment log line written while a sampled order is being processed
	var buf bytes.Buffer
	log := NewLogger(&buf, "payment", "info").With("topic", "orders.created")
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "process")
	defer span.End()

	// WHEN: the line is logged with that context
	log.InfoContext(ctx, "authorised", "id", "o-1")

	// THEN: the JSON line carries the service, the trace id and the span id
	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatal(err)
	}
	if line["service"] != "payment" || line["trace_id"] != span.SpanContext().TraceID().String() || line["span_id"] == nil {
		t.Fatalf("unexpected log line %s", buf.String())
	}
}

func Test_shouldOmitTraceFieldsWhenNoSpanIsActive(t *testing.T) {
	// GIVEN: a startup log line outside of any order processing
	var buf bytes.Buffer
	log := NewLogger(&buf, "inventory", "debug")

	// WHEN: it is logged
	log.Debug("starting")

	// THEN: no trace fields are invented
	if bytes.Contains(buf.Bytes(), []byte("trace_id")) || !bytes.Contains(buf.Bytes(), []byte(`"level":"DEBUG"`)) {
		t.Fatalf("unexpected log line %s", buf.String())
	}
}

func Test_shouldHonourTheConfiguredLevelWhenLevelsAreSpelledDifferently(t *testing.T) {
	// GIVEN: the level names operators use in practice
	cases := map[string]string{"warning": "WARN", "warn": "WARN", "error": "ERROR", "nonsense": "INFO"}
	for level, want := range cases {
		var buf bytes.Buffer
		log := NewLogger(&buf, "shipping", level)

		// WHEN: messages of every level are logged
		log.Info("info")
		log.Warn("warn")
		log.Error("error")

		// THEN: the first line printed is of the configured minimum level
		var first map[string]any
		_ = json.NewDecoder(&buf).Decode(&first)
		if first["level"] != want {
			t.Fatalf("level %q: first line had level %v", level, first["level"])
		}
	}
}

func Test_shouldLimitRootTracesPerSecondWhenOrdersArriveFasterThanTheRate(t *testing.T) {
	// GIVEN: a generator allowed to start 2 traces per second and a burst of orders
	now := time.Unix(1000, 0)
	s := NewRateSampler(2, func() time.Time { return now })
	sampled := 0

	// WHEN: 1,000 orders arrive within one second
	for i := 0; i < 1000; i++ {
		now = now.Add(time.Millisecond)
		if s.ShouldSample(sdktrace.SamplingParameters{ParentContext: context.Background()}).Decision == sdktrace.RecordAndSample {
			sampled++
		}
	}

	// THEN: only two of them are traced
	if sampled != 2 {
		t.Fatalf("expected 2 sampled roots, got %d", sampled)
	}
}

func Test_shouldNeverStartTracesWhenTheRootRateIsZero(t *testing.T) {
	// GIVEN: a downstream service that must only continue traces
	s := NewRateSampler(0, time.Now)

	// WHEN: it would start a root span
	res := s.ShouldSample(sdktrace.SamplingParameters{ParentContext: context.Background()})

	// THEN: the span is dropped
	if res.Decision != sdktrace.Drop || s.Description() != "RateSampler" {
		t.Fatalf("expected drop, got %v", res.Decision)
	}
}

func Test_shouldInstallW3CPropagationWithoutExportWhenNoEndpointIsConfigured(t *testing.T) {
	// GIVEN: a local run without a trace backend
	// WHEN: tracing is initialised
	shutdown, err := InitTracing(context.Background(), TracingOptions{Service: "payment"})

	// THEN: trace context is still propagated so logs keep correlation, and shutdown is a no-op
	if err != nil || shutdown(context.Background()) != nil {
		t.Fatalf("init failed: %v", err)
	}
	if fields := strings.Join(otel.GetTextMapPropagator().Fields(), ","); !strings.Contains(fields, "traceparent") {
		t.Fatalf("unexpected propagator fields %v", fields)
	}
}

func Test_shouldExportSampledSpansWhenAnEndpointIsConfigured(t *testing.T) {
	// GIVEN: a trace backend address (spans are buffered, nothing is sent synchronously)
	shutdown, err := InitTracing(context.Background(), TracingOptions{Service: "order-generator", Endpoint: "http://127.0.0.1:1/v1/traces", RootRate: 1})
	if err != nil {
		t.Fatal(err)
	}

	// WHEN: the generator starts a root span
	_, span := otel.Tracer("t").Start(context.Background(), "order")
	span.End()

	// THEN: the span is sampled and carries the service resource
	if !span.SpanContext().IsSampled() {
		t.Fatal("expected the first root span to be sampled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = shutdown(ctx)
	otel.SetTracerProvider(noop.NewTracerProvider())
}

func Test_shouldFailStartupWhenTheTraceEndpointIsNotAURL(t *testing.T) {
	// GIVEN: a misconfigured trace endpoint
	// WHEN: tracing is initialised
	_, err := InitTracing(context.Background(), TracingOptions{Service: "payment", Endpoint: "http://[::1"})

	// THEN: the service refuses to start instead of silently losing traces
	if err == nil {
		t.Fatal("expected an error for a malformed endpoint")
	}
}

func Test_shouldKeepTraceCorrelationWhenAGroupedLoggerIsUsed(t *testing.T) {
	// GIVEN: a logger that groups order fields
	var buf bytes.Buffer
	log := NewLogger(&buf, "shipping", "info").WithGroup("order")
	ctx, span := sdktrace.NewTracerProvider().Tracer("t").Start(context.Background(), "ship")
	defer span.End()

	// WHEN: a line is logged with a traced context
	log.InfoContext(ctx, "shipped", "id", "o-7")

	// THEN: the trace id is still present (inside the group), so the line stays linked to its trace
	if !bytes.Contains(buf.Bytes(), []byte(span.SpanContext().TraceID().String())) {
		t.Fatalf("trace id missing in %s", buf.String())
	}
}
