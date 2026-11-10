package obs

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// RateSampler samples at most `rate` root traces per second. Under 1,000 orders/s an unsampled
// pipeline would produce ~20,000 spans/s and overwhelm a small trace store; a few complete
// traces per second are enough to follow orders end to end.
type RateSampler struct {
	interval time.Duration
	now      func() time.Time
	mu       sync.Mutex
	next     time.Time
}

// NewRateSampler returns a sampler allowing `rate` root traces per second (0 = none).
func NewRateSampler(rate float64, now func() time.Time) *RateSampler {
	s := &RateSampler{now: now}
	if rate > 0 {
		s.interval = time.Duration(float64(time.Second) / rate)
	}
	return s
}

// ShouldSample implements sdktrace.Sampler.
func (s *RateSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	ts := trace.SpanContextFromContext(p.ParentContext).TraceState()
	if s.interval == 0 {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: ts}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if now := s.now(); !now.Before(s.next) {
		s.next = now.Add(s.interval)
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample, Tracestate: ts}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.Drop, Tracestate: ts}
}

// Description implements sdktrace.Sampler.
func (s *RateSampler) Description() string { return "RateSampler" }

// TracingOptions configures InitTracing.
type TracingOptions struct {
	Service  string
	Endpoint string  // OTLP/HTTP traces URL; empty disables export
	RootRate float64 // root traces per second this service may start (0 = only continue traces)
}

// InitTracing installs the global tracer provider and W3C propagator. Child spans follow the
// parent's sampling decision, so a sampled order is traced through every service.
func InitTracing(_ context.Context, o TracingOptions) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if o.Endpoint == "" {
		return func(context.Context) error { return nil }, nil
	}
	if _, err := url.ParseRequestURI(o.Endpoint); err != nil {
		return nil, fmt.Errorf("trace endpoint: %w", err)
	}
	// The HTTP client connects lazily, so an unreachable backend never blocks service startup.
	exp := otlptrace.NewUnstarted(otlptracehttp.NewClient(otlptracehttp.WithEndpointURL(o.Endpoint)))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(4096), sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithSampler(sdktrace.ParentBased(NewRateSampler(o.RootRate, time.Now))),
		sdktrace.WithResource(serviceResource(o.Service)),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

func serviceAttrs(service string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("service.name", service),
		attribute.String("service.namespace", "kafka-lab"),
	}
}
