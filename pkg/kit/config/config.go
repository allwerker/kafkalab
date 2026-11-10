// Package config reads service configuration from environment variables (docs/contracts/platform.md).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the configuration every lab service shares.
type Config struct {
	ServiceName       string
	KafkaBrokers      []string
	SchemaRegistryURL string
	DatabaseURL       string
	OTLPTracesURL     string
	TraceRootRate     float64
	LogLevel          string
	HTTPAddr          string
}

// Lookup abstracts os.LookupEnv so tests can inject an environment.
type Lookup func(key string) (string, bool)

// Load reads the shared configuration. serviceName is used when SERVICE_NAME is unset.
func Load(serviceName string, env Lookup) (Config, error) {
	if env == nil {
		env = os.LookupEnv
	}
	r := Reader{env: env}
	cfg := Config{
		ServiceName:       r.String("SERVICE_NAME", serviceName),
		KafkaBrokers:      r.List("KAFKA_BROKERS", "kafka-lab-kafka-bootstrap.kafka:9092"),
		SchemaRegistryURL: r.String("SCHEMA_REGISTRY_URL", "http://schema-registry.data:8081"),
		DatabaseURL:       r.String("DATABASE_URL", ""),
		OTLPTracesURL:     r.String("OTLP_TRACES_URL", ""),
		TraceRootRate:     r.Float("TRACE_ROOT_RATE", 2),
		LogLevel:          r.String("LOG_LEVEL", "info"),
		HTTPAddr:          r.String("HTTP_ADDR", ":8080"),
	}
	return cfg, r.Err()
}

// Reader reads typed values and remembers the first parse error.
type Reader struct {
	env Lookup
	err error
}

// NewReader returns a Reader over env (os.LookupEnv when nil).
func NewReader(env Lookup) *Reader {
	if env == nil {
		env = os.LookupEnv
	}
	return &Reader{env: env}
}

// Err returns the first parse error, if any.
func (r *Reader) Err() error { return r.err }

func (r *Reader) raw(key string) (string, bool) {
	v, ok := r.env(key)
	if !ok || strings.TrimSpace(v) == "" {
		return "", false
	}
	return strings.TrimSpace(v), true
}

func (r *Reader) fail(key, value string, err error) {
	if r.err == nil {
		r.err = fmt.Errorf("config %s=%q: %w", key, value, err)
	}
}

// String returns the value of key or def.
func (r *Reader) String(key, def string) string {
	if v, ok := r.raw(key); ok {
		return v
	}
	return def
}

// List returns a comma separated value of key or the comma separated def.
func (r *Reader) List(key, def string) []string {
	var out []string
	for _, p := range strings.Split(r.String(key, def), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Int returns the integer value of key or def.
func (r *Reader) Int(key string, def int) int {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.fail(key, v, err)
		return def
	}
	return n
}

// Float returns the float value of key or def.
func (r *Reader) Float(key string, def float64) float64 {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		r.fail(key, v, err)
		return def
	}
	return f
}

// Duration returns the duration value of key (Go syntax, e.g. "300s") or def.
func (r *Reader) Duration(key string, def time.Duration) time.Duration {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.fail(key, v, err)
		return def
	}
	return d
}

// Bool returns the boolean value of key or def.
func (r *Reader) Bool(key string, def bool) bool {
	v, ok := r.raw(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.fail(key, v, err)
		return def
	}
	return b
}

// MapEnv turns a map into a Lookup (for tests and local runs).
func MapEnv(m map[string]string) Lookup {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}
