package config

import (
	"strings"
	"testing"
	"time"
)

func Test_shouldUseClusterDefaultsWhenNothingIsConfigured(t *testing.T) {
	// GIVEN: a payment pod started without any environment overrides
	env := MapEnv(map[string]string{})

	// WHEN: the configuration is loaded
	cfg, err := Load("payment", env)

	// THEN: it points at the in-cluster Kafka and registry and tracing stays off
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "payment" || cfg.KafkaBrokers[0] != "kafka-lab-kafka-bootstrap.kafka:9092" ||
		cfg.SchemaRegistryURL != "http://schema-registry.data:8081" || cfg.OTLPTracesURL != "" ||
		cfg.HTTPAddr != ":8080" || cfg.TraceRootRate != 2 || cfg.LogLevel != "info" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func Test_shouldSplitBrokerListWhenSeveralBrokersAreConfigured(t *testing.T) {
	// GIVEN: a local run against two brokers with stray spaces and an empty entry
	env := MapEnv(map[string]string{"KAFKA_BROKERS": " a:9092, ,b:9092 ", "SERVICE_NAME": "shipping"})

	// WHEN: the configuration is loaded
	cfg, err := Load("ignored", env)

	// THEN: both brokers are used and the explicit service name wins
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cfg.KafkaBrokers, "|") != "a:9092|b:9092" || cfg.ServiceName != "shipping" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func Test_shouldReportTheFirstBadValueWhenANumberCannotBeParsed(t *testing.T) {
	// GIVEN: an operator typo in the trace rate
	env := MapEnv(map[string]string{"TRACE_ROOT_RATE": "fast"})

	// WHEN: the configuration is loaded
	_, err := Load("order-generator", env)

	// THEN: startup fails with a message naming the variable
	if err == nil || !strings.Contains(err.Error(), "TRACE_ROOT_RATE") {
		t.Fatalf("expected error naming TRACE_ROOT_RATE, got %v", err)
	}
}

func Test_shouldParseTypedServiceSettingsWhenTheyAreValid(t *testing.T) {
	// GIVEN: shipping specific settings for deadline, sweeper size, a flag and a ratio
	r := NewReader(MapEnv(map[string]string{
		"JOIN_DEADLINE": "300s", "SWEEP_BATCH": "50", "FEATURE": "true", "RATIO": "0.25",
	}))

	// WHEN: they are read with their types
	d := r.Duration("JOIN_DEADLINE", time.Second)
	n := r.Int("SWEEP_BATCH", 1)
	b := r.Bool("FEATURE", false)
	f := r.Float("RATIO", 0)

	// THEN: every value is parsed and no error is recorded
	if d != 300*time.Second || n != 50 || !b || f != 0.25 || r.Err() != nil {
		t.Fatalf("got %v %v %v %v %v", d, n, b, f, r.Err())
	}
}

func Test_shouldFallBackToDefaultsWhenTypedSettingsAreMissing(t *testing.T) {
	// GIVEN: an environment without the optional settings
	r := NewReader(MapEnv(map[string]string{"BLANK": "  "}))

	// WHEN: they are read
	d := r.Duration("JOIN_DEADLINE", time.Minute)
	n := r.Int("SWEEP_BATCH", 7)
	b := r.Bool("BLANK", true)

	// THEN: the defaults apply, blank values count as missing
	if d != time.Minute || n != 7 || !b || r.Err() != nil {
		t.Fatalf("got %v %v %v %v", d, n, b, r.Err())
	}
}

func Test_shouldKeepDefaultsAndReportErrorsWhenTypedSettingsAreMalformed(t *testing.T) {
	// GIVEN: malformed values for a duration, an integer and a boolean
	for _, tc := range []struct {
		key  string
		read func(r *Reader)
	}{
		{"D", func(r *Reader) { r.Duration("D", time.Second) }},
		{"I", func(r *Reader) { r.Int("I", 1) }},
		{"B", func(r *Reader) { r.Bool("B", false) }},
	} {
		r := NewReader(MapEnv(map[string]string{tc.key: "nonsense"}))

		// WHEN: the value is read
		tc.read(r)

		// THEN: the reader reports which variable was wrong
		if r.Err() == nil || !strings.Contains(r.Err().Error(), tc.key) {
			t.Fatalf("expected error for %s, got %v", tc.key, r.Err())
		}
	}
}

func Test_shouldReadTheProcessEnvironmentWhenNoLookupIsGiven(t *testing.T) {
	// GIVEN: a real process environment
	t.Setenv("LAB_TEST_VALUE", "42")

	// WHEN: values are read without an injected lookup
	r := NewReader(nil)
	cfg, err := Load("analytics", nil)

	// THEN: the process environment is used
	if r.Int("LAB_TEST_VALUE", 0) != 42 || err != nil || cfg.ServiceName == "" {
		t.Fatalf("process environment not used: %v %v", cfg, err)
	}
}
