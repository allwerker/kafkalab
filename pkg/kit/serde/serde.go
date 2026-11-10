// Package serde encodes and decodes lab events in the Confluent wire format, with schemas
// resolved from the schema registry (Karapace). Services never register schemas themselves:
// the schema-registration job owns the subjects, services look up the latest version.
package serde

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/sr"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
)

// ErrUndecodable marks bytes that are not a valid event of a known schema (poison messages).
var ErrUndecodable = errors.New("undecodable record")

// Prototypes creates an empty value for every Avro value type name (contracts.ValueTypes).
var Prototypes = map[string]func() any{
	"OrderCreated":      func() any { return &events.OrderCreated{} },
	"PaymentCompleted":  func() any { return &events.PaymentCompleted{} },
	"PaymentFailed":     func() any { return &events.PaymentFailed{} },
	"PaymentRefunded":   func() any { return &events.PaymentRefunded{} },
	"InventoryReserved": func() any { return &events.InventoryReserved{} },
	"InventoryRejected": func() any { return &events.InventoryRejected{} },
	"InventoryReleased": func() any { return &events.InventoryReleased{} },
	"OrderShipped":      func() any { return &events.OrderShipped{} },
	"OrderCancelled":    func() any { return &events.OrderCancelled{} },
	"NotificationSent":  func() any { return &events.NotificationSent{} },
	"ControlSnapshot":   func() any { return &events.ControlSnapshot{} },
	"AuditEvent":        func() any { return &events.AuditEvent{} },
}

// Serde encodes values of the registered topics and decodes any registered schema id.
type Serde struct {
	serde *sr.Serde
}

// Options configures New.
type Options struct {
	Registry *sr.Client
	Topics   []string // topics whose value schemas this service reads or writes
	Log      *slog.Logger
	// RetryEvery is the pause between lookups while the registry or a subject is missing.
	RetryEvery time.Duration
}

// New resolves the latest schema of every topic's subject, waiting until all of them exist
// (the registration job may still be running when a service starts) or ctx ends.
func New(ctx context.Context, o Options) (*Serde, error) {
	if o.RetryEvery == 0 {
		o.RetryEvery = 2 * time.Second
	}
	s := &Serde{serde: sr.NewSerde()}
	for _, topic := range o.Topics {
		name, ok := contracts.ValueTypes[topic]
		if !ok {
			return nil, fmt.Errorf("topic %s has no Avro value type", topic)
		}
		for {
			err := s.register(ctx, o.Registry, contracts.Subject(topic), name)
			if err == nil {
				break
			}
			if o.Log != nil {
				o.Log.WarnContext(ctx, "schema not available yet", "subject", contracts.Subject(topic), "error", err)
			}
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("schema for %s: %w", topic, err)
			case <-time.After(o.RetryEvery):
			}
		}
	}
	return s, nil
}

func (s *Serde) register(ctx context.Context, cl *sr.Client, subject, name string) error {
	ss, err := cl.SchemaByVersion(ctx, subject, -1)
	if err != nil {
		return err
	}
	schema, err := avro.Parse(ss.Schema.Schema)
	if err != nil {
		return fmt.Errorf("parse %s: %w", subject, err)
	}
	s.serde.Register(ss.ID, Prototypes[name](),
		sr.EncodeFn(func(v any) ([]byte, error) { return avro.Marshal(schema, v) }),
		sr.DecodeFn(func(b []byte, v any) error { return avro.Unmarshal(schema, b, v) }),
	)
	return nil
}

// Encode serialises an event (a pointer to a generated type) with its schema id header.
func (s *Serde) Encode(v any) ([]byte, error) { return s.serde.Encode(v) }

// Decode returns a new value of the type registered for the record's schema id.
// Anything that is not a registered event yields an error wrapping ErrUndecodable.
func (s *Serde) Decode(b []byte) (any, error) {
	v, err := s.serde.DecodeNew(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUndecodable, err)
	}
	// Types are registered as pointers (events are always handled by pointer), so DecodeNew
	// returns a pointer to a pointer; hand out the inner pointer.
	return reflect.ValueOf(v).Elem().Interface(), nil
}

// Meta returns the envelope of a business event, false for platform events or unknown values.
func Meta(v any) (events.EventMeta, bool) {
	switch e := v.(type) {
	case *events.OrderCreated:
		return e.Meta, true
	case *events.PaymentCompleted:
		return e.Meta, true
	case *events.PaymentFailed:
		return e.Meta, true
	case *events.PaymentRefunded:
		return e.Meta, true
	case *events.InventoryReserved:
		return e.Meta, true
	case *events.InventoryRejected:
		return e.Meta, true
	case *events.InventoryReleased:
		return e.Meta, true
	case *events.OrderShipped:
		return e.Meta, true
	case *events.OrderCancelled:
		return e.Meta, true
	case *events.NotificationSent:
		return e.Meta, true
	}
	return events.EventMeta{}, false
}
