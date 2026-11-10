package experiments

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/control"
)

// KafkaPublisher writes control snapshots (kit control.Publish) and audit events.
type KafkaPublisher struct {
	cl  *kgo.Client
	enc control.Encoder
}

// NewKafkaPublisher returns a publisher; enc must know the platform.control and
// platform.audit schemas (serde.New with both topics).
func NewKafkaPublisher(cl *kgo.Client, enc control.Encoder) *KafkaPublisher {
	return &KafkaPublisher{cl: cl, enc: enc}
}

// PublishControl implements Publisher.
func (p *KafkaPublisher) PublishControl(ctx context.Context, snap *events.ControlSnapshot) error {
	return control.Publish(ctx, p.cl, p.enc, snap)
}

// PublishAudit implements Publisher (key = event id).
func (p *KafkaPublisher) PublishAudit(ctx context.Context, ev *events.AuditEvent) error {
	b, err := p.enc.Encode(ev)
	if err != nil {
		return err
	}
	return p.cl.ProduceSync(ctx, &kgo.Record{Topic: contracts.TopicPlatformAudit, Key: []byte(ev.EventID), Value: b}).FirstErr()
}

// KafkaGenerations reads platform.control to its end (kit control client tracking every
// service) and returns the highest generation, so the engine continues after it.
func KafkaGenerations(brokers []string, dec control.Decoder, extra ...kgo.Opt) func(context.Context) (int64, error) {
	return func(ctx context.Context) (int64, error) {
		c, err := control.Start(ctx, control.Options{Brokers: brokers, Service: "", Decoder: dec, ExtraOpts: extra})
		if err != nil {
			return 0, err
		}
		defer c.Close()
		var top int64
		for _, svc := range contracts.Services {
			if s, ok := c.SnapshotOf(svc); ok && s.Generation > top {
				top = s.Generation
			}
		}
		return top, nil
	}
}

// KafkaAdmin implements Admin with kadm.
type KafkaAdmin struct {
	adm *kadm.Client
}

// NewKafkaAdmin wraps a client.
func NewKafkaAdmin(cl *kgo.Client) *KafkaAdmin { return &KafkaAdmin{adm: kadm.NewClient(cl)} }

// DeleteRecordsToEnd implements Admin.
func (a *KafkaAdmin) DeleteRecordsToEnd(ctx context.Context, topics []string) error {
	ends, err := a.adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return fmt.Errorf("end offsets: %w", err)
	}
	res, err := a.adm.DeleteRecords(ctx, ends.Offsets())
	if err == nil {
		err = res.Error()
	}
	if err != nil { // coverage-ignore: broker-side refusal right after a successful listing (ACLs), not reproducible with kfake
		return fmt.Errorf("delete records: %w", err)
	}
	return nil
}

// CommitGroupsToEnd implements Admin.
func (a *KafkaAdmin) CommitGroupsToEnd(ctx context.Context, groups []string) error {
	for _, g := range groups {
		cur, err := a.adm.FetchOffsets(ctx, g)
		if err == nil {
			err = cur.Error()
		}
		if errors.Is(err, kerr.GroupIDNotFound) {
			continue // never committed: it starts at the (new) log start
		}
		if err != nil {
			return fmt.Errorf("offsets of group %s: %w", g, err)
		}
		topics := cur.Offsets().TopicsSet().Topics()
		if len(topics) == 0 {
			continue
		}
		ends, err := a.adm.ListEndOffsets(ctx, topics...)
		if err == nil {
			err = ends.Error()
		}
		if err == nil {
			var res kadm.OffsetResponses
			res, err = a.adm.CommitOffsets(ctx, g, ends.Offsets())
			if err == nil {
				err = res.Error()
			}
		}
		if err != nil { // coverage-ignore: broker-side refusal after a successful fetch, not reproducible with kfake
			return fmt.Errorf("commit group %s to the end: %w", g, err)
		}
	}
	return nil
}
