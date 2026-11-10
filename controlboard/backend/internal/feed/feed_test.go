package feed

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/controlboard/backend/internal/api"
	"kafkalab/controlboard/backend/internal/grafana"
	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/testkit"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

// decoder returns the value registered under the record bytes; unknown bytes are poison.
type decoder map[string]any

func (d decoder) Decode(b []byte) (any, error) {
	if v, ok := d[string(b)]; ok {
		return v, nil
	}
	return nil, errors.New("unknown magic byte")
}

func meta(order string, at time.Time) events.EventMeta {
	return events.EventMeta{EventID: "e-" + order, OrderID: order, OccurredAt: at}
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newFeed(d decoder, orders int) (*Feed, *clock) {
	c := &clock{t: t0}
	return New(Options{Decoder: d, Grafana: grafana.Links{Base: "http://grafana.localhost:8080"}, Orders: orders, Now: c.now}), c
}

func rec(topic, key, value string) *kgo.Record {
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(value), Timestamp: t0}
}

func Test_shouldMapEveryBusinessEventToItsLiveKindWhenRecordsArrive(t *testing.T) {
	// GIVEN: one event of every business topic for order o-1
	d := decoder{
		"created":  &events.OrderCreated{Meta: meta("o-1", t0), TotalCents: 8900, Region: events.Region("NORTH")},
		"paid":     &events.PaymentCompleted{Meta: meta("o-1", t0.Add(1*time.Second)), AmountCents: 8900},
		"failed":   &events.PaymentFailed{Meta: meta("o-1", t0.Add(2*time.Second)), AmountCents: 8900, Reason: "DECLINED"},
		"refunded": &events.PaymentRefunded{Meta: meta("o-1", t0.Add(3*time.Second)), AmountCents: 8900},
		"reserved": &events.InventoryReserved{Meta: meta("o-1", t0.Add(4*time.Second))},
		"rejected": &events.InventoryRejected{Meta: meta("o-1", t0.Add(5*time.Second)), Reason: "OUT_OF_STOCK"},
		"released": &events.InventoryReleased{Meta: meta("o-1", t0.Add(6*time.Second))},
		"shipped":  &events.OrderShipped{Meta: meta("o-1", t0.Add(7*time.Second)), Carrier: "DHL"},
		"cancel":   &events.OrderCancelled{Meta: meta("o-1", t0.Add(8*time.Second)), Reason: "PAYMENT_FAILED"},
		"notified": &events.NotificationSent{Meta: meta("o-1", t0.Add(9*time.Second))},
	}
	f, _ := newFeed(d, 0)
	topics := map[string]string{"created": contracts.TopicOrdersCreated, "paid": contracts.TopicPaymentsCompleted,
		"failed": contracts.TopicPaymentsFailed, "refunded": contracts.TopicPaymentsRefunded, "reserved": contracts.TopicInventoryReserved,
		"rejected": contracts.TopicInventoryRejected, "released": contracts.TopicInventoryReleased, "shipped": contracts.TopicOrdersShipped,
		"cancel": contracts.TopicOrdersCancelled, "notified": contracts.TopicNotificationsSent}

	// WHEN: the records arrive in reverse order
	for _, v := range []string{"notified", "cancel", "shipped", "released", "rejected", "reserved", "refunded", "failed", "paid", "created"} {
		f.Ingest(rec(topics[v], "o-1", v))
	}

	// THEN: the order timeline lists them chronologically with their kinds and details
	tl, ok := f.Order("o-1")
	want := []string{"created", "paid", "payment-failed", "refunded", "reserved", "rejected", "released", "shipped", "cancelled", "notified"}
	if !ok || len(tl.Events) != len(want) {
		t.Fatalf("got %+v", tl)
	}
	for i, k := range want {
		if tl.Events[i].Kind != k {
			t.Fatalf("event %d: %s, want %s", i, tl.Events[i].Kind, k)
		}
	}
	first, failed, shipped := tl.Events[0], tl.Events[2], tl.Events[7]
	if first.ValueCents != 8900 || first.Region != "NORTH" || failed.Reason != "DECLINED" || shipped.Carrier != "DHL" || tl.Events[8].Reason != "PAYMENT_FAILED" {
		t.Fatalf("details lost: %+v", tl.Events)
	}
	if tl.TraceID != "" || tl.TraceURL != "" || !strings.Contains(tl.LogsURL, "vlogs") {
		t.Fatalf("links %+v", tl)
	}
}

func Test_shouldShowADeadLetterWithItsReasonWhenARecordLandsInTheDLQ(t *testing.T) {
	// GIVEN: a poison order that a consumer dead-lettered with reason "decode"
	f, _ := newFeed(decoder{}, 0)
	r := rec(contracts.TopicOrdersDLQ, "o-9", "\x00garbage")
	r.Headers = []kgo.RecordHeader{{Key: contracts.HeaderDLQSourceTopic, Value: []byte("orders.created")}, {Key: contracts.HeaderDLQReason, Value: []byte("decode")}}

	// WHEN: the dead letter arrives
	f.Ingest(r)

	// THEN: the live view shows a dlq event for o-9 at the record time
	ev := f.Drain().Items[0]
	if ev.Kind != api.EventDLQ || ev.OrderID != "o-9" || ev.Reason != "decode" || !ev.At.Equal(t0) {
		t.Fatalf("got %+v", ev)
	}
}

func Test_shouldCountPoisonRecordsInTheRateButNotShowThemWhenTheyCannotBeDecoded(t *testing.T) {
	// GIVEN: a poison record on orders.created and a control snapshot (not a business event)
	f, c := newFeed(decoder{"ctl": &events.ControlSnapshot{Service: "payment"}}, 0)

	// WHEN: both arrive and one second passes
	f.Ingest(rec(contracts.TopicOrdersCreated, "o-1", "poison"))
	f.Ingest(rec(contracts.TopicPlatformControl, "payment", "ctl"))
	c.t = c.t.Add(time.Second)

	// THEN: orders.created counts one record, nothing is shown, the control topic has no rate
	rates := f.Rates()
	if rates[contracts.TopicOrdersCreated] != 1.0/3 || len(f.Drain().Items) != 0 {
		t.Fatalf("rates %+v", rates)
	}
	if _, ok := rates[contracts.TopicPlatformControl]; ok {
		t.Fatal("control topic must not have a rate")
	}
	if _, ok := f.Order("o-1"); ok {
		t.Fatal("poison must not create an order")
	}
}

func Test_shouldAverageTheLastThreeCompleteSecondsWhenRatesAreRequested(t *testing.T) {
	// GIVEN: 10, 20 and 30 orders in three consecutive seconds, and 90 in an older second
	f, c := newFeed(decoder{"c": &events.OrderCreated{Meta: meta("o", t0)}}, 0)
	burst := func(n int) {
		for range n {
			f.Ingest(rec(contracts.TopicOrdersCreated, "o", "c"))
		}
		c.t = c.t.Add(time.Second)
	}
	burst(90)
	for _, n := range []int{10, 20, 30} {
		burst(n)
	}
	// AND: 5 orders in the current, incomplete second; the oldest bucket gets reused later
	for range 5 {
		f.Ingest(rec(contracts.TopicOrdersCreated, "o", "c"))
	}

	// WHEN: the rates are read, and again after five more empty seconds
	now := f.Rates()[contracts.TopicOrdersCreated]
	c.t = c.t.Add(5 * time.Second)
	later := f.Rates()[contracts.TopicOrdersCreated]

	// THEN: the rate is (10+20+30)/3 = 20/s and drops to zero once traffic stops
	if now != 20 || later != 0 {
		t.Fatalf("rate %v then %v", now, later)
	}
}

func Test_shouldSampleAtMost40ItemsWithAScaleWhenManyEventsArriveBetweenTwoMessages(t *testing.T) {
	// GIVEN: 100 orders created in one 250 ms interval
	d := decoder{}
	f, _ := newFeed(d, 0)
	for i := range 100 {
		id := "o-" + strconv.Itoa(i)
		d[id] = &events.OrderCreated{Meta: meta(id, t0.Add(time.Duration(i)*time.Millisecond))}
		f.Ingest(rec(contracts.TopicOrdersCreated, id, id))
	}

	// WHEN: the events message is built, and the next one after a quiet interval
	busy := f.Drain()
	quiet := f.Drain()

	// THEN: 40 chronological items each stand for 2.5 events; the quiet message is empty with scale 1
	if len(busy.Items) != 40 || busy.Scale != 2.5 {
		t.Fatalf("got %d items scale %v", len(busy.Items), busy.Scale)
	}
	for i := 1; i < len(busy.Items); i++ {
		if busy.Items[i].At.Before(busy.Items[i-1].At) {
			t.Fatal("items not in time order")
		}
	}
	if len(quiet.Items) != 0 || quiet.Scale != 1 {
		t.Fatalf("quiet %+v", quiet)
	}
}

func Test_shouldForgetTheOldestOrderWhenTheRingIsFull(t *testing.T) {
	// GIVEN: a ring for two orders and three orders arriving
	d := decoder{}
	f, _ := newFeed(d, 2)
	for _, id := range []string{"a", "b", "c"} {
		d[id] = &events.OrderCreated{Meta: meta(id, t0)}
		f.Ingest(rec(contracts.TopicOrdersCreated, id, id))
	}
	// AND: order d evicts b next
	d["d"] = &events.OrderCreated{Meta: meta("d", t0)}
	f.Ingest(rec(contracts.TopicOrdersCreated, "d", "d"))

	// WHEN: the board looks the orders up
	_, a := f.Order("a")
	_, b := f.Order("b")
	_, c := f.Order("c")
	_, dd := f.Order("d")

	// THEN: only the two most recent orders are still known
	if a || b || !c || !dd {
		t.Fatalf("a=%v b=%v c=%v d=%v", a, b, c, dd)
	}
}

func Test_shouldLinkTheTraceWhenTheOrderCarriedATraceparentHeader(t *testing.T) {
	// GIVEN: an order created with a sampled trace, then 60 notifications with broken trace headers
	d := decoder{"c": &events.OrderCreated{Meta: meta("o-1", t0)}, "n": &events.NotificationSent{Meta: meta("o-1", t0.Add(time.Second))}}
	f, _ := newFeed(d, 0)
	r := rec(contracts.TopicOrdersCreated, "o-1", "c")
	r.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")}}
	f.Ingest(r)
	for i := range 60 {
		n := rec(contracts.TopicNotificationsSent, "o-1", "n")
		n.Headers = []kgo.RecordHeader{{Key: "traceparent", Value: []byte([]string{"01-x-y-z", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", "garbage"}[i%3])}}
		f.Ingest(n)
	}

	// WHEN: the order timeline is requested
	tl, _ := f.Order("o-1")

	// THEN: it keeps the creation's trace id with a Grafana link and at most 50 events
	if tl.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || !strings.Contains(tl.TraceURL, "4bf92f3577b34da6a3ce929d0e0e4736") || len(tl.Events) != 50 {
		t.Fatalf("got trace %q, %d events", tl.TraceID, len(tl.Events))
	}
}

func Test_shouldFeedLiveRecordsFromKafkaWhenTheConsumerRuns(t *testing.T) {
	// GIVEN: a Kafka cluster and the feed consuming from the end of every topic
	cluster := testkit.NewCluster(t)
	d := decoder{"c": &events.OrderCreated{Meta: meta("o-live", t0)}}
	f := New(Options{Decoder: d})
	cl, err := kgo.NewClient(ClientOptions(cluster.Brokers)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, cl) }()
	prod, _ := kgo.NewClient(kgo.SeedBrokers(cluster.Brokers...))
	defer prod.Close()

	// WHEN: orders are created until the feed has seen one
	deadline := time.Now().Add(10 * time.Second)
	for {
		prod.ProduceSync(context.Background(), rec(contracts.TopicOrdersCreated, "o-live", "c"))
		if _, ok := f.Order("o-live"); ok || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()

	// THEN: the order is known and the feed stops with the context
	if _, ok := f.Order("o-live"); !ok {
		t.Fatal("order not seen")
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run ended with %v", err)
	}
}

func Test_shouldStopFeedingWhenTheClientIsClosed(t *testing.T) {
	// GIVEN: a feed polling an idle cluster
	cluster := testkit.NewCluster(t)
	f := New(Options{Decoder: decoder{}})
	cl, _ := kgo.NewClient(ClientOptions(cluster.Brokers)...)
	done := make(chan error, 1)
	go func() { done <- f.Run(context.Background(), cl) }()

	// WHEN: the client is closed during shutdown
	time.Sleep(100 * time.Millisecond)
	cl.Close()

	// THEN: the feed ends without error
	if err := <-done; err != nil {
		t.Fatalf("got %v", err)
	}
}
