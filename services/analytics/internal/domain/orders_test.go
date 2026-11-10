package domain

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)

func created(id string, cents int64, region string) Order {
	return Order{OrderID: id, Created: true, TotalCents: cents, Region: region}
}

func Test_shouldCountRevenueWhenTheShipmentFollowsTheOrder(t *testing.T) {
	// GIVEN: a 49.90 EUR order from region NORTH that analytics has seen
	o := created("o-1", 4990, "NORTH")
	first := OnCreated(o, true, o)

	// WHEN: its shipment arrives
	e := OnOutcome(first.Next, false, OutcomeShipped)

	// THEN: the creation was counted plainly and the shipment brings 49.90 EUR revenue in NORTH
	if first.Changed || first.Duplicate || first.RevenueCents != 0 {
		t.Fatalf("created: %+v", first)
	}
	if !e.Changed || e.Duplicate || e.RevenueCents != 4990 || e.Next.Region != "NORTH" || e.Next.Outcome != OutcomeShipped {
		t.Fatalf("shipped: %+v", e)
	}
}

func Test_shouldCountRevenueLateWhenTheShipmentIsProcessedBeforeTheOrder(t *testing.T) {
	// GIVEN: analytics lags on orders.created and processes the shipment first
	shipped := Order{OrderID: "o-2", Outcome: OutcomeShipped}
	first := OnOutcome(shipped, true, OutcomeShipped)

	// WHEN: the order itself arrives
	e := OnCreated(first.Next, false, created("o-2", 1200, "WEST"))

	// THEN: no revenue without a value at first; the order brings its 12.00 EUR in WEST
	if first.RevenueCents != 0 || first.Changed || first.Duplicate {
		t.Fatalf("shipped first: %+v", first)
	}
	if e.Duplicate || !e.Changed || e.RevenueCents != 1200 || e.Next.Region != "WEST" || !e.Next.Created {
		t.Fatalf("created later: %+v", e)
	}
}

func Test_shouldNeverCountRevenueWhenTheOrderIsCancelled(t *testing.T) {
	// GIVEN: a known order, and an unknown order whose cancellation came first
	known := created("o-3", 900, "EAST")
	late := OnOutcome(Order{OrderID: "o-4", Outcome: OutcomeCancelled}, true, OutcomeCancelled).Next

	// WHEN: the first is cancelled and the second's creation arrives
	a := OnOutcome(known, false, OutcomeCancelled)
	b := OnCreated(late, false, created("o-4", 700, "EAST"))

	// THEN: both are counted, neither brings revenue
	if a.RevenueCents != 0 || a.Duplicate || b.RevenueCents != 0 || b.Duplicate {
		t.Fatalf("got %+v and %+v", a, b)
	}
}

func Test_shouldFlagADuplicateWhenTheSameOrderIsCreatedTwice(t *testing.T) {
	// GIVEN: a counted order
	o := created("o-5", 500, "SOUTH")

	// WHEN: orders.created for the same order arrives again under a new event id
	e := OnCreated(o, false, created("o-5", 500, "SOUTH"))

	// THEN: it is a business duplicate and changes nothing
	if !e.Duplicate || e.Changed || e.RevenueCents != 0 {
		t.Fatalf("got %+v", e)
	}
}

func Test_shouldFlagADuplicateWhenAnOrderGetsASecondOutcome(t *testing.T) {
	// GIVEN: a shipped order
	o := created("o-6", 500, "SOUTH")
	o.Outcome = OutcomeShipped

	// WHEN: another shipment or a cancellation arrives for it
	again, contradicting := OnOutcome(o, false, OutcomeShipped), OnOutcome(o, false, OutcomeCancelled)

	// THEN: both are duplicates; revenue is not counted twice
	if !again.Duplicate || !contradicting.Duplicate || again.RevenueCents != 0 || contradicting.Next.Outcome != OutcomeShipped {
		t.Fatalf("got %+v and %+v", again, contradicting)
	}
}

func Test_shouldForgetOrdersSoonerWhenTheyReachedAnOutcome(t *testing.T) {
	// GIVEN: a finished order and one still in flight
	done := Order{Outcome: OutcomeCancelled}
	open := created("o-7", 1, "NORTH")

	// WHEN: their retention is computed now
	// THEN: the finished one is kept an hour, the open one a day
	if !ForgetAfter(done, t0).Equal(t0.Add(time.Hour)) || !ForgetAfter(open, t0).Equal(t0.Add(24*time.Hour)) {
		t.Fatalf("got %v and %v", ForgetAfter(done, t0), ForgetAfter(open, t0))
	}
}

func Test_shouldMeasureCompletionTimeWhenAnOrderFinishes(t *testing.T) {
	// GIVEN: an order created 3.5 s ago, and one stamped by a clock running ahead
	// WHEN: their completion time is measured
	normal, skewed := CompletionSeconds(t0.Add(-3500*time.Millisecond), t0), CompletionSeconds(t0.Add(time.Second), t0)

	// THEN: it is 3.5 s, and never negative
	if normal != 3.5 || skewed != 0 {
		t.Fatalf("got %v and %v", normal, skewed)
	}
}
