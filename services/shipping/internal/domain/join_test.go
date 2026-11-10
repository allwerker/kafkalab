package domain

import (
	"strconv"
	"strings"
	"testing"
)

var normal = Policy{}
var outage = Policy{WarehouseOutage: true}

// arrival is one input reaching shipping: which half and whether it succeeded.
type arrival struct {
	payment bool // true: payments.*, false: inventory.*
	ok      bool
}

func apply(j Join, a arrival, p Policy) Decision {
	if a.payment {
		return OnPayment(j, a.ok, p)
	}
	return OnStock(j, a.ok, p)
}

func Test_shouldReachTheContractOutcomeWhenBothHalvesArriveInAnyOrder(t *testing.T) {
	// GIVEN: every combination of payment and stock outcome, in both arrival orders
	type expect struct {
		first, second   Action
		status          Status
		reason          Reason
		refund, release bool
	}
	cases := []struct {
		name          string
		first, second arrival
		want          expect
	}{
		{"payment ok, then stock ok", arrival{true, true}, arrival{false, true}, expect{ActionNone, ActionShip, StatusShipped, "", false, false}},
		{"stock ok, then payment ok", arrival{false, true}, arrival{true, true}, expect{ActionNone, ActionShip, StatusShipped, "", false, false}},
		{"payment ok, then stock rejected", arrival{true, true}, arrival{false, false}, expect{ActionNone, ActionCancel, StatusCancelled, ReasonOutOfStock, true, false}},
		{"stock rejected, then payment ok", arrival{false, false}, arrival{true, true}, expect{ActionCancel, ActionNone, StatusCancelled, ReasonOutOfStock, false, false}},
		{"payment failed, then stock ok", arrival{true, false}, arrival{false, true}, expect{ActionCancel, ActionNone, StatusCancelled, ReasonPaymentFailed, false, false}},
		{"stock ok, then payment failed", arrival{false, true}, arrival{true, false}, expect{ActionNone, ActionCancel, StatusCancelled, ReasonPaymentFailed, false, true}},
		{"payment failed, then stock rejected", arrival{true, false}, arrival{false, false}, expect{ActionCancel, ActionNone, StatusCancelled, ReasonPaymentFailed, false, false}},
		{"stock rejected, then payment failed", arrival{false, false}, arrival{true, false}, expect{ActionCancel, ActionNone, StatusCancelled, ReasonOutOfStock, false, false}},
	}
	for _, c := range cases {
		// WHEN: the first half arrives, then the second one
		d1 := apply(Join{Status: StatusOpen}, c.first, normal)
		d2 := apply(d1.Next, c.second, normal)

		// THEN: exactly one terminal decision is made, with the reason and compensation flags
		// of what was known at that moment, and the later half changes nothing
		cancel := d1
		if d2.Action != ActionNone {
			cancel = d2
		}
		got := expect{d1.Action, d2.Action, d2.Next.Status, cancel.Reason, cancel.RefundRequired, cancel.ReleaseRequired}
		if got != c.want {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func Test_shouldWaitForTheOtherHalfWhenOnlyASuccessIsKnown(t *testing.T) {
	// GIVEN: a newly seen order
	for _, a := range []arrival{{true, true}, {false, true}} {
		// WHEN: only its payment (or only its reservation) succeeded so far
		d := apply(Join{}, a, normal)

		// THEN: the order stays open and nothing is published yet
		if d.Action != ActionNone || d.Next.Status != StatusOpen {
			t.Fatalf("%+v: got %+v", a, d)
		}
	}
}

func Test_shouldHoldTheShipmentBackWhenTheWarehouseIsDown(t *testing.T) {
	// GIVEN: a paid order during a warehouse outage
	paid := OnPayment(Join{Status: StatusOpen}, true, outage).Next

	// WHEN: its stock is reserved
	d := OnStock(paid, true, outage)

	// THEN: it is ready to ship but nothing goes out yet
	if d.Action != ActionNone || d.Next != (Join{Payment: HalfOK, Stock: HalfOK, Status: StatusReady}) {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldStillCancelImmediatelyWhenAFailureArrivesDuringAWarehouseOutage(t *testing.T) {
	// GIVEN: a warehouse outage
	// WHEN: the payment of a new order fails
	d := OnPayment(Join{}, false, outage)

	// THEN: the order is cancelled right away; the outage only delays shipments
	if d.Action != ActionCancel || d.Reason != ReasonPaymentFailed {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldShipHeldBackOrdersWhenTheWarehouseOutageEnds(t *testing.T) {
	// GIVEN: an order that became ready during an outage
	ready := Join{Payment: HalfOK, Stock: HalfOK, Status: StatusReady}

	// WHEN: the sweeper looks at it during and after the outage
	during := OnSweep(ready, outage)
	after := OnSweep(ready, normal)

	// THEN: it waits while the warehouse is down and ships once it is back
	if during.Action != ActionNone || during.Next != ready {
		t.Fatalf("during outage: %+v", during)
	}
	if after.Action != ActionShip || after.Next.Status != StatusShipped {
		t.Fatalf("after outage: %+v", after)
	}
}

func Test_shouldIgnoreTheSweepWhenTheOrderIsNotHeldBack(t *testing.T) {
	// GIVEN: orders that are open, shipped or cancelled
	for _, s := range []Status{StatusOpen, StatusShipped, StatusCancelled} {
		// WHEN: the sweeper releases held-back shipments
		d := OnSweep(Join{Payment: HalfOK, Status: s}, normal)

		// THEN: nothing happens to them
		if d.Action != ActionNone || d.Next.Status != s {
			t.Fatalf("%s: got %+v", s, d)
		}
	}
}

func Test_shouldCancelWithWhatIsKnownWhenTheJoinDeadlinePasses(t *testing.T) {
	// GIVEN: open orders that only know their payment, only their stock, or nothing
	cases := []struct {
		j               Join
		refund, release bool
	}{
		{Join{Payment: HalfOK, Status: StatusOpen}, true, false},
		{Join{Stock: HalfOK, Status: StatusOpen}, false, true},
		{Join{Status: StatusOpen}, false, false},
	}
	for _, c := range cases {
		// WHEN: the deadline passes
		d := OnDeadline(c.j)

		// THEN: they are cancelled as DEADLINE_EXCEEDED and the known-good half is compensated
		if d.Action != ActionCancel || d.Reason != ReasonDeadlineExceeded || d.Next.Status != StatusCancelled ||
			d.RefundRequired != c.refund || d.ReleaseRequired != c.release {
			t.Fatalf("%+v: got %+v", c.j, d)
		}
	}
}

func Test_shouldNotCancelOnTheDeadlineWhenTheOrderIsCompleteOrDecided(t *testing.T) {
	// GIVEN: an order held back by an outage, a shipped and a cancelled order
	for _, s := range []Status{StatusReady, StatusShipped, StatusCancelled} {
		// WHEN: their deadline passes
		d := OnDeadline(Join{Payment: HalfOK, Stock: HalfOK, Status: s})

		// THEN: nothing changes
		if d.Action != ActionNone || d.Next.Status != s {
			t.Fatalf("%s: got %+v", s, d)
		}
	}
}

func Test_shouldIgnoreLateInputsWhenTheOrderIsAlreadyTerminal(t *testing.T) {
	// GIVEN: a shipped and a cancelled order
	for _, j := range []Join{
		{Payment: HalfOK, Stock: HalfOK, Status: StatusShipped},
		{Payment: HalfFailed, Status: StatusCancelled},
	} {
		for _, a := range []arrival{{true, true}, {true, false}, {false, true}, {false, false}} {
			// WHEN: any payment or stock outcome arrives afterwards
			d := apply(j, a, normal)

			// THEN: it is a no-op
			if d.Action != ActionNone || d.Next != j {
				t.Fatalf("%+v after %+v: got %+v", a, j, d)
			}
		}
	}
}

func Test_shouldKeepTheFirstOutcomeWhenTheSameHalfArrivesTwice(t *testing.T) {
	// GIVEN: an open order whose payment already succeeded
	j := Join{Payment: HalfOK, Status: StatusOpen}

	// WHEN: a contradicting payment failure with another event id arrives
	d := OnPayment(j, false, normal)

	// THEN: the first outcome stands and nothing is published
	if d.Action != ActionNone || d.Next != j {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldDescribeTheSameParcelWhenTwoPodsShipTheSameOrder(t *testing.T) {
	// GIVEN: an order leaving warehouse WH-NORTH
	// WHEN: two pods create its shipment independently
	a, b := NewShipment("o-1", "WH-NORTH"), NewShipment("o-1", "WH-NORTH")

	// THEN: carrier and tracking code are identical and the code names the carrier
	if a != b || !strings.HasPrefix(a.TrackingCode, strings.ToUpper(a.Carrier[:2])+"-") {
		t.Fatalf("got %+v and %+v", a, b)
	}
}

func Test_shouldUseEveryCarrierWhenManyOrdersAreShipped(t *testing.T) {
	// GIVEN: a thousand orders from one warehouse
	seen := map[string]int{}

	// WHEN: their shipments are created
	for i := range 1000 {
		seen[NewShipment("o-"+strconv.Itoa(i), "WH-1").Carrier]++
	}

	// THEN: all four carriers get parcels
	for _, c := range Carriers {
		if seen[c] == 0 {
			t.Fatalf("carrier %s never used: %v", c, seen)
		}
	}
}
