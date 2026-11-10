// Package domain holds the shipping rules: the join of an order's payment and stock outcomes
// into exactly one terminal decision (shipped or cancelled). Pure Go, no infrastructure; see
// the shipping state machine in docs/contracts/events.md.
package domain

import (
	"fmt"
	"hash/fnv"
	"strings"
)

// Half is what shipping knows about one half of the join (payment or stock).
type Half string

const (
	HalfUnknown Half = ""       // not seen yet
	HalfOK      Half = "ok"     // payment captured / stock reserved
	HalfFailed  Half = "failed" // payment failed / stock rejected
)

// Status is the join status of one order.
type Status string

const (
	StatusOpen      Status = "open"      // waiting for the other half
	StatusReady     Status = "ready"     // both halves ok, held back by a warehouse outage
	StatusShipped   Status = "shipped"   // terminal: orders.shipped published
	StatusCancelled Status = "cancelled" // terminal: orders.cancelled published
)

// Terminal reports whether no input can change the order any more.
func (s Status) Terminal() bool { return s == StatusShipped || s == StatusCancelled }

// Reason explains a cancellation (mirror events.CancellationReason).
type Reason string

const (
	ReasonPaymentFailed    Reason = "PAYMENT_FAILED"
	ReasonOutOfStock       Reason = "OUT_OF_STOCK"
	ReasonDeadlineExceeded Reason = "DEADLINE_EXCEEDED"
	// BOTH_FAILED exists in the contract but cannot occur here: shipping cancels on the first
	// failure it sees, so a second failure always finds the order already terminal.
)

// Join is the per-order state shipping keeps.
type Join struct {
	Payment Half
	Stock   Half
	Status  Status
}

// Action is what the service must publish after a decision.
type Action int

const (
	ActionNone   Action = iota // nothing to publish
	ActionShip                 // publish orders.shipped
	ActionCancel               // publish orders.cancelled
)

// Decision is the result of applying an input to the current join.
type Decision struct {
	Next   Join
	Action Action
	// Set for ActionCancel: the reason and which compensations the other services must run.
	Reason          Reason
	RefundRequired  bool
	ReleaseRequired bool
}

// Policy holds the runtime conditions that change decisions.
type Policy struct {
	// WarehouseOutage holds back shipments that would otherwise go out (status ready).
	WarehouseOutage bool
}

// OnPayment applies a payment outcome (payments.completed: ok=true, payments.failed: ok=false).
func OnPayment(j Join, ok bool, p Policy) Decision {
	if j.Status.Terminal() || j.Payment != HalfUnknown {
		return Decision{Next: j}
	}
	j.Payment = half(ok)
	return decide(j, p)
}

// OnStock applies a stock outcome (inventory.reserved: ok=true, inventory.rejected: ok=false).
func OnStock(j Join, ok bool, p Policy) Decision {
	if j.Status.Terminal() || j.Stock != HalfUnknown {
		return Decision{Next: j}
	}
	j.Stock = half(ok)
	return decide(j, p)
}

// OnDeadline cancels an order that is still waiting for one of its halves when its join
// deadline passed. Orders held back by an outage (ready) are complete and keep waiting.
func OnDeadline(j Join) Decision {
	if j.Status != StatusOpen {
		return Decision{Next: j}
	}
	return cancel(j, ReasonDeadlineExceeded)
}

// OnSweep releases an order held back by a warehouse outage once the outage is over.
func OnSweep(j Join, p Policy) Decision {
	if j.Status != StatusReady || p.WarehouseOutage {
		return Decision{Next: j}
	}
	j.Status = StatusShipped
	return Decision{Next: j, Action: ActionShip}
}

func half(ok bool) Half {
	if ok {
		return HalfOK
	}
	return HalfFailed
}

func decide(j Join, p Policy) Decision {
	switch {
	case j.Payment == HalfFailed:
		return cancel(j, ReasonPaymentFailed)
	case j.Stock == HalfFailed:
		return cancel(j, ReasonOutOfStock)
	case j.Payment == HalfOK && j.Stock == HalfOK && p.WarehouseOutage:
		j.Status = StatusReady
		return Decision{Next: j}
	case j.Payment == HalfOK && j.Stock == HalfOK:
		j.Status = StatusShipped
		return Decision{Next: j, Action: ActionShip}
	default:
		j.Status = StatusOpen
		return Decision{Next: j}
	}
}

func cancel(j Join, r Reason) Decision {
	j.Status = StatusCancelled
	return Decision{Next: j, Action: ActionCancel, Reason: r,
		RefundRequired: j.Payment == HalfOK, ReleaseRequired: j.Stock == HalfOK}
}

// Carriers ship orders; each warehouse spreads its parcels over all of them.
var Carriers = []string{"DHL", "DPD", "UPS", "Hermes"}

// Shipment is the parcel created for a shipped order.
type Shipment struct {
	Carrier      string
	TrackingCode string
}

// NewShipment picks the carrier and tracking code for an order leaving warehouse. It is
// deterministic, so two pods deciding the same order describe the same parcel.
func NewShipment(orderID, warehouse string) Shipment {
	h := fnv.New64a()
	_, _ = h.Write([]byte(warehouse + "|" + orderID))
	sum := h.Sum64()
	carrier := Carriers[sum%uint64(len(Carriers))]
	return Shipment{Carrier: carrier, TrackingCode: fmt.Sprintf("%s-%012d", strings.ToUpper(carrier[:2]), (sum>>8)%1_000_000_000_000)}
}
