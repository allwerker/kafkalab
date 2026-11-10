// Package domain holds the analytics rules that need memory across events: an order's value
// is only known from orders.created, but revenue is counted when the order ships, and the two
// topics are consumed independently (either may be processed first). It also recognises
// business duplicates: the same order created twice under different event ids. Pure Go.
package domain

import "time"

// Outcome is the terminal state of an order as analytics saw it.
type Outcome string

const (
	OutcomeNone      Outcome = ""
	OutcomeShipped   Outcome = "shipped"
	OutcomeCancelled Outcome = "cancelled"
)

// Order is what analytics remembers about one order.
type Order struct {
	OrderID string
	// Created is true once orders.created was counted; TotalCents and Region come from it.
	Created    bool
	TotalCents int64
	Region     string
	Outcome    Outcome
}

// Effects says what an event means for the business metrics.
type Effects struct {
	Next Order
	// Changed: Next must be stored.
	Changed bool
	// Duplicate: the event repeats something already counted; it must not be counted again.
	Duplicate bool
	// RevenueCents of a shipped order that became countable with this event (region Next.Region).
	RevenueCents int64
}

// OnCreated applies orders.created. stored is the order's row after the store inserted c if the
// order was unknown (fresh=true).
func OnCreated(stored Order, fresh bool, c Order) Effects {
	if fresh {
		return Effects{Next: stored}
	}
	if stored.Created {
		return Effects{Next: stored, Duplicate: true}
	}
	// The order's outcome was processed first: now its value is known.
	next := stored
	next.Created, next.TotalCents, next.Region = true, c.TotalCents, c.Region
	e := Effects{Next: next, Changed: true}
	if next.Outcome == OutcomeShipped {
		e.RevenueCents = next.TotalCents
	}
	return e
}

// OnOutcome applies orders.shipped or orders.cancelled. stored is the order's row after the
// store inserted the outcome if the order was unknown (fresh=true).
func OnOutcome(stored Order, fresh bool, outcome Outcome) Effects {
	if fresh {
		// The value is not known yet; revenue is counted when orders.created arrives.
		return Effects{Next: stored}
	}
	if stored.Outcome != OutcomeNone {
		return Effects{Next: stored, Duplicate: true}
	}
	next := stored
	next.Outcome = outcome
	e := Effects{Next: next, Changed: true}
	if outcome == OutcomeShipped && next.Created {
		e.RevenueCents = next.TotalCents
	}
	return e
}

// Retention of order rows: an hour after the outcome (late duplicates still get recognised),
// a day for orders that never reached an outcome.
const (
	KeepAfterOutcome = time.Hour
	KeepWithout      = 24 * time.Hour
)

// ForgetAfter returns when the row of o may be deleted, given it was last changed at now.
func ForgetAfter(o Order, now time.Time) time.Time {
	if o.Outcome != OutcomeNone {
		return now.Add(KeepAfterOutcome)
	}
	return now.Add(KeepWithout)
}

// CompletionSeconds is the end-to-end time of an order, never negative (clock skew).
func CompletionSeconds(orderCreatedAt, now time.Time) float64 {
	return max(now.Sub(orderCreatedAt).Seconds(), 0)
}
