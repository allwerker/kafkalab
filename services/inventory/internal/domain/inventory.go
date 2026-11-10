// Package domain holds the inventory rules: when an order's lines are reserved or rejected,
// when a reservation is released and when a product is restocked. Pure Go, no infrastructure;
// see docs/contracts/events.md.
package domain

import (
	"fmt"
	"sort"
)

// State is the reservation state of one order.
type State string

const (
	StateNone           State = ""                // never seen
	StateReserved       State = "reserved"        // stock taken for every line
	StateRejected       State = "rejected"        // at least one line could not be served
	StateReleased       State = "released"        // reserved, then returned after cancellation
	StateCancelledEarly State = "cancelled_early" // cancellation arrived before the order
)

// Reason explains a rejection (mirror events.InventoryRejectionReason).
type Reason string

const (
	ReasonOutOfStock Reason = "OUT_OF_STOCK"
	ReasonUnknownSKU Reason = "UNKNOWN_SKU"
)

// Action is what the service must do after applying an input.
type Action int

const (
	ActionNone    Action = iota // nothing to publish
	ActionReserve               // take the stock, publish inventory.reserved
	ActionReject                // publish inventory.rejected
	ActionRelease               // return the stock, publish inventory.released
)

// Line is a quantity of one product.
type Line struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// Decision is the result of applying an input to the current state.
type Decision struct {
	Next   State
	Action Action
	Reason Reason // set for ActionReject
	SKU    string // first SKU that could not be served (ActionReject)
}

// Demand merges the lines of an order per SKU and sorts them by SKU. Stock rows are always
// locked in this order, so two orders touching the same products can never deadlock each other.
// Every quantity must be positive.
func Demand(lines []Line) ([]Line, error) {
	total := map[string]int{}
	for _, l := range lines {
		if l.Quantity <= 0 {
			return nil, fmt.Errorf("line %s has quantity %d", l.SKU, l.Quantity)
		}
		total[l.SKU] += l.Quantity
	}
	if len(total) == 0 {
		return nil, fmt.Errorf("order has no lines")
	}
	out := make([]Line, 0, len(total))
	for sku, q := range total {
		out = append(out, Line{SKU: sku, Quantity: q})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out, nil
}

// SKUs returns the products of lines (in the lines' order).
func SKUs(lines []Line) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.SKU
	}
	return out
}

// NeedsStock reports whether an order arriving in state current must look at the stock.
func NeedsStock(current State) bool { return current == StateNone }

// OnOrderCreated decides about a new order. demand comes from Demand; available holds the
// units on hand per SKU (products missing from it do not exist). Either every line is reserved
// or none is.
func OnOrderCreated(current State, demand []Line, available map[string]int) Decision {
	if !NeedsStock(current) {
		// Cancelled before we saw it, or already decided (redelivery with a new event id).
		return Decision{Next: current, Action: ActionNone}
	}
	for _, l := range demand {
		have, known := available[l.SKU]
		if !known {
			return Decision{Next: StateRejected, Action: ActionReject, Reason: ReasonUnknownSKU, SKU: l.SKU}
		}
		if have < l.Quantity {
			return Decision{Next: StateRejected, Action: ActionReject, Reason: ReasonOutOfStock, SKU: l.SKU}
		}
	}
	return Decision{Next: StateReserved, Action: ActionReserve}
}

// OnOrderCancelled decides about a cancellation (decided by shipping).
func OnOrderCancelled(current State) Decision {
	switch current {
	case StateReserved:
		return Decision{Next: StateReleased, Action: ActionRelease}
	case StateNone:
		// Cancellation overtook the order (deadline cancel while inventory lagged): remember it.
		return Decision{Next: StateCancelledEarly, Action: ActionNone}
	default:
		return Decision{Next: current, Action: ActionNone}
	}
}

// warehouses serve the customer regions (mirror events.Region).
var warehouses = map[string]string{
	"NORTH": "hamburg",
	"SOUTH": "munich",
	"EAST":  "leipzig",
	"WEST":  "cologne",
}

// Warehouse returns the warehouse that ships to region ("" for an unknown region).
func Warehouse(region string) string { return warehouses[region] }

// RestockBelow is the fill level under which a product is refilled to capacity.
const RestockBelow = 0.2

// Capacity is the shelf size of the product with popularity rank (1 = most popular). It follows
// the catalogue's Zipf demand, so at the default order rate no product runs empty between two
// restocks, while a Flash Sale can still drain any of them within seconds.
func Capacity(rank int) int {
	c := (3000/rank + 25) / 50 * 50
	return max(c, 200)
}
