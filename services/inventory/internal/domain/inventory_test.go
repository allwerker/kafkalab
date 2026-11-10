package domain

import (
	"reflect"
	"testing"
)

func Test_shouldMergeAndSortLinesWhenAnOrderContainsTheSameProductTwice(t *testing.T) {
	// GIVEN: an order with sku-007 twice and sku-002 once
	lines := []Line{{"sku-007", 1}, {"sku-002", 3}, {"sku-007", 2}}

	// WHEN: its demand is computed
	got, err := Demand(lines)

	// THEN: sku-007 is needed three times, and products are in SKU (= lock) order
	want := []Line{{"sku-002", 3}, {"sku-007", 3}}
	if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(SKUs(got), []string{"sku-002", "sku-007"}) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func Test_shouldRefuseTheOrderWhenALineHasNoPositiveQuantity(t *testing.T) {
	// GIVEN: a corrupt order with a zero quantity, and an order without lines
	// WHEN: their demand is computed
	_, errZero := Demand([]Line{{"sku-001", 1}, {"sku-002", 0}})
	_, errEmpty := Demand(nil)

	// THEN: both are invalid
	if errZero == nil || errEmpty == nil {
		t.Fatalf("got %v / %v", errZero, errEmpty)
	}
}

func Test_shouldReserveEveryLineWhenAllProductsAreOnStock(t *testing.T) {
	// GIVEN: a new order for 2x sku-001 and 1x sku-005 with plenty of stock
	demand := []Line{{"sku-001", 2}, {"sku-005", 1}}

	// WHEN: inventory decides
	d := OnOrderCreated(StateNone, demand, map[string]int{"sku-001": 2, "sku-005": 40})

	// THEN: the order is reserved
	if d != (Decision{Next: StateReserved, Action: ActionReserve}) {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldRejectWithTheFirstShortProductWhenStockIsMissing(t *testing.T) {
	// GIVEN: an order whose second and third products are both sold out
	demand := []Line{{"sku-001", 1}, {"sku-002", 5}, {"sku-003", 1}}

	// WHEN: inventory decides
	d := OnOrderCreated(StateNone, demand, map[string]int{"sku-001": 9, "sku-002": 4, "sku-003": 0})

	// THEN: it is rejected as out of stock, naming sku-002, and nothing is reserved
	if d != (Decision{Next: StateRejected, Action: ActionReject, Reason: ReasonOutOfStock, SKU: "sku-002"}) {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldRejectAsUnknownWhenAProductIsNotInTheCatalogue(t *testing.T) {
	// GIVEN: a flash sale order for a product the warehouse does not carry
	demand := []Line{{"sku-999", 1}}

	// WHEN: inventory decides
	d := OnOrderCreated(StateNone, demand, map[string]int{})

	// THEN: it is rejected with UNKNOWN_SKU
	if d.Action != ActionReject || d.Reason != ReasonUnknownSKU || d.SKU != "sku-999" {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldNeverReserveWhenTheOrderWasCancelledBeforeItArrived(t *testing.T) {
	// GIVEN: an order whose cancellation came first
	// WHEN: the order itself arrives
	d := OnOrderCreated(StateCancelledEarly, []Line{{"sku-001", 1}}, nil)

	// THEN: no stock is taken and nothing is published
	if d != (Decision{Next: StateCancelledEarly, Action: ActionNone}) || NeedsStock(StateCancelledEarly) {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldKeepTheDecisionWhenTheSameOrderArrivesAgain(t *testing.T) {
	// GIVEN: orders that were already reserved or rejected
	for _, s := range []State{StateReserved, StateRejected, StateReleased} {
		// WHEN: a copy of the order arrives
		d := OnOrderCreated(s, []Line{{"sku-001", 1}}, nil)

		// THEN: nothing changes
		if d != (Decision{Next: s, Action: ActionNone}) {
			t.Fatalf("%s: got %+v", s, d)
		}
	}
}

func Test_shouldReleaseTheStockWhenAReservedOrderIsCancelled(t *testing.T) {
	// GIVEN: a reserved order
	// WHEN: shipping cancels it
	d := OnOrderCancelled(StateReserved)

	// THEN: the reservation is released
	if d != (Decision{Next: StateReleased, Action: ActionRelease}) {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldRememberTheCancellationWhenItArrivesBeforeTheOrder(t *testing.T) {
	// GIVEN: an order inventory has never seen
	// WHEN: its cancellation arrives
	d := OnOrderCancelled(StateNone)

	// THEN: the order is marked cancelled early and nothing is published
	if d != (Decision{Next: StateCancelledEarly, Action: ActionNone}) {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldIgnoreTheCancellationWhenNothingWasReserved(t *testing.T) {
	// GIVEN: orders that were rejected, already released or already cancelled early
	for _, s := range []State{StateRejected, StateReleased, StateCancelledEarly} {
		// WHEN: a cancellation arrives
		d := OnOrderCancelled(s)

		// THEN: nothing is released
		if d != (Decision{Next: s, Action: ActionNone}) {
			t.Fatalf("%s: got %+v", s, d)
		}
	}
}

func Test_shouldShipFromTheRegionalWarehouseWhenARegionIsGiven(t *testing.T) {
	// GIVEN: the four customer regions
	want := map[string]string{"NORTH": "hamburg", "SOUTH": "munich", "EAST": "leipzig", "WEST": "cologne"}

	// WHEN / THEN: each is served by its warehouse
	for region, wh := range want {
		if got := Warehouse(region); got != wh {
			t.Fatalf("%s: got %s", region, got)
		}
	}
}

func Test_shouldGiveBestsellersLargerShelvesWhenCapacityIsPlanned(t *testing.T) {
	// GIVEN: products ranked by popularity
	// WHEN: their capacities are planned
	got := []int{Capacity(1), Capacity(2), Capacity(3), Capacity(5), Capacity(10), Capacity(40)}

	// THEN: the bestseller gets 3,000 units, niche products the minimum of 200
	if !reflect.DeepEqual(got, []int{3000, 1500, 1000, 600, 300, 200}) {
		t.Fatalf("got %v", got)
	}
}
