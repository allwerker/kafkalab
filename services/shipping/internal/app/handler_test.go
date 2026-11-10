package app

import (
	"context"
	"testing"

	"kafkalab/pkg/contracts"
	"kafkalab/pkg/events"
	"kafkalab/pkg/kit/runner"
	"kafkalab/services/shipping/internal/domain"
)

func Test_shouldShipTheOrderWhenItsStockIsReservedAfterThePayment(t *testing.T) {
	// GIVEN: an order whose payment was captured
	w := newWorld()
	if outs := w.handle(t, paid("o-1")); outs != nil {
		t.Fatalf("the first half alone must publish nothing, got %+v", outs)
	}

	// WHEN: its stock is reserved in warehouse WH-NORTH
	res := reserved("o-1")
	outs := w.handle(t, res)

	// THEN: orders.shipped goes out once, from WH-NORTH, caused by the reservation, with an id
	// that only depends on the order
	s := outs[0].Value.(*events.OrderShipped)
	parcel := domain.NewShipment("o-1", "WH-NORTH")
	if len(outs) != 1 || outs[0].Topic != contracts.TopicOrdersShipped || outs[0].Key != "o-1" ||
		s.Meta.EventID != contracts.DeriveEventID("o-1", "shipped") || s.Meta.CausationID != res.Meta.EventID ||
		s.Meta.Producer != contracts.ServiceShipping || !s.Meta.OrderCreatedAt.Equal(res.Meta.OrderCreatedAt) ||
		!s.Meta.OccurredAt.Equal(t0) || s.Warehouse != "WH-NORTH" || s.Carrier != parcel.Carrier ||
		s.TrackingCode != parcel.TrackingCode || s.ShipmentID != "shp-"+s.Meta.EventID[:13] {
		t.Fatalf("unexpected %+v", s)
	}
	stored := w.repo.rows["o-1"]
	if stored.Join.Status != domain.StatusShipped || stored.PaymentID != "pay-o-1" || stored.ReservationID != "res-o-1" {
		t.Fatalf("stored %+v", stored)
	}
}

func Test_shouldPublishTheSameShipmentWhenTheHalvesArriveInTheOppositeOrder(t *testing.T) {
	// GIVEN: two shipping pods, one sees payment first, the other stock first
	a, b := newWorld(), newWorld()

	// WHEN: each completes the join
	a.handle(t, paid("o-2"))
	viaStock := a.handle(t, reserved("o-2"))[0].Value.(*events.OrderShipped)
	b.handle(t, reserved("o-2"))
	viaPayment := b.handle(t, paid("o-2"))[0].Value.(*events.OrderShipped)

	// THEN: both describe the same event and parcel, so downstream deduplicates either copy
	if viaStock.Meta.EventID != viaPayment.Meta.EventID || viaStock.Carrier != viaPayment.Carrier ||
		viaStock.TrackingCode != viaPayment.TrackingCode || viaPayment.Warehouse != "WH-NORTH" {
		t.Fatalf("got %+v and %+v", viaStock, viaPayment)
	}
}

func Test_shouldCancelAndAskForAReleaseWhenThePaymentFailsAfterStockWasReserved(t *testing.T) {
	// GIVEN: an order whose stock is already reserved
	w := newWorld()
	w.handle(t, reserved("o-3"))

	// WHEN: its payment is declined
	fail := declined("o-3")
	outs := w.handle(t, fail)

	// THEN: orders.cancelled (PAYMENT_FAILED) asks inventory to release, not payment to refund
	c := outs[0].Value.(*events.OrderCancelled)
	if outs[0].Topic != contracts.TopicOrdersCancelled || c.Reason != events.CancellationReasonPaymentFailed ||
		c.RefundRequired || !c.ReleaseRequired || c.Meta.EventID != contracts.DeriveEventID("o-3", "cancelled") ||
		c.Meta.CausationID != fail.Meta.EventID {
		t.Fatalf("unexpected %+v", c)
	}
}

func Test_shouldCancelImmediatelyWhenStockIsRejectedBeforeThePaymentIsKnown(t *testing.T) {
	// GIVEN: a new order
	w := newWorld()

	// WHEN: inventory rejects it for lack of stock
	outs := w.handle(t, rejected("o-4"))

	// THEN: it is cancelled as OUT_OF_STOCK without waiting for the payment
	c := outs[0].Value.(*events.OrderCancelled)
	if c.Reason != events.CancellationReasonOutOfStock || c.RefundRequired || c.ReleaseRequired {
		t.Fatalf("unexpected %+v", c)
	}
}

func Test_shouldPublishNothingWhenThePaymentArrivesAfterTheOrderWasCancelled(t *testing.T) {
	// GIVEN: an order cancelled because its stock was rejected
	w := newWorld()
	w.handle(t, rejected("o-5"))
	before := w.repo.rows["o-5"]

	// WHEN: its captured payment arrives late
	outs := w.handle(t, paid("o-5"))

	// THEN: nothing is published and the stored decision is untouched (payment refunds on its own
	// when it sees the cancellation)
	if outs != nil || w.repo.rows["o-5"] != before {
		t.Fatalf("outs=%+v row=%+v", outs, w.repo.rows["o-5"])
	}
}

func Test_shouldHoldTheOrderAsReadyWhenTheWarehouseIsDown(t *testing.T) {
	// GIVEN: a warehouse outage and a paid order
	w := newWorld()
	w.knobs.WarehouseOutage = true
	w.handle(t, paid("o-6"))

	// WHEN: its stock is reserved
	res := reserved("o-6")
	outs := w.handle(t, res)

	// THEN: nothing ships yet; the order waits as ready, remembering what completed it
	row := w.repo.rows["o-6"]
	if outs != nil || row.Join.Status != domain.StatusReady || row.CauseEventID != res.Meta.EventID || row.Warehouse != "WH-NORTH" {
		t.Fatalf("outs=%+v row=%+v", outs, row)
	}
}

func Test_shouldRejectEventsShippingDoesNotHandleWhenTheyArrive(t *testing.T) {
	// GIVEN: an orders.created event delivered to shipping by mistake
	// WHEN: shipping handles it
	_, err := newWorld().handler.Handle(context.Background(), nil, runner.Input{Value: &events.OrderCreated{}})

	// THEN: it is rejected permanently (dead-lettered)
	if !runner.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
}

func Test_shouldRetryLaterWhenTheJoinStoreIsUnavailable(t *testing.T) {
	// GIVEN: a database that cannot be reached
	w := newWorld()
	w.repo.failLock = errDown

	// WHEN: a payment arrives
	_, err := w.handler.Handle(context.Background(), nil, runner.Input{Value: paid("o-7")})

	// THEN: the error is transient, so the runner retries the batch
	if err == nil || runner.IsPermanent(err) {
		t.Fatalf("got %v", err)
	}
}

func Test_shouldNotShipWhenTheDecisionCannotBeStored(t *testing.T) {
	// GIVEN: a paid order and a database that starts failing on writes
	w := newWorld()
	w.handle(t, paid("o-8"))
	w.repo.failSave = errDown

	// WHEN: the reservation arrives
	outs, err := w.handler.Handle(context.Background(), nil, runner.Input{Value: reserved("o-8")})

	// THEN: no shipment is published and the input is retried
	if err == nil || outs != nil {
		t.Fatalf("outs=%+v err=%v", outs, err)
	}
}
