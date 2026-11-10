package domain

import "testing"

func ptr(f float64) *float64 { return &f }

func Test_shouldChargeTheCustomerWhenACardPaymentIsAuthorised(t *testing.T) {
	// GIVEN: a new 49.90 EUR card order and a healthy payment provider
	o := Order{AmountCents: 4990, Method: MethodCard}

	// WHEN: payment sees the order with an unlucky-free roll
	d := OnOrderCreated(StateNone, o, Policy{}, 0.5)

	// THEN: the customer is charged
	if d.Next != StateCharged || d.Action != ActionCharged {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldDeclineThePaymentWhenTheProviderRejectsTheCard(t *testing.T) {
	// GIVEN: a card order that falls into the provider's 3% decline rate
	o := Order{AmountCents: 4990, Method: MethodCard}

	// WHEN: payment authorises it
	d := OnOrderCreated(StateNone, o, Policy{}, 0.02)

	// THEN: the payment fails as declined
	if d.Next != StateDeclined || d.Action != ActionDeclined || d.Reason != ReasonDeclined {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldApplyMethodSpecificDeclineRatesWhenNoScenarioOverridesThem(t *testing.T) {
	// GIVEN: the same 4% roll for every payment method
	cases := map[Method]Action{MethodCard: ActionCharged, MethodInvoice: ActionDeclined, MethodPaypal: ActionCharged, MethodWallet: ActionCharged}
	for m, want := range cases {
		// WHEN: an order with that method is authorised
		d := OnOrderCreated(StateNone, Order{AmountCents: 1000, Method: m}, Policy{}, 0.04)

		// THEN: card (3%) passes, invoice (5%) is declined, PayPal and wallet (1%) pass
		if d.Action != want {
			t.Fatalf("%s: got %+v", m, d)
		}
	}
}

func Test_shouldFailEveryPaymentWhenTheProviderIsDown(t *testing.T) {
	// GIVEN: the "Payment Provider Down" scenario is active
	p := Policy{ProviderDown: true}

	// WHEN: a perfectly fine wallet order arrives
	d := OnOrderCreated(StateNone, Order{AmountCents: 100, Method: MethodWallet}, p, 0.99)

	// THEN: it fails because the provider is unavailable
	if d.Action != ActionDeclined || d.Reason != ReasonProviderUnavailable {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldSuspectFraudWhenAnInvoiceOrderExceedsTheLimit(t *testing.T) {
	// GIVEN: a 2,000 EUR order on invoice and the default 1,500 EUR invoice limit
	o := Order{AmountCents: 200_000, Method: MethodInvoice}

	// WHEN: payment authorises it
	d := OnOrderCreated(StateNone, o, Policy{}, 0.99)

	// THEN: it is refused as suspected fraud
	if d.Action != ActionDeclined || d.Reason != ReasonFraudSuspected {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldAcceptLargeInvoicesWhenTheyStayBelowACustomLimit(t *testing.T) {
	// GIVEN: a merchant with a 5,000 EUR invoice limit and a 2,000 EUR invoice order
	p := Policy{FraudLimitCents: 500_000}

	// WHEN: payment authorises it
	d := OnOrderCreated(StateNone, Order{AmountCents: 200_000, Method: MethodInvoice}, p, 0.99)

	// THEN: it is charged
	if d.Action != ActionCharged {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldDeclineMoreOftenWhenAScenarioRaisesTheFailureProbability(t *testing.T) {
	// GIVEN: a scenario raising the decline probability to 50%
	p := Policy{DeclineProbability: ptr(0.5)}

	// WHEN: an order with a 40% roll is authorised (normally far above the 1% wallet rate)
	d := OnOrderCreated(StateNone, Order{AmountCents: 100, Method: MethodWallet}, p, 0.4)

	// THEN: it is declined
	if d.Action != ActionDeclined {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldNeverChargeWhenTheOrderWasCancelledBeforePaymentSawIt(t *testing.T) {
	// GIVEN: shipping cancelled the order on its deadline while payment was lagging
	// WHEN: the order finally reaches payment
	d := OnOrderCreated(StateCancelledEarly, Order{AmountCents: 100, Method: MethodCard}, Policy{}, 0.9)

	// THEN: the customer is not charged and nothing is published
	if d.Next != StateCancelledEarly || d.Action != ActionNone {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldNotDecideAgainWhenAnAlreadyChargedOrderArrivesAgain(t *testing.T) {
	// GIVEN: an order that is already charged
	// WHEN: a second copy of the order event arrives with a new event id
	d := OnOrderCreated(StateCharged, Order{AmountCents: 100, Method: MethodCard}, Policy{}, 0.0)

	// THEN: the existing decision stands and nothing is published
	if d.Next != StateCharged || d.Action != ActionNone {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldRefundTheCustomerWhenAChargedOrderIsCancelled(t *testing.T) {
	// GIVEN: a charged order that shipping cancelled because the item is out of stock
	// WHEN: payment receives the cancellation
	d := OnOrderCancelled(StateCharged)

	// THEN: the charge is refunded
	if d.Next != StateRefunded || d.Action != ActionRefund {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldRememberTheCancellationWhenItArrivesBeforeTheOrder(t *testing.T) {
	// GIVEN: payment has never seen the order
	// WHEN: its cancellation arrives first
	d := OnOrderCancelled(StateNone)

	// THEN: the order is marked so a late arrival is never charged
	if d.Next != StateCancelledEarly || d.Action != ActionNone {
		t.Fatalf("got %+v", d)
	}
}

func Test_shouldIgnoreTheCancellationWhenThePaymentWasDeclinedOrAlreadyRefunded(t *testing.T) {
	// GIVEN: orders whose payment failed or was refunded already
	for _, s := range []State{StateDeclined, StateRefunded, StateCancelledEarly} {
		// WHEN: a cancellation arrives
		d := OnOrderCancelled(s)

		// THEN: no money moves
		if d.Next != s || d.Action != ActionNone {
			t.Fatalf("%s: got %+v", s, d)
		}
	}
}
