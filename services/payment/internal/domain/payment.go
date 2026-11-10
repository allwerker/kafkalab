// Package domain holds the payment rules: when an order is charged or declined and when a
// charge is refunded. Pure Go, no infrastructure; see docs/contracts/events.md.
package domain

// State is the payment state of one order.
type State string

const (
	StateNone           State = ""                // never seen
	StateCharged        State = "charged"         // money captured
	StateDeclined       State = "declined"        // authorisation failed
	StateRefunded       State = "refunded"        // charged, then refunded after cancellation
	StateCancelledEarly State = "cancelled_early" // cancellation arrived before the order
)

// Method is the payment method chosen by the customer.
type Method string

// Payment methods (mirror events.PaymentMethod).
const (
	MethodCard    Method = "CARD"
	MethodPaypal  Method = "PAYPAL"
	MethodInvoice Method = "INVOICE"
	MethodWallet  Method = "WALLET"
)

// Reason explains a declined payment (mirror events.PaymentFailureReason).
type Reason string

const (
	ReasonDeclined            Reason = "DECLINED"
	ReasonProviderUnavailable Reason = "PROVIDER_UNAVAILABLE"
	ReasonFraudSuspected      Reason = "FRAUD_SUSPECTED"
)

// Action is what the service must do after applying an input.
type Action int

const (
	ActionNone     Action = iota // nothing to publish
	ActionCharged                // publish payments.completed
	ActionDeclined               // publish payments.failed
	ActionRefund                 // publish payments.refunded
)

// Policy holds the authorisation rules that scenarios can change.
type Policy struct {
	// DeclineProbability overrides the per-method decline rates when set (businessFailureProbability knob).
	DeclineProbability *float64
	// ProviderDown makes every authorisation fail (Payment Provider Down scenario).
	ProviderDown bool
	// FraudLimitCents: invoice orders above this amount are refused as suspected fraud.
	FraudLimitCents int64
}

// DefaultFraudLimitCents is the invoice limit (1,500.00 EUR).
const DefaultFraudLimitCents = 150_000

// baseDeclineRate is the share of authorisations a payment provider declines per method.
var baseDeclineRate = map[Method]float64{
	MethodCard:    0.03,
	MethodPaypal:  0.01,
	MethodInvoice: 0.05,
	MethodWallet:  0.01,
}

// Order is what payment needs to know about an order.
type Order struct {
	AmountCents int64
	Method      Method
}

// Decision is the result of applying an input to the current state.
type Decision struct {
	Next   State
	Action Action
	Reason Reason // set for ActionDeclined
}

// OnOrderCreated decides about a new order. roll is a uniform number in [0,1) that is stable
// for the order (derived from its event id), so a redelivery always gets the same decision.
func OnOrderCreated(current State, o Order, p Policy, roll float64) Decision {
	switch current {
	case StateCancelledEarly:
		// The order was cancelled before we saw it: never charge.
		return Decision{Next: current, Action: ActionNone}
	case StateNone:
		return authorise(o, p, roll)
	default:
		// Already decided (redelivery after a state change): nothing new to say.
		return Decision{Next: current, Action: ActionNone}
	}
}

func authorise(o Order, p Policy, roll float64) Decision {
	limit := p.FraudLimitCents
	if limit == 0 {
		limit = DefaultFraudLimitCents
	}
	switch {
	case p.ProviderDown:
		return Decision{Next: StateDeclined, Action: ActionDeclined, Reason: ReasonProviderUnavailable}
	case o.Method == MethodInvoice && o.AmountCents > limit:
		return Decision{Next: StateDeclined, Action: ActionDeclined, Reason: ReasonFraudSuspected}
	case roll < declineRate(o.Method, p):
		return Decision{Next: StateDeclined, Action: ActionDeclined, Reason: ReasonDeclined}
	default:
		return Decision{Next: StateCharged, Action: ActionCharged}
	}
}

func declineRate(m Method, p Policy) float64 {
	if p.DeclineProbability != nil {
		return *p.DeclineProbability
	}
	return baseDeclineRate[m]
}

// OnOrderCancelled decides about a cancellation (decided by shipping).
func OnOrderCancelled(current State) Decision {
	switch current {
	case StateCharged:
		return Decision{Next: StateRefunded, Action: ActionRefund}
	case StateNone:
		// Cancellation overtook the order (deadline cancel while payment lagged): remember it.
		return Decision{Next: StateCancelledEarly, Action: ActionNone}
	default:
		return Decision{Next: current, Action: ActionNone}
	}
}
