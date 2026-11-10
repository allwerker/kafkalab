package domain

import (
	"fmt"
	"testing"
)

func Test_shouldReachCustomersMostlyByEmailWhenChannelsAreChosen(t *testing.T) {
	// GIVEN: 10,000 different orders
	counts := map[Channel]int{}

	// WHEN: the channel of each order is chosen
	for i := 0; i < 10_000; i++ {
		counts[ChannelFor(fmt.Sprintf("order-%d", i))]++
	}

	// THEN: about 60% get an e-mail, 25% a push message and 15% an SMS
	if counts[ChannelEmail] < 5700 || counts[ChannelEmail] > 6300 ||
		counts[ChannelPush] < 2250 || counts[ChannelPush] > 2750 ||
		counts[ChannelSMS] < 1300 || counts[ChannelSMS] > 1700 {
		t.Fatalf("unexpected mix %v", counts)
	}
}

func Test_shouldUseTheSameChannelWhenTheSameOrderIsNotifiedAgain(t *testing.T) {
	// GIVEN: an order that was confirmed earlier
	first := ChannelFor("5f0c1e9a-order")

	// WHEN: the customer is notified again about it (shipping, refund, redelivery)
	again := ChannelFor("5f0c1e9a-order")

	// THEN: the same channel is used
	if first != again {
		t.Fatalf("%s vs %s", first, again)
	}
}
