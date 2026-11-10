package contracts

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func Test_shouldDeriveTheSameEventIDWhenTheSameInputIsProcessedTwice(t *testing.T) {
	// GIVEN: an order event that is redelivered after a consumer crash
	cause := "7f0c3c1e-5a1b-4d7e-9a2f-0c7b1e2d3f4a"

	// WHEN: the payment outcome id is derived on the first and on the second delivery
	first := DeriveEventID(cause, "payment")
	second := DeriveEventID(cause, "payment")

	// THEN: both deliveries yield the same id, so downstream services detect the duplicate
	if first != second {
		t.Fatalf("expected stable id, got %s and %s", first, second)
	}
}

func Test_shouldDeriveDifferentEventIDsWhenOneEventCausesDifferentOutcomes(t *testing.T) {
	// GIVEN: one cancelled order that leads to a refund and a stock release
	cause := "order-42"

	// WHEN: the ids for both compensations are derived
	refund := DeriveEventID(cause, "refunded")
	release := DeriveEventID(cause, "released")

	// THEN: the compensations get distinct ids
	if refund == release {
		t.Fatalf("expected distinct ids, both were %s", refund)
	}
}

func Test_shouldFormatDerivedIDsAsVersion8UUIDWhenAnIDIsDerived(t *testing.T) {
	// GIVEN: any causing event
	// WHEN: an outcome id is derived
	id := DeriveEventID("any", "outcome")

	// THEN: it looks like a UUID with version 8 and the RFC variant, so tools display it as one
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Fatalf("not a version 8 UUID: %s", id)
	}
}

func Test_shouldNameSubjectsAfterTheTopicWhenTheTopicNameStrategyIsUsed(t *testing.T) {
	// GIVEN: the orders topic
	// WHEN: its value subject is requested
	subject := Subject(TopicOrdersCreated)

	// THEN: the subject follows the Confluent TopicNameStrategy
	if subject != "orders.created-value" {
		t.Fatalf("unexpected subject %s", subject)
	}
}

func Test_shouldMatchTheDeployedTopicCatalogWhenTopicsAreDeclaredInCode(t *testing.T) {
	// GIVEN: the deployed topic catalog in deploy/topics.yaml
	raw, err := os.ReadFile("../../deploy/topics.yaml")
	if err != nil {
		t.Fatal(err)
	}
	deployed := regexp.MustCompile(`name: ([a-z.]+),`).FindAllStringSubmatch(string(raw), -1)
	var names []string
	for _, m := range deployed {
		names = append(names, m[1])
	}

	// WHEN: it is compared with the topics the code knows about
	known := append(append([]string{}, BusinessTopics...), TopicOrdersDLQ, TopicPlatformControl, TopicPlatformAudit)
	sort.Strings(names)
	sort.Strings(known)

	// THEN: both lists are identical, so no service writes to a topic that does not exist
	if strings.Join(names, ",") != strings.Join(known, ",") {
		t.Fatalf("deployed %v\ncode     %v", names, known)
	}
}

func Test_shouldHaveAValueSchemaWhenATopicCarriesAvroEvents(t *testing.T) {
	// GIVEN: every topic that carries Avro values
	topics := append(append([]string{}, BusinessTopics...), TopicPlatformControl, TopicPlatformAudit)

	// WHEN: its value schema is looked up
	for _, topic := range topics {
		name, ok := ValueTypes[topic]

		// THEN: a generated schema file exists for it, so the registration job can register it
		if !ok {
			t.Fatalf("no value type for %s", topic)
		}
		if _, err := os.Stat("../../schemas/gen/" + name + ".avsc"); err != nil {
			t.Fatalf("schema file for %s: %v", topic, err)
		}
	}
}
