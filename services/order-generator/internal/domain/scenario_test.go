package domain

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Test_shouldPublishOnlyRegularOrdersWhenNoScenarioIsActive(t *testing.T) {
	// GIVEN: a generator in normal operation
	g := NewGenerator(11)

	// WHEN: plans for 1,000 records are drawn
	for i := 0; i < 1000; i++ {
		p := g.Plan(Scenario{})

		// THEN: none of them is poison, hot or duplicated
		if p != (Plan{}) {
			t.Fatalf("unexpected plan %+v", p)
		}
	}
}

func Test_shouldMixInTheRequestedSharesWhenPoisonDuplicateAndHotPartitionScenariosRun(t *testing.T) {
	// GIVEN: 10% poison messages, 20% duplicates and 50% of orders on hot partition 3
	g := NewGenerator(12)
	hot := 3
	s := Scenario{PoisonProbability: 0.1, DuplicateProbability: 0.2, HotPartition: &hot, HotPartitionRatio: 0.5}

	// WHEN: plans for 10,000 records are drawn
	var poison, dup, hotN int
	for i := 0; i < 10_000; i++ {
		p := g.Plan(s)
		if p.Poison {
			poison++
			// AND: a poison record is never also hot or duplicated
			if p.Hot || p.Duplicate {
				t.Fatalf("poison plan %+v", p)
			}
		}
		if p.Duplicate {
			dup++
		}
		if p.Hot {
			hotN++
		}
	}

	// THEN: the shares match the knobs (duplicate and hot apply to the 90% regular orders)
	if poison < 900 || poison > 1100 || dup < 1600 || dup > 2000 || hotN < 4200 || hotN > 4800 {
		t.Fatalf("poison=%d duplicate=%d hot=%d", poison, dup, hotN)
	}
}

func Test_shouldNeverPickAHotPartitionWhenOnlyTheRatioIsSet(t *testing.T) {
	// GIVEN: a hot partition ratio without a target partition
	g := NewGenerator(13)

	// WHEN: 1,000 plans are drawn
	for i := 0; i < 1000; i++ {
		// THEN: no order is marked hot
		if g.Plan(Scenario{HotPartitionRatio: 1}).Hot {
			t.Fatal("hot order without a hot partition")
		}
	}
}

func Test_shouldStayFatAfterCompressionWhenPaddingIsGenerated(t *testing.T) {
	// GIVEN: the Fat Messages scenario asking for 10,001 bytes of filler
	g := NewGenerator(14)

	// WHEN: the filler is generated and gzip-compressed like a tuned producer would
	pad := g.Padding(10_001)
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(pad)
	_ = w.Close()

	// THEN: it has exactly the requested size and barely compresses
	if len(pad) != 10_001 || buf.Len() < 9_500 {
		t.Fatalf("len=%d compressed=%d", len(pad), buf.Len())
	}
}

func Test_shouldAddNoFillerWhenThePaddingKnobIsZero(t *testing.T) {
	// GIVEN: normal operation without Fat Messages
	g := NewGenerator(15)

	// WHEN: the filler is requested
	pad := g.Padding(0)

	// THEN: orders carry no padding at all
	if pad != nil {
		t.Fatalf("unexpected padding of %d bytes", len(pad))
	}
}

func Test_shouldChooseTheSamePartitionAsFranzGoWhenAnOrderIdIsHashed(t *testing.T) {
	// GIVEN: franz-go's default key partitioner and keys of every tail length
	part := kgo.StickyKeyPartitioner(nil).ForTopic("orders.created")

	// WHEN / THEN: our pure hash picks the same partition of 6 for each key
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("%08x-order-key-with-enough-characters", i*7919)[:1+i%40]
		want := part.Partition(&kgo.Record{Key: []byte(key)}, 6)
		if got := Partition(key, 6); got != want {
			t.Fatalf("key %q: got partition %d, franz-go %d", key, got, want)
		}
	}
}

func Test_shouldFindAnIdOnTheHotPartitionWhenIdsAreDrawnUntilOneFits(t *testing.T) {
	// GIVEN: a source of sequential order ids
	n := 0
	next := func() string { n++; return fmt.Sprintf("order-%d", n) }

	// WHEN: ids are requested for hot partition 4 of 6
	for i := 0; i < 50; i++ {
		id := KeyOnPartition(next, 4, 6)

		// THEN: every id lands on partition 4
		if Partition(id, 6) != 4 {
			t.Fatalf("%s is not on partition 4", id)
		}
	}
}

func Test_shouldModulateTrafficByAtMostTwentyPercentWhenTheDayWaveIsApplied(t *testing.T) {
	// GIVEN: a base rate of 100 orders per second over two compressed days
	start := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	lo, hi := 1e9, 0.0

	// WHEN: the rate is sampled every ten seconds
	for d := time.Duration(0); d < 2*time.Hour; d += 10 * time.Second {
		r := Wave(100, start.Add(d))
		lo, hi = min(lo, r), max(hi, r)
	}

	// THEN: traffic visibly breathes but stays within 80..120
	if lo < 80 || hi > 120 || hi-lo < 25 {
		t.Fatalf("range %.1f..%.1f", lo, hi)
	}
}
