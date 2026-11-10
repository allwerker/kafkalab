package domain

import (
	"math"
	"time"
)

// Scenario holds the generator knobs of the running experiments (see schemas/platform.avdl).
type Scenario struct {
	// DuplicateProbability: share of orders published twice with the same event id.
	DuplicateProbability float64
	// PoisonProbability: share of records that are undecodable garbage instead of an order.
	PoisonProbability float64
	// HotPartition (if not nil) receives HotPartitionRatio of all orders.
	HotPartition      *int
	HotPartitionRatio float64
}

// Plan is what to do with the next record.
type Plan struct {
	Poison    bool // publish garbage instead of an order
	Hot       bool // the order id must hash onto the hot partition
	Duplicate bool // publish the order a second time
}

// Plan decides how the next record misbehaves. A poison record is never also hot or duplicated.
func (g *Generator) Plan(s Scenario) Plan {
	if g.rnd.Float64() < s.PoisonProbability {
		return Plan{Poison: true}
	}
	return Plan{
		Hot:       s.HotPartition != nil && g.rnd.Float64() < s.HotPartitionRatio,
		Duplicate: g.rnd.Float64() < s.DuplicateProbability,
	}
}

// Padding returns n bytes of incompressible filler, so "Fat Messages" stay fat even when the
// Tuning Lab enables compression.
func (g *Generator) Padding(n int) []byte {
	if n <= 0 {
		return nil
	}
	b := make([]byte, n)
	for i := 0; i < n; i += 8 {
		v := g.rnd.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			b[i+j] = byte(v >> (8 * j))
		}
	}
	return b
}

// Murmur2 is Kafka's default key hash (the Java client's Utils.murmur2, also used by franz-go's
// StickyKeyPartitioner).
func Murmur2(b []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(b))
	for len(b) >= 4 {
		k := uint32(b[3])<<24 | uint32(b[2])<<16 | uint32(b[1])<<8 | uint32(b[0])
		b = b[4:]
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}
	switch len(b) {
	case 3:
		h ^= uint32(b[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(b[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(b[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// Partition returns the partition Kafka's default partitioner chooses for key among n partitions.
func Partition(key string, n int) int {
	return int(Murmur2([]byte(key))&0x7fffffff) % n
}

// KeyOnPartition draws ids from next until one lands on partition p of n. With uniform ids this
// takes n attempts on average. p must be in [0, n).
func KeyOnPartition(next func() string, p, n int) string {
	for {
		if id := next(); Partition(id, n) == p {
			return id
		}
	}
}

// Traffic wave: a compressed "day" (one hour) of ±15% plus a 7-minute ripple of ±5%, so
// dashboards show living traffic instead of a flat line. Together at most ±20%.
const (
	wavePeriod   = time.Hour
	ripplePeriod = 7 * time.Minute
)

// Wave modulates base by the time of day of t.
func Wave(base float64, t time.Time) float64 {
	sec := float64(t.UnixNano()) / 1e9
	day := math.Sin(2 * math.Pi * sec / wavePeriod.Seconds())
	ripple := math.Sin(2 * math.Pi * sec / ripplePeriod.Seconds())
	return base * (1 + 0.15*day + 0.05*ripple)
}
