package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/events"
)

// DefaultMaxPollRecords bounds one poll so a partition batch fits in one short transaction.
const DefaultMaxPollRecords = 500

// TuningOptions translates Tuning Lab settings into client options. Unset values keep the
// lab defaults. It also returns the effective max records per poll and whether the settings
// weaken delivery guarantees (acks other than "all" disable idempotent writes).
func TuningOptions(t events.ClientTuning) (opts []kgo.Opt, maxPollRecords int, lossy bool) {
	maxPollRecords = DefaultMaxPollRecords
	opts = []kgo.Opt{kgo.ProducerLinger(5 * time.Millisecond)}
	if t.Acks != nil {
		switch *t.Acks {
		case "leader":
			opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
			lossy = true
		case "none":
			opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
			lossy = true
		default:
			opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
		}
	}
	if t.LingerMs != nil {
		opts = append(opts, kgo.ProducerLinger(time.Duration(*t.LingerMs)*time.Millisecond))
	}
	if t.BatchMaxBytes != nil {
		opts = append(opts, kgo.ProducerBatchMaxBytes(*t.BatchMaxBytes))
	}
	if t.Compression != nil {
		opts = append(opts, kgo.ProducerBatchCompression(compression(*t.Compression)))
	}
	if t.FetchMaxBytes != nil {
		opts = append(opts, kgo.FetchMaxBytes(*t.FetchMaxBytes))
	}
	if t.FetchMaxWaitMs != nil {
		opts = append(opts, kgo.FetchMaxWait(time.Duration(*t.FetchMaxWaitMs)*time.Millisecond))
	}
	if t.FetchMinBytes != nil {
		opts = append(opts, kgo.FetchMinBytes(*t.FetchMinBytes))
	}
	if t.MaxPollRecords != nil && *t.MaxPollRecords > 0 {
		maxPollRecords = int(*t.MaxPollRecords)
	}
	return opts, maxPollRecords, lossy
}

func compression(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.NoCompression()
	}
}

// BaseOptions are the options every lab client gets.
func BaseOptions(brokers []string, clientID string, hooks ...kgo.Hook) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(clientID),
		kgo.WithHooks(hooks...),
		kgo.RetryBackoffFn(retryBackoff),
		// Keep client memory bounded during catch-up after an outage (Recovery Storm): with 64 MiB
		// pods, larger fetch buffers (franz-go defaults: 50 MiB / 1 MiB per partition, several
		// concurrent fetches) OOM-kill a consumer that has a big backlog.
		kgo.FetchMaxBytes(2 << 20),
		kgo.FetchMaxPartitionBytes(256 << 10),
		kgo.MaxConcurrentFetches(2),
		kgo.MaxBufferedRecords(20_000),
		kgo.MetadataMaxAge(30 * time.Second),
	}
}

// SameTuning reports whether two tuning settings are equal (a change triggers a client rebuild).
func SameTuning(a, b events.ClientTuning) bool {
	return eqS(a.Acks, b.Acks) && eqI(a.LingerMs, b.LingerMs) && eqI(a.BatchMaxBytes, b.BatchMaxBytes) &&
		eqS(a.Compression, b.Compression) && eqI(a.FetchMaxBytes, b.FetchMaxBytes) &&
		eqI(a.FetchMaxWaitMs, b.FetchMaxWaitMs) && eqI(a.FetchMinBytes, b.FetchMinBytes) &&
		eqI(a.MaxPollRecords, b.MaxPollRecords)
}

func eqS(a, b *string) bool { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }
func eqI(a, b *int32) bool  { return (a == nil && b == nil) || (a != nil && b != nil && *a == *b) }

func retryBackoff(tries int) time.Duration {
	return min(time.Duration(tries)*100*time.Millisecond, 2*time.Second)
}
