package observe

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"kafkalab/pkg/contracts"
)

// GroupTopics is who consumes what (docs/contracts/events.md); consumer group = service name.
var GroupTopics = map[string][]string{
	contracts.ServicePayment:   {contracts.TopicOrdersCreated, contracts.TopicOrdersCancelled},
	contracts.ServiceInventory: {contracts.TopicOrdersCreated, contracts.TopicOrdersCancelled},
	contracts.ServiceShipping: {contracts.TopicPaymentsCompleted, contracts.TopicPaymentsFailed,
		contracts.TopicInventoryReserved, contracts.TopicInventoryRejected},
	contracts.ServiceNotification: {contracts.TopicOrdersCreated, contracts.TopicOrdersShipped,
		contracts.TopicOrdersCancelled, contracts.TopicPaymentsRefunded},
	contracts.ServiceAnalytics: append(slices.Clone(contracts.BusinessTopics), contracts.TopicOrdersDLQ),
}

// Lag is the consumer lag of the five groups.
type Lag struct {
	// ByGroup[group][topic][p] is the lag of partition p (0 where nothing was committed yet).
	ByGroup map[string]map[string][]int64
	// Partitions is the partition count per topic.
	Partitions map[string]int
	Total      int64
}

// AdminLag computes lag from committed offsets and end offsets (cheap for five groups, and
// works without KMinion).
type AdminLag struct {
	Adm *kadm.Client
}

// Lag fetches end offsets once and the committed offsets of every group.
func (a AdminLag) Lag(ctx context.Context) (Lag, error) {
	topics := append(slices.Clone(contracts.BusinessTopics), contracts.TopicOrdersDLQ)
	ends, err := a.Adm.ListEndOffsets(ctx, topics...)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return Lag{}, fmt.Errorf("end offsets: %w", err)
	}
	l := Lag{ByGroup: map[string]map[string][]int64{}, Partitions: map[string]int{}}
	ends.Each(func(o kadm.ListedOffset) { l.Partitions[o.Topic] = max(l.Partitions[o.Topic], int(o.Partition)+1) })
	for _, group := range slices.Sorted(maps.Keys(GroupTopics)) {
		committed, err := a.Adm.FetchOffsets(ctx, group)
		if err == nil {
			err = committed.Error()
		}
		if errors.Is(err, kerr.GroupIDNotFound) {
			err = nil // the service never ran: nothing committed, no lag
		}
		if err != nil {
			return Lag{}, fmt.Errorf("committed offsets of %s: %w", group, err)
		}
		byTopic := map[string][]int64{}
		for _, topic := range GroupTopics[group] {
			lags := make([]int64, l.Partitions[topic])
			for p := range lags {
				c, ok := committed.Lookup(topic, int32(p))
				end, _ := ends.Lookup(topic, int32(p))
				if ok && c.At >= 0 {
					lags[p] = max(end.Offset-c.At, 0)
					l.Total += lags[p]
				}
			}
			byTopic[topic] = lags
		}
		l.ByGroup[group] = byTopic
	}
	return l, nil
}
