package testkit

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"kafkalab/pkg/contracts"
)

// Cluster is an in-process Kafka cluster (franz-go kfake) with every lab topic created.
type Cluster struct {
	*kfake.Cluster
	Brokers []string
}

// NewCluster starts a fake cluster with all business and platform topics. Partition counts
// are small and uniform: tests care about behaviour, not about the production layout.
func NewCluster(t testing.TB, opts ...kfake.Opt) *Cluster {
	t.Helper()
	topics := append(append([]string{}, contracts.BusinessTopics...), contracts.TopicOrdersDLQ, contracts.TopicPlatformControl, contracts.TopicPlatformAudit)
	all := append([]kfake.Opt{kfake.NumBrokers(1), kfake.SeedTopics(3, topics...)}, opts...)
	c, err := kfake.NewCluster(all...)
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(c.Close)
	return &Cluster{Cluster: c, Brokers: c.ListenAddrs()}
}
