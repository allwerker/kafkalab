package app

import (
	"github.com/twmb/franz-go/pkg/kgo"

	"kafkalab/pkg/contracts"
)

func testRecord(value []byte) *kgo.Record {
	return &kgo.Record{Topic: contracts.TopicOrdersCreated, Key: []byte("o1"), Value: value}
}
