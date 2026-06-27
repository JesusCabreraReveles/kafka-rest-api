package kafka

import (
	"context"
	"fmt"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// SyncProducer publishes via the low-level kafka.Client.Produce API, which
// returns the base offset per partition. It groups messages by their hashed
// partition and reports each record's exact (partition, offset). This is the
// "sync" produce mode: lower throughput than the batched Writer, but it gives
// callers per-record offsets. See ADR 0001.
type SyncProducer struct {
	client   *segkafka.Client
	acks     segkafka.RequiredAcks
	balancer segkafka.Balancer
}

// SyncProducerConfig configures a SyncProducer.
type SyncProducerConfig struct {
	Brokers      []string
	WriteTimeout time.Duration
	RequiredAcks string // all | one | none
	Security     SecurityConfig
}

// NewSyncProducer constructs a SyncProducer.
func NewSyncProducer(cfg SyncProducerConfig) (*SyncProducer, error) {
	acks, err := parseRequiredAcks(cfg.RequiredAcks)
	if err != nil {
		return nil, err
	}
	transport, err := cfg.Security.transport()
	if err != nil {
		return nil, fmt.Errorf("sync producer security: %w", err)
	}

	client := &segkafka.Client{
		Addr:    segkafka.TCP(cfg.Brokers...),
		Timeout: cfg.WriteTimeout,
	}
	if transport != nil {
		client.Transport = transport
	}

	return &SyncProducer{client: client, acks: acks, balancer: &segkafka.Hash{}}, nil
}

// indexedRecord keeps a record's original position so results can be returned
// in input order after grouping by partition.
type indexedRecord struct {
	index  int
	record segkafka.Record
}

// Produce writes msgs and returns each record's (partition, offset) in input
// order. Messages are routed to partitions by key hash, then produced in one
// request per partition.
func (p *SyncProducer) Produce(ctx context.Context, topic string, msgs ...service.Message) ([]service.PublishResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	partitions, err := p.partitions(ctx, topic)
	if err != nil {
		return nil, err
	}

	groups := make(map[int][]indexedRecord)
	for i, m := range msgs {
		// The balancer hashes the key to choose a partition.
		part := p.balancer.Balance(segkafka.Message{Key: m.Key, Value: m.Value}, partitions...)
		groups[part] = append(groups[part], indexedRecord{
			index: i,
			record: segkafka.Record{
				Key:     segkafka.NewBytes(m.Key),
				Value:   segkafka.NewBytes(m.Value),
				Headers: toKafkaHeaders(m.Headers),
			},
		})
	}

	results := make([]service.PublishResult, len(msgs))
	for part, items := range groups {
		records := make([]segkafka.Record, len(items))
		for j, it := range items {
			records[j] = it.record
		}

		resp, err := p.client.Produce(ctx, &segkafka.ProduceRequest{
			Topic:        topic,
			Partition:    part,
			RequiredAcks: p.acks,
			Records:      segkafka.NewRecordReader(records...),
		})
		if err != nil {
			return nil, p.translate(err, topic, part)
		}
		if resp.Error != nil {
			return nil, p.translate(resp.Error, topic, part)
		}

		// The broker assigns sequential offsets starting at BaseOffset.
		for j, it := range items {
			results[it.index] = service.PublishResult{
				Partition: part,
				Offset:    resp.BaseOffset + int64(j),
			}
		}
	}

	return results, nil
}

// partitions returns the partition IDs of topic, mapping an unknown topic to the
// domain error.
func (p *SyncProducer) partitions(ctx context.Context, topic string) ([]int, error) {
	resp, err := p.client.Metadata(ctx, &segkafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return nil, fmt.Errorf("metadata for %q: %w", topic, err)
	}
	for _, t := range resp.Topics {
		if t.Name != topic {
			continue
		}
		if containsKafkaError(t.Error, segkafka.UnknownTopicOrPartition) || len(t.Partitions) == 0 {
			return nil, service.ErrTopicNotFound
		}
		if t.Error != nil {
			return nil, fmt.Errorf("metadata for %q: %w", topic, t.Error)
		}
		ids := make([]int, len(t.Partitions))
		for i, part := range t.Partitions {
			ids[i] = part.ID
		}
		return ids, nil
	}
	return nil, service.ErrTopicNotFound
}

func (p *SyncProducer) translate(err error, topic string, partition int) error {
	if containsKafkaError(err, segkafka.UnknownTopicOrPartition) {
		return service.ErrTopicNotFound
	}
	return fmt.Errorf("kafka produce to %q/%d: %w", topic, partition, err)
}

// Close releases idle connections held by the underlying transport.
func (p *SyncProducer) Close() error {
	if t, ok := p.client.Transport.(*segkafka.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}
