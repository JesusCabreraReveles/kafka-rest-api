package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// readerMaxBytes bounds a single fetch response from the broker.
const readerMaxBytes = 10 << 20 // 10 MiB

// readerMaxWait bounds how long a single fetch blocks at the broker when the
// partition is drained. Keeping it small lets the read loop re-check the
// caller's context promptly, so the consume timeout budget is honored instead
// of being dominated by kafka-go's 10s default.
const readerMaxWait = 500 * time.Millisecond

// Client adapts segmentio/kafka-go's reader and admin APIs to the application's
// MessageReader and TopicAdmin interfaces.
type Client struct {
	brokers []string
	client  *segkafka.Client
	dialer  *segkafka.Dialer // applies SASL/TLS to readers; nil for plaintext
}

// ClientConfig configures a Client.
type ClientConfig struct {
	Brokers      []string
	AdminTimeout time.Duration
	Security     SecurityConfig
}

// NewClient constructs a Client, applying any configured SASL/TLS security.
func NewClient(cfg ClientConfig) (*Client, error) {
	transport, err := cfg.Security.transport()
	if err != nil {
		return nil, fmt.Errorf("kafka client security: %w", err)
	}
	dialer, err := cfg.Security.dialer(cfg.AdminTimeout)
	if err != nil {
		return nil, fmt.Errorf("kafka reader security: %w", err)
	}

	client := &segkafka.Client{
		Addr:    segkafka.TCP(cfg.Brokers...),
		Timeout: cfg.AdminTimeout,
	}
	if transport != nil {
		client.Transport = transport
	}

	return &Client{brokers: cfg.Brokers, client: client, dialer: dialer}, nil
}

// Read scans up to req.Limit messages from a topic partition, starting at an
// absolute offset or — when req.FromTime is set — at the first offset whose
// timestamp is >= that time (enabling time-based replay). A context deadline
// reached while waiting for more messages is treated as a normal end-of-poll:
// the messages read so far are returned without error.
func (c *Client) Read(ctx context.Context, req service.ReadRequest) ([]service.ConsumedMessage, error) {
	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers:   c.brokers,
		Topic:     req.Topic,
		Partition: req.Partition,
		MinBytes:  1,
		MaxBytes:  readerMaxBytes,
		MaxWait:   readerMaxWait,
		Dialer:    c.dialer, // nil is fine: kafka-go falls back to the default dialer
	})
	defer func() { _ = r.Close() }()

	if err := c.seek(ctx, r, req); err != nil {
		return nil, err
	}

	out := make([]service.ConsumedMessage, 0, req.Limit)
	for len(out) < req.Limit {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				break // long-poll budget spent; return what we have
			}
			if containsKafkaError(err, segkafka.UnknownTopicOrPartition) {
				return nil, service.ErrTopicNotFound
			}
			return nil, fmt.Errorf("read from %q/%d: %w", req.Topic, req.Partition, err)
		}
		out = append(out, toConsumedMessage(m))
	}
	return out, nil
}

// seek positions the reader at the requested start: a timestamp takes priority
// over an absolute/sentinel offset.
func (c *Client) seek(ctx context.Context, r *segkafka.Reader, req service.ReadRequest) error {
	if req.FromTime != nil {
		if err := r.SetOffsetAt(ctx, *req.FromTime); err != nil {
			if containsKafkaError(err, segkafka.UnknownTopicOrPartition) {
				return service.ErrTopicNotFound
			}
			return fmt.Errorf("seek to time %s: %w", req.FromTime.Format(time.RFC3339), err)
		}
		return nil
	}
	if err := r.SetOffset(resolveOffset(req.Offset)); err != nil {
		return fmt.Errorf("set offset: %w", err)
	}
	return nil
}

// Name identifies this dependency in health reports.
func (c *Client) Name() string { return "kafka" }

// Check probes broker reachability by requesting cluster metadata. It satisfies
// service.DependencyChecker, backing the readiness portion of /health.
func (c *Client) Check(ctx context.Context) error {
	if _, err := c.client.Metadata(ctx, &segkafka.MetadataRequest{}); err != nil {
		return fmt.Errorf("kafka metadata: %w", err)
	}
	return nil
}

// ListTopics returns the names of non-internal topics, sorted.
func (c *Client) ListTopics(ctx context.Context) ([]string, error) {
	resp, err := c.client.Metadata(ctx, &segkafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("metadata: %w", err)
	}

	names := make([]string, 0, len(resp.Topics))
	for _, t := range resp.Topics {
		if t.Internal || t.Error != nil {
			continue
		}
		names = append(names, t.Name)
	}
	sort.Strings(names)
	return names, nil
}

// DescribeTopic returns metadata and watermarks for a single topic.
func (c *Client) DescribeTopic(ctx context.Context, topic string) (service.TopicMetadata, error) {
	resp, err := c.client.Metadata(ctx, &segkafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return service.TopicMetadata{}, fmt.Errorf("metadata: %w", err)
	}

	var found *segkafka.Topic
	for i := range resp.Topics {
		if resp.Topics[i].Name == topic {
			found = &resp.Topics[i]
			break
		}
	}
	if found == nil || containsKafkaError(found.Error, segkafka.UnknownTopicOrPartition) {
		return service.TopicMetadata{}, service.ErrTopicNotFound
	}
	if found.Error != nil {
		return service.TopicMetadata{}, fmt.Errorf("topic %q metadata: %w", topic, found.Error)
	}

	low, high, err := c.watermarks(ctx, *found)
	if err != nil {
		return service.TopicMetadata{}, err
	}

	return buildTopicMetadata(*found, low, high), nil
}

// watermarks returns the first and last offsets per partition for the topic.
func (c *Client) watermarks(ctx context.Context, t segkafka.Topic) (low, high map[int]int64, err error) {
	reqs := make([]segkafka.OffsetRequest, 0, len(t.Partitions)*2)
	for _, p := range t.Partitions {
		reqs = append(reqs, segkafka.FirstOffsetOf(p.ID), segkafka.LastOffsetOf(p.ID))
	}

	resp, err := c.client.ListOffsets(ctx, &segkafka.ListOffsetsRequest{
		Addr:   c.client.Addr,
		Topics: map[string][]segkafka.OffsetRequest{t.Name: reqs},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list offsets for %q: %w", t.Name, err)
	}

	low = make(map[int]int64, len(t.Partitions))
	high = make(map[int]int64, len(t.Partitions))
	for _, po := range resp.Topics[t.Name] {
		if po.Error != nil {
			return nil, nil, fmt.Errorf("list offsets partition %d: %w", po.Partition, po.Error)
		}
		low[po.Partition] = po.FirstOffset
		high[po.Partition] = po.LastOffset
	}
	return low, high, nil
}

func resolveOffset(offset int64) int64 {
	switch offset {
	case service.OffsetEarliest:
		return segkafka.FirstOffset
	case service.OffsetLatest:
		return segkafka.LastOffset
	default:
		return offset
	}
}

func buildTopicMetadata(t segkafka.Topic, low, high map[int]int64) service.TopicMetadata {
	parts := make([]service.PartitionMetadata, 0, len(t.Partitions))
	replication := 0
	for _, p := range t.Partitions {
		if len(p.Replicas) > replication {
			replication = len(p.Replicas)
		}
		parts = append(parts, service.PartitionMetadata{
			ID:            p.ID,
			Leader:        p.Leader.ID,
			Replicas:      brokerIDs(p.Replicas),
			ISR:           brokerIDs(p.Isr),
			LowWatermark:  low[p.ID],
			HighWatermark: high[p.ID],
		})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].ID < parts[j].ID })

	return service.TopicMetadata{
		Name:              t.Name,
		ReplicationFactor: replication,
		Partitions:        parts,
	}
}

func brokerIDs(brokers []segkafka.Broker) []int {
	ids := make([]int, len(brokers))
	for i, b := range brokers {
		ids[i] = b.ID
	}
	return ids
}

func toConsumedMessage(m segkafka.Message) service.ConsumedMessage {
	var headers []service.Header
	if len(m.Headers) > 0 {
		headers = make([]service.Header, len(m.Headers))
		for i, h := range m.Headers {
			headers[i] = service.Header{Key: h.Key, Value: h.Value}
		}
	}
	return service.ConsumedMessage{
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   headers,
		Timestamp: m.Time,
	}
}
