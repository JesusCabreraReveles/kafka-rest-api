// Package kafka adapts the application's use-case interfaces to the
// segmentio/kafka-go client. It is the only package that imports the Kafka
// client, keeping that dependency at the edge of the system.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// Auto-created topics are not immediately writable: the first produce triggers
// creation but may fail until metadata propagates. When auto-creation is
// enabled we retry briefly so the caller's first publish succeeds.
const (
	autoCreateMaxAttempts = 5
	autoCreateBackoff     = 250 * time.Millisecond
)

// WriterConfig configures a Writer.
type WriterConfig struct {
	Brokers         []string
	WriteTimeout    time.Duration
	BatchTimeout    time.Duration
	RequiredAcks    string // all | one | none
	AllowAutoCreate bool   // request broker-side topic auto-creation on publish
	Security        SecurityConfig
}

// Writer is a Kafka producer that satisfies service.Producer.
type Writer struct {
	w               *segkafka.Writer
	allowAutoCreate bool
}

// NewWriter constructs a Writer. It validates the acknowledgement setting and
// configures a synchronous, hash-balanced producer suitable for a
// request/response gateway.
func NewWriter(cfg WriterConfig) (*Writer, error) {
	acks, err := parseRequiredAcks(cfg.RequiredAcks)
	if err != nil {
		return nil, err
	}

	transport, err := cfg.Security.transport()
	if err != nil {
		return nil, fmt.Errorf("kafka writer security: %w", err)
	}

	w := &segkafka.Writer{
		Addr:                   segkafka.TCP(cfg.Brokers...),
		Balancer:               &segkafka.Hash{}, // same key -> same partition
		WriteTimeout:           cfg.WriteTimeout,
		BatchTimeout:           cfg.BatchTimeout,
		RequiredAcks:           acks,
		Async:                  false, // synchronous: errors surface to the HTTP caller
		AllowAutoTopicCreation: cfg.AllowAutoCreate,
	}
	if transport != nil {
		w.Transport = transport
	}

	return &Writer{w: w, allowAutoCreate: cfg.AllowAutoCreate}, nil
}

// Produce writes one or more messages to topic, blocking until the broker
// acknowledges (or the context is canceled). The high-level writer does not
// surface per-record offsets, so it returns a nil result slice — use the "sync"
// produce mode (SyncProducer) when offsets are required. See ADR 0001.
func (wr *Writer) Produce(ctx context.Context, topic string, msgs ...service.Message) ([]service.PublishResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	km := make([]segkafka.Message, len(msgs))
	for i, m := range msgs {
		km[i] = segkafka.Message{
			Topic:   topic,
			Key:     m.Key,
			Value:   m.Value,
			Headers: toKafkaHeaders(m.Headers),
		}
	}

	var err error
	for attempt := 1; attempt <= autoCreateMaxAttempts; attempt++ {
		if err = wr.w.WriteMessages(ctx, km...); err == nil {
			return nil, nil
		}
		if !wr.allowAutoCreate || !containsKafkaError(err, segkafka.UnknownTopicOrPartition) {
			break
		}
		// Topic is being auto-created; wait briefly and retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(autoCreateBackoff):
		}
	}

	if containsKafkaError(err, segkafka.UnknownTopicOrPartition) {
		// Let the caller supply topic context; keep the domain error clean.
		return nil, service.ErrTopicNotFound
	}
	return nil, fmt.Errorf("kafka write to %q: %w", topic, err)
}

// containsKafkaError reports whether err is, or contains, the given Kafka
// protocol error. Batched writes return a kafka.WriteErrors aggregate, which
// errors.Is does not traverse, so this also inspects its elements.
func containsKafkaError(err error, target segkafka.Error) bool {
	if errors.Is(err, target) {
		return true
	}
	var we segkafka.WriteErrors
	if errors.As(err, &we) {
		for _, e := range we {
			if errors.Is(e, target) {
				return true
			}
		}
	}
	return false
}

// Close flushes and releases the underlying writer's resources.
func (wr *Writer) Close() error {
	return wr.w.Close()
}

func toKafkaHeaders(headers []service.Header) []segkafka.Header {
	if len(headers) == 0 {
		return nil
	}
	out := make([]segkafka.Header, len(headers))
	for i, h := range headers {
		out[i] = segkafka.Header{Key: h.Key, Value: h.Value}
	}
	return out
}

func parseRequiredAcks(s string) (segkafka.RequiredAcks, error) {
	switch strings.ToLower(s) {
	case "all", "-1", "":
		return segkafka.RequireAll, nil
	case "one", "1":
		return segkafka.RequireOne, nil
	case "none", "0":
		return segkafka.RequireNone, nil
	default:
		return 0, fmt.Errorf("invalid required acks %q (want all, one, or none)", s)
	}
}
