package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Validation errors returned by the publisher. Callers (e.g. the HTTP layer)
// use errors.Is to map these to client-facing status codes.
var (
	// ErrEmptyTopic is returned when no topic is supplied.
	ErrEmptyTopic = errors.New("topic must not be empty")
	// ErrNoMessages is returned when a batch contains no messages.
	ErrNoMessages = errors.New("no messages to publish")
	// ErrEmptyValue is returned when a message has an empty value.
	ErrEmptyValue = errors.New("message value must not be empty")
)

// PublishResult is the coordinate a record was written to. It is populated only
// by producers that report offsets (the "sync" mode); the high-throughput
// "batched" producer returns no results. See ADR 0001.
type PublishResult struct {
	Partition int
	Offset    int64
}

// Producer is the abstraction the publisher depends on to hand messages to
// Kafka. It is defined here, at the point of use, and implemented by the
// infrastructure layer (internal/kafka). The returned results, when non-nil,
// carry the per-record partition/offset in the same order as msgs.
type Producer interface {
	Produce(ctx context.Context, topic string, msgs ...Message) ([]PublishResult, error)
}

// PublisherService validates messages and publishes them via a Producer.
type PublisherService struct {
	producer Producer
	logger   *slog.Logger
}

// NewPublisherService wires a PublisherService to its dependencies.
func NewPublisherService(producer Producer, logger *slog.Logger) *PublisherService {
	return &PublisherService{producer: producer, logger: logger}
}

// Publish validates and publishes a single message to topic.
func (s *PublisherService) Publish(ctx context.Context, topic string, msg Message) ([]PublishResult, error) {
	return s.PublishBatch(ctx, topic, []Message{msg})
}

// PublishBatch validates and publishes a batch of messages to topic. It is
// all-or-nothing from the caller's perspective: any validation failure rejects
// the whole batch before anything is sent. The returned results carry per-record
// offsets when the underlying producer reports them.
func (s *PublisherService) PublishBatch(ctx context.Context, topic string, msgs []Message) ([]PublishResult, error) {
	if topic == "" {
		return nil, ErrEmptyTopic
	}
	if len(msgs) == 0 {
		return nil, ErrNoMessages
	}
	for i, m := range msgs {
		if len(m.Value) == 0 {
			return nil, fmt.Errorf("message at index %d: %w", i, ErrEmptyValue)
		}
	}

	results, err := s.producer.Produce(ctx, topic, msgs...)
	if err != nil {
		return nil, fmt.Errorf("produce to %q: %w", topic, err)
	}

	s.logger.LogAttrs(ctx, slog.LevelDebug, "published messages",
		slog.String("topic", topic),
		slog.Int("count", len(msgs)),
	)
	return results, nil
}
