package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Special offset values understood by Consume. They mirror Kafka's well-known
// sentinels; the infrastructure layer maps them to client constants.
const (
	OffsetEarliest int64 = -2
	OffsetLatest   int64 = -1
)

// ConsumedMessage is a single record returned from a consume operation.
type ConsumedMessage struct {
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// MessageFilter narrows consumed messages. A zero filter matches everything.
// Filtering is applied by the gateway over the scanned window (see ConsumeQuery
// Limit), since Kafka has no server-side key/header index.
type MessageFilter struct {
	Key     *string           // exact key match (nil = any)
	Headers map[string]string // all entries must match (AND)
	Since   *time.Time        // keep messages with Timestamp >= Since
	Until   *time.Time        // keep messages with Timestamp <= Until
}

// IsZero reports whether the filter has no criteria.
func (f MessageFilter) IsZero() bool {
	return f.Key == nil && len(f.Headers) == 0 && f.Since == nil && f.Until == nil
}

// Matches reports whether m satisfies every criterion in the filter.
func (f MessageFilter) Matches(m ConsumedMessage) bool {
	if f.Key != nil && string(m.Key) != *f.Key {
		return false
	}
	if f.Since != nil && m.Timestamp.Before(*f.Since) {
		return false
	}
	if f.Until != nil && m.Timestamp.After(*f.Until) {
		return false
	}
	for k, v := range f.Headers {
		if !hasHeader(m.Headers, k, v) {
			return false
		}
	}
	return true
}

func hasHeader(headers []Header, key, value string) bool {
	for _, h := range headers {
		if h.Key == key && string(h.Value) == value {
			return true
		}
	}
	return false
}

// ConsumeQuery describes a bounded read from a single topic partition,
// optionally starting at a timestamp and/or filtering the results.
type ConsumeQuery struct {
	Topic     string
	Partition int
	Offset    int64         // explicit offset, or OffsetEarliest / OffsetLatest
	FromTime  *time.Time    // start at the first offset >= this time (overrides Offset)
	Limit     int           // max messages to scan (0 => service default)
	Timeout   time.Duration // long-poll budget (0 => service default)
	Filter    MessageFilter
}

// ReadRequest is the infrastructure-level read instruction. The gateway scans
// up to Limit messages starting at Offset (or FromTime, if set).
type ReadRequest struct {
	Topic     string
	Partition int
	Offset    int64
	FromTime  *time.Time
	Limit     int
}

// ConsumeResult carries the matched messages plus how many were scanned, so
// callers understand filter selectivity within the bounded window.
type ConsumeResult struct {
	Messages []ConsumedMessage
	Scanned  int
}

// MessageReader scans a bounded set of messages from a topic partition.
// Implemented by the infrastructure layer (internal/kafka).
type MessageReader interface {
	Read(ctx context.Context, req ReadRequest) ([]ConsumedMessage, error)
}

// ConsumerConfig holds the limits applied to consume queries.
type ConsumerConfig struct {
	DefaultLimit int
	MaxLimit     int
	DefaultWait  time.Duration
	MaxWait      time.Duration
}

// ConsumerService validates and executes consume queries, enforcing limits and
// applying message filters.
type ConsumerService struct {
	reader MessageReader
	cfg    ConsumerConfig
	logger *slog.Logger
}

// NewConsumerService wires a ConsumerService to its dependencies.
func NewConsumerService(reader MessageReader, cfg ConsumerConfig, logger *slog.Logger) *ConsumerService {
	return &ConsumerService{reader: reader, cfg: cfg, logger: logger}
}

// Consume validates the query, clamps it to configured limits, scans up to
// Limit messages (from an offset or timestamp), and returns those that match
// the filter. A timeout while waiting for more messages is not an error.
func (s *ConsumerService) Consume(ctx context.Context, q ConsumeQuery) (ConsumeResult, error) {
	if q.Topic == "" {
		return ConsumeResult{}, ErrEmptyTopic
	}
	if q.Partition < 0 {
		return ConsumeResult{}, fmt.Errorf("%w: partition must be >= 0", ErrInvalidQuery)
	}
	if q.Filter.Since != nil && q.Filter.Until != nil && q.Filter.Until.Before(*q.Filter.Since) {
		return ConsumeResult{}, fmt.Errorf("%w: 'until' must not be before 'since'", ErrInvalidQuery)
	}

	limit := clamp(q.Limit, s.cfg.DefaultLimit, s.cfg.MaxLimit)
	wait := clampDuration(q.Timeout, s.cfg.DefaultWait, s.cfg.MaxWait)

	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()

	scanned, err := s.reader.Read(ctx, ReadRequest{
		Topic:     q.Topic,
		Partition: q.Partition,
		Offset:    q.Offset,
		FromTime:  q.FromTime,
		Limit:     limit,
	})
	if err != nil {
		return ConsumeResult{}, fmt.Errorf("consume from %q: %w", q.Topic, err)
	}

	messages := scanned
	if !q.Filter.IsZero() {
		messages = make([]ConsumedMessage, 0, len(scanned))
		for _, m := range scanned {
			if q.Filter.Matches(m) {
				messages = append(messages, m)
			}
		}
	}

	s.logger.LogAttrs(ctx, slog.LevelDebug, "consumed messages",
		slog.String("topic", q.Topic),
		slog.Int("partition", q.Partition),
		slog.Int("scanned", len(scanned)),
		slog.Int("matched", len(messages)),
	)
	return ConsumeResult{Messages: messages, Scanned: len(scanned)}, nil
}

// clamp returns v bounded to [1, max], substituting def when v <= 0.
func clamp(v, def, max int) int {
	if v <= 0 {
		v = def
	}
	if v > max {
		return max
	}
	return v
}

func clampDuration(v, def, max time.Duration) time.Duration {
	if v <= 0 {
		v = def
	}
	if v > max {
		return max
	}
	return v
}
