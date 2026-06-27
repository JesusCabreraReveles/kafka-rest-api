package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// MessageConsumer is the use case the consume handler depends on.
type MessageConsumer interface {
	Consume(ctx context.Context, q service.ConsumeQuery) (service.ConsumeResult, error)
}

// ConsumeHandler serves GET /topics/{topic}/consume.
type ConsumeHandler struct {
	consumer MessageConsumer
	logger   *slog.Logger
}

// NewConsumeHandler wires a ConsumeHandler to its dependencies.
func NewConsumeHandler(consumer MessageConsumer, logger *slog.Logger) *ConsumeHandler {
	return &ConsumeHandler{consumer: consumer, logger: logger}
}

type consumedMessage struct {
	Partition     int               `json:"partition"`
	Offset        int64             `json:"offset"`
	Key           string            `json:"key,omitempty"`
	KeyEncoding   string            `json:"key_encoding,omitempty"`
	Value         json.RawMessage   `json:"value"`
	ValueEncoding string            `json:"value_encoding"`
	Headers       map[string]string `json:"headers,omitempty"`
	Timestamp     time.Time         `json:"timestamp"`
}

type consumeResponse struct {
	Topic     string            `json:"topic"`
	Partition int               `json:"partition"`
	Count     int               `json:"count"`
	Scanned   int               `json:"scanned"`
	Messages  []consumedMessage `json:"messages"`
}

// Consume reads a bounded set of messages from a topic partition.
//
// Query parameters:
//   - partition (int, default 0)
//   - offset ("earliest" | "latest" | non-negative int, default "earliest")
//   - from_timestamp (RFC3339 or Unix seconds; overrides offset — time-based replay)
//   - limit (int) — max messages to scan
//   - timeout (Go duration) — long-poll budget
//   - key (string) — keep only messages with this exact key
//   - header (repeatable "name:value") — keep messages carrying all given headers
//   - since / until (RFC3339 or Unix seconds) — keep messages in the time range
func (h *ConsumeHandler) Consume(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")
	q := r.URL.Query()

	query, err := parseConsumeQuery(topic, q)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	result, err := h.consumer.Consume(r.Context(), query)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	respondJSON(w, h.logger, http.StatusOK, consumeResponse{
		Topic:     topic,
		Partition: query.Partition,
		Count:     len(result.Messages),
		Scanned:   result.Scanned,
		Messages:  toConsumedMessages(result.Messages),
	})
}

// parseConsumeQuery extracts and validates all consume query parameters.
func parseConsumeQuery(topic string, q url.Values) (service.ConsumeQuery, error) {
	partition, err := intParam(q, "partition", 0)
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	offset, err := offsetParam(q, "offset")
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	limit, err := intParam(q, "limit", 0)
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	timeout, err := durationParam(q, "timeout", 0)
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	fromTime, err := timeParam(q, "from_timestamp")
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	since, err := timeParam(q, "since")
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	until, err := timeParam(q, "until")
	if err != nil {
		return service.ConsumeQuery{}, err
	}
	headers, err := headerFilters(q)
	if err != nil {
		return service.ConsumeQuery{}, err
	}

	return service.ConsumeQuery{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		FromTime:  fromTime,
		Limit:     limit,
		Timeout:   timeout,
		Filter: service.MessageFilter{
			Key:     keyParam(q),
			Headers: headers,
			Since:   since,
			Until:   until,
		},
	}, nil
}

func toConsumedMessages(msgs []service.ConsumedMessage) []consumedMessage {
	out := make([]consumedMessage, len(msgs))
	for i, m := range msgs {
		key, keyEnc := encodeKey(m.Key)
		value, valEnc := encodeValue(m.Value)

		var headers map[string]string
		if len(m.Headers) > 0 {
			headers = make(map[string]string, len(m.Headers))
			for _, hdr := range m.Headers {
				headers[hdr.Key] = string(hdr.Value)
			}
		}

		out[i] = consumedMessage{
			Partition:     m.Partition,
			Offset:        m.Offset,
			Key:           key,
			KeyEncoding:   keyEnc,
			Value:         value,
			ValueEncoding: valEnc,
			Headers:       headers,
			Timestamp:     m.Timestamp,
		}
	}
	return out
}
