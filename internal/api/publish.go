package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// Publisher is the use case the publish handlers depend on.
type Publisher interface {
	Publish(ctx context.Context, topic string, msg service.Message) ([]service.PublishResult, error)
	PublishBatch(ctx context.Context, topic string, msgs []service.Message) ([]service.PublishResult, error)
}

// PublishHandler serves the publish endpoints.
type PublishHandler struct {
	publisher    Publisher
	logger       *slog.Logger
	maxBodyBytes int64
	maxBatchSize int
}

// NewPublishHandler wires a PublishHandler to its dependencies. maxBodyBytes
// caps the request body size; maxBatchSize caps messages per batch request.
func NewPublishHandler(publisher Publisher, logger *slog.Logger, maxBodyBytes int64, maxBatchSize int) *PublishHandler {
	return &PublishHandler{
		publisher:    publisher,
		logger:       logger,
		maxBodyBytes: maxBodyBytes,
		maxBatchSize: maxBatchSize,
	}
}

// messageRequest is the wire representation of a single message.
//
// ValueEncoding selects how `value` is interpreted before publishing:
//   - "json" (default): any JSON document, published verbatim as its bytes.
//   - "string": value must be a JSON string, published as raw UTF-8 bytes.
//   - "base64": value must be a base64 JSON string, decoded to raw bytes
//     (use this to publish arbitrary binary payloads).
type messageRequest struct {
	Key           string            `json:"key"`
	Value         json.RawMessage   `json:"value"`
	ValueEncoding string            `json:"value_encoding,omitempty"`
	Headers       map[string]string `json:"headers"`
}

func (m messageRequest) toMessage() (service.Message, error) {
	value, err := decodePublishValue(m.Value, m.ValueEncoding)
	if err != nil {
		return service.Message{}, err
	}

	msg := service.Message{Value: value}
	if m.Key != "" {
		msg.Key = []byte(m.Key)
	}
	if len(m.Headers) > 0 {
		msg.Headers = make([]service.Header, 0, len(m.Headers))
		for k, v := range m.Headers {
			msg.Headers = append(msg.Headers, service.Header{Key: k, Value: []byte(v)})
		}
	}
	return msg, nil
}

// decodePublishValue converts the wire `value` into the raw bytes to publish,
// according to the declared encoding.
func decodePublishValue(raw json.RawMessage, encoding string) ([]byte, error) {
	switch strings.ToLower(encoding) {
	case "", encodingJSON:
		return []byte(raw), nil
	case encodingString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: value must be a JSON string when value_encoding=string", errBadRequest)
		}
		return []byte(s), nil
	case encodingBase64:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%w: value must be a base64 JSON string when value_encoding=base64", errBadRequest)
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("%w: value is not valid base64: %w", errBadRequest, err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("%w: unknown value_encoding %q (want json, string, or base64)", errBadRequest, encoding)
	}
}

type batchRequest struct {
	Messages []messageRequest `json:"messages"`
}

type recordOffset struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

type publishResponse struct {
	Topic     string         `json:"topic"`
	Published int            `json:"published"`
	Offsets   []recordOffset `json:"offsets,omitempty"`
}

// toOffsets maps service results to the response shape. It returns nil (so the
// field is omitted) when the producer reported no offsets (batched mode).
func toOffsets(results []service.PublishResult) []recordOffset {
	if len(results) == 0 {
		return nil
	}
	out := make([]recordOffset, len(results))
	for i, r := range results {
		out[i] = recordOffset{Partition: r.Partition, Offset: r.Offset}
	}
	return out
}

// PublishOne handles POST /topics/{topic}/publish.
func (h *PublishHandler) PublishOne(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")

	var req messageRequest
	if err := h.decode(w, r, &req); err != nil {
		writeError(w, h.logger, err)
		return
	}

	msg, err := req.toMessage()
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	results, err := h.publisher.Publish(r.Context(), topic, msg)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	respondJSON(w, h.logger, http.StatusOK, publishResponse{
		Topic:     topic,
		Published: 1,
		Offsets:   toOffsets(results),
	})
}

// PublishBatch handles POST /topics/{topic}/publish/batch.
func (h *PublishHandler) PublishBatch(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")

	var req batchRequest
	if err := h.decode(w, r, &req); err != nil {
		writeError(w, h.logger, err)
		return
	}
	if len(req.Messages) > h.maxBatchSize {
		writeError(w, h.logger, fmt.Errorf("%w: batch size %d exceeds limit %d",
			errBadRequest, len(req.Messages), h.maxBatchSize))
		return
	}

	msgs := make([]service.Message, len(req.Messages))
	for i, m := range req.Messages {
		msg, err := m.toMessage()
		if err != nil {
			writeError(w, h.logger, fmt.Errorf("message at index %d: %w", i, err))
			return
		}
		msgs[i] = msg
	}

	results, err := h.publisher.PublishBatch(r.Context(), topic, msgs)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}

	respondJSON(w, h.logger, http.StatusOK, publishResponse{
		Topic:     topic,
		Published: len(msgs),
		Offsets:   toOffsets(results),
	})
}

// decode reads and strictly decodes a JSON body, enforcing the size limit and
// rejecting unknown fields. Decode failures are wrapped as bad requests.
func (h *PublishHandler) decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return fmt.Errorf("%w: request body exceeds %d bytes", errBadRequest, h.maxBodyBytes)
		}
		return fmt.Errorf("%w: invalid JSON body: %w", errBadRequest, err)
	}
	if dec.More() {
		return fmt.Errorf("%w: body must contain a single JSON object", errBadRequest)
	}
	return nil
}
