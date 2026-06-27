package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// TopicReader is the use case the topics handler depends on.
type TopicReader interface {
	List(ctx context.Context) ([]string, error)
	Describe(ctx context.Context, topic string) (service.TopicMetadata, error)
}

// TopicsHandler serves GET /topics and GET /topics/{topic}.
type TopicsHandler struct {
	topics TopicReader
	logger *slog.Logger
}

// NewTopicsHandler wires a TopicsHandler to its dependencies.
func NewTopicsHandler(topics TopicReader, logger *slog.Logger) *TopicsHandler {
	return &TopicsHandler{topics: topics, logger: logger}
}

type listTopicsResponse struct {
	Topics []string `json:"topics"`
	Count  int      `json:"count"`
}

type partitionMetadata struct {
	ID            int   `json:"id"`
	Leader        int   `json:"leader"`
	Replicas      []int `json:"replicas"`
	ISR           []int `json:"isr"`
	LowWatermark  int64 `json:"low_watermark"`
	HighWatermark int64 `json:"high_watermark"`
}

type topicMetadataResponse struct {
	Name              string              `json:"name"`
	ReplicationFactor int                 `json:"replication_factor"`
	Partitions        []partitionMetadata `json:"partitions"`
}

// List handles GET /topics.
func (h *TopicsHandler) List(w http.ResponseWriter, r *http.Request) {
	topics, err := h.topics.List(r.Context())
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	respondJSON(w, h.logger, http.StatusOK, listTopicsResponse{Topics: topics, Count: len(topics)})
}

// Describe handles GET /topics/{topic}.
func (h *TopicsHandler) Describe(w http.ResponseWriter, r *http.Request) {
	topic := chi.URLParam(r, "topic")

	md, err := h.topics.Describe(r.Context(), topic)
	if err != nil {
		writeError(w, h.logger, err)
		return
	}
	respondJSON(w, h.logger, http.StatusOK, toTopicMetadataResponse(md))
}

func toTopicMetadataResponse(md service.TopicMetadata) topicMetadataResponse {
	parts := make([]partitionMetadata, len(md.Partitions))
	for i, p := range md.Partitions {
		parts[i] = partitionMetadata{
			ID:            p.ID,
			Leader:        p.Leader,
			Replicas:      p.Replicas,
			ISR:           p.ISR,
			LowWatermark:  p.LowWatermark,
			HighWatermark: p.HighWatermark,
		}
	}
	return topicMetadataResponse{
		Name:              md.Name,
		ReplicationFactor: md.ReplicationFactor,
		Partitions:        parts,
	}
}
