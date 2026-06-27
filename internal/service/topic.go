package service

import (
	"context"
	"fmt"
	"log/slog"
)

// PartitionMetadata describes a single partition of a topic.
type PartitionMetadata struct {
	ID            int
	Leader        int
	Replicas      []int
	ISR           []int
	LowWatermark  int64
	HighWatermark int64
}

// TopicMetadata describes a topic's partitions and replication.
type TopicMetadata struct {
	Name              string
	ReplicationFactor int
	Partitions        []PartitionMetadata
}

// TopicAdmin exposes cluster metadata. Implemented by the infrastructure layer.
type TopicAdmin interface {
	ListTopics(ctx context.Context) ([]string, error)
	DescribeTopic(ctx context.Context, topic string) (TopicMetadata, error)
}

// TopicService provides topic discovery and metadata.
type TopicService struct {
	admin  TopicAdmin
	logger *slog.Logger
}

// NewTopicService wires a TopicService to its dependencies.
func NewTopicService(admin TopicAdmin, logger *slog.Logger) *TopicService {
	return &TopicService{admin: admin, logger: logger}
}

// List returns the names of available topics.
func (s *TopicService) List(ctx context.Context) ([]string, error) {
	topics, err := s.admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	return topics, nil
}

// Describe returns metadata for a single topic.
func (s *TopicService) Describe(ctx context.Context, topic string) (TopicMetadata, error) {
	if topic == "" {
		return TopicMetadata{}, ErrEmptyTopic
	}
	md, err := s.admin.DescribeTopic(ctx, topic)
	if err != nil {
		return TopicMetadata{}, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	return md, nil
}
