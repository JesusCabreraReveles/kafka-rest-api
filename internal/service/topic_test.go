package service

import (
	"context"
	"errors"
	"testing"
)

type fakeAdmin struct {
	topics   []string
	metadata TopicMetadata
	listErr  error
	descErr  error
	gotTopic string
}

func (f *fakeAdmin) ListTopics(_ context.Context) ([]string, error) {
	return f.topics, f.listErr
}

func (f *fakeAdmin) DescribeTopic(_ context.Context, topic string) (TopicMetadata, error) {
	f.gotTopic = topic
	return f.metadata, f.descErr
}

func TestTopicServiceList(t *testing.T) {
	fa := &fakeAdmin{topics: []string{"a", "b"}}
	svc := NewTopicService(fa, testLogger())

	got, err := svc.List(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d topics, want 2", len(got))
	}
}

func TestTopicServiceDescribe(t *testing.T) {
	tests := []struct {
		name      string
		topic     string
		descErr   error
		wantErrIs error
	}{
		{
			name:  "valid topic",
			topic: "orders",
		},
		{
			name:      "empty topic rejected before call",
			topic:     "",
			wantErrIs: ErrEmptyTopic,
		},
		{
			name:      "not found propagated",
			topic:     "missing",
			descErr:   ErrTopicNotFound,
			wantErrIs: ErrTopicNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fa := &fakeAdmin{
				metadata: TopicMetadata{Name: tt.topic, ReplicationFactor: 1},
				descErr:  tt.descErr,
			}
			svc := NewTopicService(fa, testLogger())

			md, err := svc.Describe(context.Background(), tt.topic)

			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(_, %v)", err, tt.wantErrIs)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if md.Name != tt.topic {
				t.Errorf("name = %q, want %q", md.Name, tt.topic)
			}
		})
	}
}
