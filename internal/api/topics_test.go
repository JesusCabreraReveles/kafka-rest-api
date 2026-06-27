package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

type fakeTopicReader struct {
	topics   []string
	metadata service.TopicMetadata
	listErr  error
	descErr  error
}

func (f *fakeTopicReader) List(_ context.Context) ([]string, error) {
	return f.topics, f.listErr
}

func (f *fakeTopicReader) Describe(_ context.Context, _ string) (service.TopicMetadata, error) {
	return f.metadata, f.descErr
}

func TestTopicsList(t *testing.T) {
	fr := &fakeTopicReader{topics: []string{"a", "b", "c"}}
	h := NewTopicsHandler(fr, newTestLogger())

	r := chi.NewRouter()
	r.Get("/topics", h.List)
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}
	var resp listTopicsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 3 || len(resp.Topics) != 3 {
		t.Errorf("resp = %+v", resp)
	}
}

func describeGet(h *TopicsHandler, topic string) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	r.Get("/topics/{topic}", h.Describe)
	req := httptest.NewRequest(http.MethodGet, "/topics/"+topic, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestTopicsDescribe(t *testing.T) {
	fr := &fakeTopicReader{metadata: service.TopicMetadata{
		Name:              "orders",
		ReplicationFactor: 3,
		Partitions: []service.PartitionMetadata{
			{ID: 0, Leader: 1, Replicas: []int{1, 2, 3}, ISR: []int{1, 2, 3}, LowWatermark: 5, HighWatermark: 42},
		},
	}}
	h := NewTopicsHandler(fr, newTestLogger())

	rec := describeGet(h, "orders")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}

	var resp topicMetadataResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Name != "orders" || resp.ReplicationFactor != 3 || len(resp.Partitions) != 1 {
		t.Fatalf("resp = %+v", resp)
	}
	p := resp.Partitions[0]
	if p.LowWatermark != 5 || p.HighWatermark != 42 || p.Leader != 1 {
		t.Errorf("partition = %+v", p)
	}
}

func TestTopicsDescribeNotFound(t *testing.T) {
	fr := &fakeTopicReader{descErr: service.ErrTopicNotFound}
	h := NewTopicsHandler(fr, newTestLogger())

	rec := describeGet(h, "missing")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	assertErrCode(t, rec, "topic_not_found")
}
