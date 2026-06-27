package metrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

func TestMiddlewareRecordsRequest(t *testing.T) {
	m := New(prometheus.NewRegistry())

	r := chi.NewRouter()
	r.Use(m.Middleware)
	r.Get("/topics/{topic}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/topics/orders", nil)
	r.ServeHTTP(httptest.NewRecorder(), req)

	// The route label must be the pattern, not the concrete path (low cardinality).
	if got := testutil.ToFloat64(m.httpRequests.WithLabelValues("GET", "/topics/{topic}", "200")); got != 1 {
		t.Errorf("http_requests_total = %v, want 1", got)
	}
}

type fakeProducer struct{ err error }

func (f fakeProducer) Produce(_ context.Context, _ string, _ ...service.Message) ([]service.PublishResult, error) {
	return nil, f.err
}

func TestInstrumentedProducer(t *testing.T) {
	m := New(prometheus.NewRegistry())

	p := NewInstrumentedProducer(fakeProducer{}, m)
	_, err := p.Produce(context.Background(), "t",
		service.Message{Value: []byte("x")},
		service.Message{Value: []byte("y")},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := testutil.ToFloat64(m.kafkaOps.WithLabelValues(opPublish, "success")); got != 1 {
		t.Errorf("ops success = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.kafkaMessages.WithLabelValues(opPublish)); got != 2 {
		t.Errorf("messages = %v, want 2", got)
	}

	// A failing produce increments the error counter, not the message counter.
	pe := NewInstrumentedProducer(fakeProducer{err: errors.New("boom")}, m)
	_, _ = pe.Produce(context.Background(), "t", service.Message{Value: []byte("z")})

	if got := testutil.ToFloat64(m.kafkaOps.WithLabelValues(opPublish, "error")); got != 1 {
		t.Errorf("ops error = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.kafkaMessages.WithLabelValues(opPublish)); got != 2 {
		t.Errorf("messages after error = %v, want 2 (unchanged)", got)
	}
}
