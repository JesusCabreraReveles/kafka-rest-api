package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

type fakeConsumer struct {
	gotQuery service.ConsumeQuery
	msgs     []service.ConsumedMessage
	scanned  int
	err      error
}

func (f *fakeConsumer) Consume(_ context.Context, q service.ConsumeQuery) (service.ConsumeResult, error) {
	f.gotQuery = q
	if f.err != nil {
		return service.ConsumeResult{}, f.err
	}
	scanned := f.scanned
	if scanned == 0 {
		scanned = len(f.msgs)
	}
	return service.ConsumeResult{Messages: f.msgs, Scanned: scanned}, nil
}

func consumeGet(h *ConsumeHandler, target string) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	r.Get("/topics/{topic}/consume", h.Consume)
	req := httptest.NewRequest(http.MethodGet, target, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestConsumeParamParsing(t *testing.T) {
	tests := []struct {
		name     string
		target   string
		wantCode int
	}{
		{"defaults ok", "/topics/orders/consume", http.StatusOK},
		{"valid params", "/topics/orders/consume?partition=2&offset=latest&limit=5&timeout=3s", http.StatusOK},
		{"offset earliest", "/topics/orders/consume?offset=earliest", http.StatusOK},
		{"absolute offset", "/topics/orders/consume?offset=42", http.StatusOK},
		{"with filters", "/topics/orders/consume?key=k1&header=source:web&since=2026-01-01T00:00:00Z", http.StatusOK},
		{"from_timestamp unix", "/topics/orders/consume?from_timestamp=1700000000", http.StatusOK},
		{"bad partition", "/topics/orders/consume?partition=x", http.StatusBadRequest},
		{"bad offset", "/topics/orders/consume?offset=-5", http.StatusBadRequest},
		{"bad limit", "/topics/orders/consume?limit=abc", http.StatusBadRequest},
		{"bad timeout", "/topics/orders/consume?timeout=soon", http.StatusBadRequest},
		{"bad since", "/topics/orders/consume?since=yesterday", http.StatusBadRequest},
		{"bad header filter", "/topics/orders/consume?header=novalue", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc := &fakeConsumer{}
			h := NewConsumeHandler(fc, newTestLogger())

			rec := consumeGet(h, tt.target)
			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
		})
	}
}

func TestConsumeForwardsQuery(t *testing.T) {
	fc := &fakeConsumer{}
	h := NewConsumeHandler(fc, newTestLogger())

	consumeGet(h, "/topics/orders/consume?partition=3&offset=latest&limit=7&timeout=4s")

	q := fc.gotQuery
	if q.Topic != "orders" || q.Partition != 3 || q.Offset != service.OffsetLatest || q.Limit != 7 || q.Timeout != 4*time.Second {
		t.Errorf("forwarded query = %+v", q)
	}
}

func TestConsumeForwardsFilters(t *testing.T) {
	fc := &fakeConsumer{}
	h := NewConsumeHandler(fc, newTestLogger())

	consumeGet(h, "/topics/orders/consume?key=k1&header=source:web&header=tenant:acme&from_timestamp=1700000000&since=2026-01-01T00:00:00Z")

	q := fc.gotQuery
	if q.Filter.Key == nil || *q.Filter.Key != "k1" {
		t.Errorf("key filter = %v", q.Filter.Key)
	}
	if q.Filter.Headers["source"] != "web" || q.Filter.Headers["tenant"] != "acme" {
		t.Errorf("header filters = %v", q.Filter.Headers)
	}
	if q.FromTime == nil || q.FromTime.Unix() != 1700000000 {
		t.Errorf("from time = %v", q.FromTime)
	}
	if q.Filter.Since == nil {
		t.Error("expected since filter to be set")
	}
}

func TestConsumeReportsScanned(t *testing.T) {
	fc := &fakeConsumer{
		msgs:    []service.ConsumedMessage{{Offset: 5, Value: []byte(`{"a":1}`)}},
		scanned: 50, // scanned 50, matched 1 (filter selectivity)
	}
	h := NewConsumeHandler(fc, newTestLogger())

	rec := consumeGet(h, "/topics/orders/consume?key=rare")
	var resp consumeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 || resp.Scanned != 50 {
		t.Errorf("count=%d scanned=%d, want 1/50", resp.Count, resp.Scanned)
	}
}

func TestConsumeResponseEncoding(t *testing.T) {
	fc := &fakeConsumer{msgs: []service.ConsumedMessage{
		{
			Partition: 0, Offset: 10,
			Key:     []byte("user-1"),
			Value:   []byte(`{"amount":42}`),
			Headers: []service.Header{{Key: "source", Value: []byte("test")}},
		},
		{
			Partition: 0, Offset: 11,
			Key:   []byte{0xff, 0xfe}, // not valid UTF-8
			Value: []byte{0x00, 0x01}, // not valid JSON
		},
	}}
	h := NewConsumeHandler(fc, newTestLogger())

	rec := consumeGet(h, "/topics/orders/consume")
	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d", rec.Code)
	}

	var resp consumeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 2 {
		t.Fatalf("count = %d, want 2", resp.Count)
	}

	first := resp.Messages[0]
	if first.KeyEncoding != "utf8" || first.Key != "user-1" {
		t.Errorf("first key = %q (%s)", first.Key, first.KeyEncoding)
	}
	if first.ValueEncoding != "json" || string(first.Value) != `{"amount":42}` {
		t.Errorf("first value = %s (%s)", first.Value, first.ValueEncoding)
	}
	if first.Headers["source"] != "test" {
		t.Errorf("first headers = %v", first.Headers)
	}

	second := resp.Messages[1]
	if second.KeyEncoding != "base64" {
		t.Errorf("second key encoding = %s, want base64", second.KeyEncoding)
	}
	if second.ValueEncoding != "base64" {
		t.Errorf("second value encoding = %s, want base64", second.ValueEncoding)
	}
}

func TestConsumeTopicNotFound(t *testing.T) {
	fc := &fakeConsumer{err: service.ErrTopicNotFound}
	h := NewConsumeHandler(fc, newTestLogger())

	rec := consumeGet(h, "/topics/missing/consume")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	assertErrCode(t, rec, "topic_not_found")
}
