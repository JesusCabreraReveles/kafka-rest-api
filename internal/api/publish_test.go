package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// fakePublisher implements Publisher for handler tests.
type fakePublisher struct {
	err       error
	results   []service.PublishResult
	gotTopic  string
	gotSingle service.Message
	gotBatch  []service.Message
}

func (f *fakePublisher) Publish(_ context.Context, topic string, msg service.Message) ([]service.PublishResult, error) {
	f.gotTopic = topic
	f.gotSingle = msg
	return f.results, f.err
}

func (f *fakePublisher) PublishBatch(_ context.Context, topic string, msgs []service.Message) ([]service.PublishResult, error) {
	f.gotTopic = topic
	f.gotBatch = msgs
	return f.results, f.err
}

// newPublishRequest builds a request routed through chi so {topic} is set.
func doRequest(t *testing.T, h *PublishHandler, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	r.Post("/topics/{topic}/publish", h.PublishOne)
	r.Post("/topics/{topic}/publish/batch", h.PublishBatch)

	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func TestPublishOne(t *testing.T) {
	tests := []struct {
		name         string
		body         string
		publisherErr error
		wantCode     int
		wantErrCode  string
	}{
		{
			name:     "valid publish",
			body:     `{"key":"123","value":{"amount":42},"headers":{"source":"test"}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "value can be any JSON",
			body:     `{"value":"a plain string"}`,
			wantCode: http.StatusOK,
		},
		{
			name:         "service validation error maps to 400",
			body:         `{"key":"123"}`,
			publisherErr: service.ErrEmptyValue,
			wantCode:     http.StatusBadRequest,
			wantErrCode:  "invalid_request",
		},
		{
			name:        "malformed JSON",
			body:        `{"key":`,
			wantCode:    http.StatusBadRequest,
			wantErrCode: "invalid_request",
		},
		{
			name:        "unknown field rejected",
			body:        `{"value":{},"bogus":1}`,
			wantCode:    http.StatusBadRequest,
			wantErrCode: "invalid_request",
		},
		{
			name:         "unknown topic maps to 404",
			body:         `{"value":{"ok":true}}`,
			publisherErr: service.ErrTopicNotFound,
			wantCode:     http.StatusNotFound,
			wantErrCode:  "topic_not_found",
		},
		{
			name:         "producer failure maps to 502",
			body:         `{"value":{"ok":true}}`,
			publisherErr: errors.New("broker unreachable"),
			wantCode:     http.StatusBadGateway,
			wantErrCode:  "kafka_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := &fakePublisher{err: tt.publisherErr}
			h := NewPublishHandler(fp, newTestLogger(), 1<<20, 100)

			rec := doRequest(t, h, http.MethodPost, "/topics/orders/publish", tt.body)

			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantCode == http.StatusOK {
				var resp publishResponse
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if resp.Topic != "orders" || resp.Published != 1 {
					t.Errorf("resp = %+v", resp)
				}
				return
			}
			assertErrCode(t, rec, tt.wantErrCode)
		})
	}
}

func TestPublishValueEncoding(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantCode  int
		wantValue string // expected raw bytes forwarded to the publisher
	}{
		{
			name:      "json default published verbatim",
			body:      `{"value":{"a":1}}`,
			wantCode:  http.StatusOK,
			wantValue: `{"a":1}`,
		},
		{
			name:      "string encoding publishes raw utf8",
			body:      `{"value":"hello world","value_encoding":"string"}`,
			wantCode:  http.StatusOK,
			wantValue: "hello world",
		},
		{
			name:      "base64 encoding decodes to raw bytes",
			body:      `{"value":"aGVsbG8=","value_encoding":"base64"}`, // "hello"
			wantCode:  http.StatusOK,
			wantValue: "hello",
		},
		{
			name:     "base64 with non-string value rejected",
			body:     `{"value":{"a":1},"value_encoding":"base64"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid base64 rejected",
			body:     `{"value":"not!base64!","value_encoding":"base64"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "unknown encoding rejected",
			body:     `{"value":"x","value_encoding":"protobuf"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := &fakePublisher{}
			h := NewPublishHandler(fp, newTestLogger(), 1<<20, 100)

			rec := doRequest(t, h, http.MethodPost, "/topics/orders/publish", tt.body)

			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantCode == http.StatusOK && string(fp.gotSingle.Value) != tt.wantValue {
				t.Errorf("forwarded value = %q, want %q", fp.gotSingle.Value, tt.wantValue)
			}
			if tt.wantCode == http.StatusBadRequest {
				assertErrCode(t, rec, "invalid_request")
			}
		})
	}
}

func TestPublishOffsetsInResponse(t *testing.T) {
	// sync mode: producer reports per-record offsets, which appear in the body.
	fp := &fakePublisher{results: []service.PublishResult{{Partition: 2, Offset: 41}}}
	h := NewPublishHandler(fp, newTestLogger(), 1<<20, 100)

	rec := doRequest(t, h, http.MethodPost, "/topics/orders/publish", `{"value":{"a":1}}`)
	var resp publishResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Offsets) != 1 || resp.Offsets[0].Partition != 2 || resp.Offsets[0].Offset != 41 {
		t.Errorf("offsets = %+v", resp.Offsets)
	}
}

func TestPublishNoOffsetsOmitted(t *testing.T) {
	// batched mode: no offsets reported, so the field is omitted from the body.
	fp := &fakePublisher{} // results nil
	h := NewPublishHandler(fp, newTestLogger(), 1<<20, 100)

	rec := doRequest(t, h, http.MethodPost, "/topics/orders/publish", `{"value":{"a":1}}`)
	if strings.Contains(rec.Body.String(), "offsets") {
		t.Errorf("expected offsets to be omitted, got %s", rec.Body.String())
	}
}

func TestPublishOneForwardsFields(t *testing.T) {
	fp := &fakePublisher{}
	h := NewPublishHandler(fp, newTestLogger(), 1<<20, 100)

	doRequest(t, h, http.MethodPost, "/topics/orders/publish",
		`{"key":"k1","value":{"a":1},"headers":{"source":"test"}}`)

	if string(fp.gotSingle.Key) != "k1" {
		t.Errorf("key = %q", fp.gotSingle.Key)
	}
	if string(fp.gotSingle.Value) != `{"a":1}` {
		t.Errorf("value = %q", fp.gotSingle.Value)
	}
	if len(fp.gotSingle.Headers) != 1 || fp.gotSingle.Headers[0].Key != "source" {
		t.Errorf("headers = %+v", fp.gotSingle.Headers)
	}
}

func TestPublishBatch(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		maxBatch  int
		wantCode  int
		wantCount int
	}{
		{
			name:      "valid batch",
			body:      `{"messages":[{"value":{"a":1}},{"value":{"b":2}}]}`,
			maxBatch:  100,
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
		{
			name:     "batch over limit rejected",
			body:     `{"messages":[{"value":1},{"value":2},{"value":3}]}`,
			maxBatch: 2,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := &fakePublisher{}
			h := NewPublishHandler(fp, newTestLogger(), 1<<20, tt.maxBatch)

			rec := doRequest(t, h, http.MethodPost, "/topics/orders/publish/batch", tt.body)

			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantCode == http.StatusOK {
				var resp publishResponse
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if resp.Published != tt.wantCount {
					t.Errorf("published = %d, want %d", resp.Published, tt.wantCount)
				}
			}
		})
	}
}

func TestPublishBodyTooLarge(t *testing.T) {
	fp := &fakePublisher{}
	h := NewPublishHandler(fp, newTestLogger(), 32, 100) // tiny limit

	big := `{"value":"` + strings.Repeat("x", 1000) + `"}`
	rec := doRequest(t, h, http.MethodPost, "/topics/orders/publish", big)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrCode(t, rec, "invalid_request")
}

func assertErrCode(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	var resp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if resp.Error.Code != want {
		t.Errorf("error code = %q, want %q (message: %q)", resp.Error.Code, want, resp.Error.Message)
	}
}
