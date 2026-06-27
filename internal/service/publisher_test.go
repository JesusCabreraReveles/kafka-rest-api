package service

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

// fakeProducer records the messages it was asked to produce and can be made to
// fail on demand.
type fakeProducer struct {
	err       error
	results   []PublishResult
	gotTopic  string
	gotMsgs   []Message
	callCount int
}

func (f *fakeProducer) Produce(_ context.Context, topic string, msgs ...Message) ([]PublishResult, error) {
	f.callCount++
	f.gotTopic = topic
	f.gotMsgs = msgs
	return f.results, f.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPublisherPublishBatch(t *testing.T) {
	valid := Message{Value: []byte(`{"hello":"world"}`)}

	tests := []struct {
		name         string
		topic        string
		msgs         []Message
		producerErr  error
		wantErrIs    error
		wantProduced bool
	}{
		{
			name:         "valid single message",
			topic:        "orders",
			msgs:         []Message{valid},
			wantProduced: true,
		},
		{
			name:         "valid multiple messages",
			topic:        "orders",
			msgs:         []Message{valid, {Value: []byte("raw")}},
			wantProduced: true,
		},
		{
			name:      "empty topic rejected",
			topic:     "",
			msgs:      []Message{valid},
			wantErrIs: ErrEmptyTopic,
		},
		{
			name:      "empty batch rejected",
			topic:     "orders",
			msgs:      nil,
			wantErrIs: ErrNoMessages,
		},
		{
			name:      "empty value rejected before producing",
			topic:     "orders",
			msgs:      []Message{valid, {Value: nil}},
			wantErrIs: ErrEmptyValue,
		},
		{
			name:        "producer error is wrapped",
			topic:       "orders",
			msgs:        []Message{valid},
			producerErr: errors.New("broker down"),
			wantErrIs:   nil, // checked separately below
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := &fakeProducer{err: tt.producerErr}
			svc := NewPublisherService(fp, testLogger())

			_, err := svc.PublishBatch(context.Background(), tt.topic, tt.msgs)

			switch {
			case tt.wantErrIs != nil:
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(_, %v)", err, tt.wantErrIs)
				}
				if fp.callCount != 0 {
					t.Errorf("producer called %d times on validation failure, want 0", fp.callCount)
				}
			case tt.producerErr != nil:
				if err == nil || !errors.Is(err, tt.producerErr) {
					t.Fatalf("err = %v, want wrapped producer error", err)
				}
			default:
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !tt.wantProduced {
					return
				}
				if fp.gotTopic != tt.topic {
					t.Errorf("topic = %q, want %q", fp.gotTopic, tt.topic)
				}
				if len(fp.gotMsgs) != len(tt.msgs) {
					t.Errorf("produced %d messages, want %d", len(fp.gotMsgs), len(tt.msgs))
				}
			}
		})
	}
}

func TestPublisherPublishDelegatesToBatch(t *testing.T) {
	fp := &fakeProducer{}
	svc := NewPublisherService(fp, testLogger())

	if _, err := svc.Publish(context.Background(), "t", Message{Value: []byte("v")}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fp.callCount != 1 || len(fp.gotMsgs) != 1 {
		t.Fatalf("expected one message produced, got call=%d msgs=%d", fp.callCount, len(fp.gotMsgs))
	}
}
