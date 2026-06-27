package service

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeReader struct {
	gotReq ReadRequest
	called bool
	msgs   []ConsumedMessage
	err    error
}

func (f *fakeReader) Read(_ context.Context, req ReadRequest) ([]ConsumedMessage, error) {
	f.called = true
	f.gotReq = req
	return f.msgs, f.err
}

func testConsumerCfg() ConsumerConfig {
	return ConsumerConfig{
		DefaultLimit: 10,
		MaxLimit:     100,
		DefaultWait:  2 * time.Second,
		MaxWait:      10 * time.Second,
	}
}

func TestConsumerConsume(t *testing.T) {
	tests := []struct {
		name      string
		query     ConsumeQuery
		wantErrIs error
		wantLimit int // expected limit forwarded to the reader (0 => skip check)
	}{
		{
			name:      "defaults applied when limit omitted",
			query:     ConsumeQuery{Topic: "t", Offset: OffsetEarliest},
			wantLimit: 10,
		},
		{
			name:      "limit clamped to max",
			query:     ConsumeQuery{Topic: "t", Limit: 5000},
			wantLimit: 100,
		},
		{
			name:      "explicit limit preserved",
			query:     ConsumeQuery{Topic: "t", Limit: 25},
			wantLimit: 25,
		},
		{
			name:      "empty topic rejected",
			query:     ConsumeQuery{Topic: ""},
			wantErrIs: ErrEmptyTopic,
		},
		{
			name:      "negative partition rejected",
			query:     ConsumeQuery{Topic: "t", Partition: -1},
			wantErrIs: ErrInvalidQuery,
		},
		{
			name: "until before since rejected",
			query: ConsumeQuery{Topic: "t", Filter: MessageFilter{
				Since: timePtr(time.Unix(100, 0)),
				Until: timePtr(time.Unix(50, 0)),
			}},
			wantErrIs: ErrInvalidQuery,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fr := &fakeReader{msgs: []ConsumedMessage{{Offset: 1, Value: []byte("v")}}}
			svc := NewConsumerService(fr, testConsumerCfg(), testLogger())

			_, err := svc.Consume(context.Background(), tt.query)

			if tt.wantErrIs != nil {
				if !errors.Is(err, tt.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(_, %v)", err, tt.wantErrIs)
				}
				if fr.called {
					t.Errorf("reader was called despite validation failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantLimit != 0 && fr.gotReq.Limit != tt.wantLimit {
				t.Errorf("forwarded limit = %d, want %d", fr.gotReq.Limit, tt.wantLimit)
			}
		})
	}
}

func TestConsumerForwardsFromTime(t *testing.T) {
	when := time.Unix(1700000000, 0).UTC()
	fr := &fakeReader{}
	svc := NewConsumerService(fr, testConsumerCfg(), testLogger())

	if _, err := svc.Consume(context.Background(), ConsumeQuery{Topic: "t", FromTime: &when}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fr.gotReq.FromTime == nil || !fr.gotReq.FromTime.Equal(when) {
		t.Errorf("from time = %v, want %v", fr.gotReq.FromTime, when)
	}
}

func TestConsumerFiltersAndReportsScanned(t *testing.T) {
	scanned := []ConsumedMessage{
		{Offset: 0, Key: []byte("a"), Value: []byte("1")},
		{Offset: 1, Key: []byte("b"), Value: []byte("2")},
		{Offset: 2, Key: []byte("a"), Value: []byte("3")},
	}
	fr := &fakeReader{msgs: scanned}
	svc := NewConsumerService(fr, testConsumerCfg(), testLogger())

	key := "a"
	res, err := svc.Consume(context.Background(), ConsumeQuery{
		Topic:  "t",
		Filter: MessageFilter{Key: &key},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Scanned != 3 {
		t.Errorf("scanned = %d, want 3", res.Scanned)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("matched = %d, want 2", len(res.Messages))
	}
	for _, m := range res.Messages {
		if string(m.Key) != "a" {
			t.Errorf("unexpected key %q in filtered result", m.Key)
		}
	}
}

func TestConsumerWrapsReaderError(t *testing.T) {
	fr := &fakeReader{err: ErrTopicNotFound}
	svc := NewConsumerService(fr, testConsumerCfg(), testLogger())

	_, err := svc.Consume(context.Background(), ConsumeQuery{Topic: "missing"})
	if !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("err = %v, want errors.Is(_, ErrTopicNotFound)", err)
	}
}

func TestConsumerAppliesTimeout(t *testing.T) {
	cfg := testConsumerCfg()
	cfg.DefaultWait = 20 * time.Millisecond
	cfg.MaxWait = 20 * time.Millisecond

	var sawDeadline bool
	fr := readerFunc(func(ctx context.Context, _ ReadRequest) ([]ConsumedMessage, error) {
		_, sawDeadline = ctx.Deadline()
		return nil, nil
	})
	svc := NewConsumerService(fr, cfg, testLogger())

	if _, err := svc.Consume(context.Background(), ConsumeQuery{Topic: "t"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !sawDeadline {
		t.Error("expected reader context to carry a deadline")
	}
}

func TestMessageFilterMatches(t *testing.T) {
	base := time.Unix(1000, 0).UTC()
	msg := ConsumedMessage{
		Key:       []byte("user-1"),
		Headers:   []Header{{Key: "source", Value: []byte("web")}},
		Timestamp: base,
	}

	tests := []struct {
		name   string
		filter MessageFilter
		want   bool
	}{
		{name: "zero filter matches", filter: MessageFilter{}, want: true},
		{name: "matching key", filter: MessageFilter{Key: strPtr("user-1")}, want: true},
		{name: "non-matching key", filter: MessageFilter{Key: strPtr("user-2")}, want: false},
		{name: "matching header", filter: MessageFilter{Headers: map[string]string{"source": "web"}}, want: true},
		{name: "non-matching header value", filter: MessageFilter{Headers: map[string]string{"source": "mobile"}}, want: false},
		{name: "missing header", filter: MessageFilter{Headers: map[string]string{"x": "y"}}, want: false},
		{name: "within time range", filter: MessageFilter{Since: timePtr(base.Add(-time.Second)), Until: timePtr(base.Add(time.Second))}, want: true},
		{name: "before since", filter: MessageFilter{Since: timePtr(base.Add(time.Second))}, want: false},
		{name: "after until", filter: MessageFilter{Until: timePtr(base.Add(-time.Second))}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.filter.Matches(msg); got != tt.want {
				t.Errorf("Matches = %v, want %v", got, tt.want)
			}
		})
	}
}

// readerFunc adapts a function to the MessageReader interface.
type readerFunc func(ctx context.Context, req ReadRequest) ([]ConsumedMessage, error)

func (f readerFunc) Read(ctx context.Context, req ReadRequest) ([]ConsumedMessage, error) {
	return f(ctx, req)
}

func strPtr(s string) *string        { return &s }
func timePtr(t time.Time) *time.Time { return &t }
