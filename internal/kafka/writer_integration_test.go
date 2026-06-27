//go:build integration

// Package kafka integration tests run against a real broker. Enable with:
//
//	go test -tags=integration ./internal/kafka/...
//
// Override the broker list via KRA_KAFKA_BROKERS (default localhost:9092).
package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	segkafka "github.com/segmentio/kafka-go"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

func brokers() []string {
	if v := os.Getenv("KRA_KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

func TestWriterProduceAndReadBack(t *testing.T) {
	bks := brokers()
	topic := fmt.Sprintf("kra-it-%d", time.Now().UnixNano())

	w, err := NewWriter(WriterConfig{
		Brokers:         bks,
		WriteTimeout:    10 * time.Second,
		BatchTimeout:    10 * time.Millisecond,
		RequiredAcks:    "all",
		AllowAutoCreate: true,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := service.Message{
		Key:   []byte("k1"),
		Value: []byte(`{"hello":"world"}`),
		Headers: []service.Header{
			{Key: "source", Value: []byte("integration-test")},
		},
	}
	if _, err := w.Produce(ctx, topic, msg); err != nil {
		t.Fatalf("produce: %v", err)
	}

	r := segkafka.NewReader(segkafka.ReaderConfig{
		Brokers:   bks,
		Topic:     topic,
		Partition: 0,
		MaxWait:   500 * time.Millisecond,
	})
	t.Cleanup(func() { _ = r.Close() })

	got, err := r.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got.Key) != "k1" {
		t.Errorf("key = %q, want k1", got.Key)
	}
	if string(got.Value) != `{"hello":"world"}` {
		t.Errorf("value = %q", got.Value)
	}
	if len(got.Headers) != 1 || got.Headers[0].Key != "source" {
		t.Errorf("headers = %+v", got.Headers)
	}
}

func TestClientConsumeAndMetadata(t *testing.T) {
	bks := brokers()
	topic := fmt.Sprintf("kra-it-cm-%d", time.Now().UnixNano())

	w, err := NewWriter(WriterConfig{
		Brokers:         bks,
		WriteTimeout:    10 * time.Second,
		BatchTimeout:    10 * time.Millisecond,
		RequiredAcks:    "all",
		AllowAutoCreate: true,
	})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		msg := service.Message{Value: []byte(fmt.Sprintf(`{"n":%d}`, i))}
		if _, err := w.Produce(ctx, topic, msg); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	client, err := NewClient(ClientConfig{Brokers: bks, AdminTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	// Consume from the beginning, bounded by limit.
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	defer readCancel()
	msgs, err := client.Read(readCtx, service.ReadRequest{Topic: topic, Partition: 0, Offset: service.OffsetEarliest, Limit: 3})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("read %d messages, want 3", len(msgs))
	}
	if string(msgs[0].Value) != `{"n":0}` {
		t.Errorf("first value = %q", msgs[0].Value)
	}

	// Time-based replay: seeking to just before the last message's timestamp
	// must not return earlier messages.
	from := msgs[2].Timestamp
	replayCtx, replayCancel := context.WithTimeout(ctx, 5*time.Second)
	defer replayCancel()
	replayed, err := client.Read(replayCtx, service.ReadRequest{Topic: topic, Partition: 0, FromTime: &from, Limit: 3})
	if err != nil {
		t.Fatalf("replay read: %v", err)
	}
	if len(replayed) == 0 || replayed[0].Offset < msgs[2].Offset {
		t.Errorf("replay from %s returned offset %d, want >= %d", from, replayed[0].Offset, msgs[2].Offset)
	}

	// Topic appears in the listing.
	topics, err := client.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !contains(topics, topic) {
		t.Errorf("topic %q not found in listing", topic)
	}

	// Metadata reports a single partition with the expected high watermark.
	md, err := client.DescribeTopic(ctx, topic)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(md.Partitions) != 1 {
		t.Fatalf("partitions = %d, want 1", len(md.Partitions))
	}
	if got := md.Partitions[0].HighWatermark; got != 3 {
		t.Errorf("high watermark = %d, want 3", got)
	}
	if md.ReplicationFactor != 1 {
		t.Errorf("replication factor = %d, want 1", md.ReplicationFactor)
	}

	// Unknown topic is reported as not found.
	if _, err := client.DescribeTopic(ctx, "kra-it-does-not-exist"); err == nil {
		t.Error("expected error describing unknown topic")
	}
}

func TestSyncProducerReturnsOffsets(t *testing.T) {
	bks := brokers()
	topic := fmt.Sprintf("kra-it-sync-%d", time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the topic first (the sync producer does not auto-create).
	w, err := NewWriter(WriterConfig{Brokers: bks, WriteTimeout: 10 * time.Second, RequiredAcks: "all", AllowAutoCreate: true})
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })
	if _, err := w.Produce(ctx, topic, service.Message{Value: []byte(`{"seed":true}`)}); err != nil {
		t.Fatalf("seed produce: %v", err)
	}

	sp, err := NewSyncProducer(SyncProducerConfig{Brokers: bks, WriteTimeout: 10 * time.Second, RequiredAcks: "all"})
	if err != nil {
		t.Fatalf("new sync producer: %v", err)
	}
	t.Cleanup(func() { _ = sp.Close() })

	results, err := sp.Produce(ctx, topic,
		service.Message{Key: []byte("k"), Value: []byte(`{"n":1}`)},
		service.Message{Key: []byte("k"), Value: []byte(`{"n":2}`)},
	)
	if err != nil {
		t.Fatalf("sync produce: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results = %d, want 2", len(results))
	}
	// Same key -> same partition, consecutive offsets.
	if results[0].Partition != results[1].Partition {
		t.Errorf("same key landed on different partitions: %d vs %d", results[0].Partition, results[1].Partition)
	}
	if results[1].Offset != results[0].Offset+1 {
		t.Errorf("offsets not consecutive: %d, %d", results[0].Offset, results[1].Offset)
	}

	// Unknown topic maps to the domain error.
	if _, err := sp.Produce(ctx, "kra-it-sync-missing", service.Message{Value: []byte("x")}); !errors.Is(err, service.ErrTopicNotFound) {
		t.Errorf("err = %v, want ErrTopicNotFound", err)
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
