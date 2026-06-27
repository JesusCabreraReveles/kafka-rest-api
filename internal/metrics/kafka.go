package metrics

import (
	"context"
	"time"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// Kafka operation labels.
const (
	opPublish       = "publish"
	opConsume       = "consume"
	opListTopics    = "list_topics"
	opDescribeTopic = "describe_topic"
)

// record updates the operation counter and latency histogram.
func (m *Metrics) record(operation string, start time.Time, err error) {
	status := "success"
	if err != nil {
		status = "error"
	}
	m.kafkaOps.WithLabelValues(operation, status).Inc()
	m.kafkaOpLatency.WithLabelValues(operation).Observe(time.Since(start).Seconds())
}

// InstrumentedProducer wraps a service.Producer with metrics.
type InstrumentedProducer struct {
	next service.Producer
	m    *Metrics
}

// NewInstrumentedProducer decorates p with metrics recording.
func NewInstrumentedProducer(p service.Producer, m *Metrics) *InstrumentedProducer {
	return &InstrumentedProducer{next: p, m: m}
}

// Produce records publish metrics around the wrapped producer.
func (p *InstrumentedProducer) Produce(ctx context.Context, topic string, msgs ...service.Message) ([]service.PublishResult, error) {
	start := time.Now()
	results, err := p.next.Produce(ctx, topic, msgs...)
	p.m.record(opPublish, start, err)
	if err == nil {
		p.m.kafkaMessages.WithLabelValues(opPublish).Add(float64(len(msgs)))
	}
	return results, err
}

// InstrumentedReader wraps a service.MessageReader with metrics.
type InstrumentedReader struct {
	next service.MessageReader
	m    *Metrics
}

// NewInstrumentedReader decorates r with metrics recording.
func NewInstrumentedReader(r service.MessageReader, m *Metrics) *InstrumentedReader {
	return &InstrumentedReader{next: r, m: m}
}

// Read records consume metrics around the wrapped reader.
func (r *InstrumentedReader) Read(ctx context.Context, req service.ReadRequest) ([]service.ConsumedMessage, error) {
	start := time.Now()
	msgs, err := r.next.Read(ctx, req)
	r.m.record(opConsume, start, err)
	if err == nil {
		r.m.kafkaMessages.WithLabelValues(opConsume).Add(float64(len(msgs)))
	}
	return msgs, err
}

// InstrumentedAdmin wraps a service.TopicAdmin with metrics.
type InstrumentedAdmin struct {
	next service.TopicAdmin
	m    *Metrics
}

// NewInstrumentedAdmin decorates a with metrics recording.
func NewInstrumentedAdmin(a service.TopicAdmin, m *Metrics) *InstrumentedAdmin {
	return &InstrumentedAdmin{next: a, m: m}
}

// ListTopics records metrics around the wrapped admin call.
func (a *InstrumentedAdmin) ListTopics(ctx context.Context) ([]string, error) {
	start := time.Now()
	topics, err := a.next.ListTopics(ctx)
	a.m.record(opListTopics, start, err)
	return topics, err
}

// DescribeTopic records metrics around the wrapped admin call.
func (a *InstrumentedAdmin) DescribeTopic(ctx context.Context, topic string) (service.TopicMetadata, error) {
	start := time.Now()
	md, err := a.next.DescribeTopic(ctx, topic)
	a.m.record(opDescribeTopic, start, err)
	return md, err
}
