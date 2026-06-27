// Package metrics provides Prometheus instrumentation for the application:
// a registry, HTTP middleware, and decorators that wrap the Kafka use-case
// interfaces. Collectors are injected rather than global, so tests get a fresh
// registry and there is no shared mutable state.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const namespace = "kra"

// Metrics holds the application's Prometheus collectors.
type Metrics struct {
	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
	kafkaOps       *prometheus.CounterVec
	kafkaOpLatency *prometheus.HistogramVec
	kafkaMessages  *prometheus.CounterVec
}

// New constructs the collectors and registers them with reg.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "http", Name: "requests_total",
			Help: "Total number of HTTP requests by method, route, and status.",
		}, []string{"method", "route", "status"}),

		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "http", Name: "request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),

		kafkaOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "kafka", Name: "operations_total",
			Help: "Total number of Kafka operations by operation and status.",
		}, []string{"operation", "status"}),

		kafkaOpLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Subsystem: "kafka", Name: "operation_duration_seconds",
			Help:    "Kafka operation latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"operation"}),

		kafkaMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Subsystem: "kafka", Name: "messages_total",
			Help: "Total number of Kafka messages processed by operation.",
		}, []string{"operation"}),
	}

	reg.MustRegister(
		m.httpRequests,
		m.httpDuration,
		m.kafkaOps,
		m.kafkaOpLatency,
		m.kafkaMessages,
	)
	return m
}

// NewRegistry returns a registry preloaded with Go runtime and process
// collectors, suitable for serving at /metrics.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}
