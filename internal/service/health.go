// Package service holds the application's use-case layer. It depends only on
// domain concepts and standard library types — never on transport (HTTP) or
// infrastructure (Kafka client) details — so it can be tested in isolation.
package service

import (
	"context"
	"time"
)

// Overall health status values.
const (
	StatusOK       = "ok"
	StatusDegraded = "degraded"
)

// DependencyChecker reports whether a downstream dependency is reachable.
// Implemented by the infrastructure layer (e.g. the Kafka client).
type DependencyChecker interface {
	Name() string
	Check(ctx context.Context) error
}

// DependencyStatus is the result of a single dependency check.
type DependencyStatus struct {
	Name   string
	Status string // "ok" or "failed"
	Error  string
}

// HealthStatus is the outcome of a health evaluation.
type HealthStatus struct {
	Status  string
	Version string
	Uptime  time.Duration
	Checks  []DependencyStatus
}

// Clock returns the current time. It is injected so tests can control time.
type Clock func() time.Time

// HealthService reports the liveness of the application and the readiness of
// its dependencies.
type HealthService struct {
	version string
	started time.Time
	now     Clock
	deps    []DependencyChecker
	timeout time.Duration
}

// NewHealthService constructs a HealthService. If now is nil, time.Now is used.
// deps are probed on each Check, each bounded by timeout.
func NewHealthService(version string, now Clock, timeout time.Duration, deps ...DependencyChecker) *HealthService {
	if now == nil {
		now = time.Now
	}
	return &HealthService{
		version: version,
		started: now(),
		now:     now,
		deps:    deps,
		timeout: timeout,
	}
}

// Live reports liveness: whether the process itself is up and serving. It does
// not probe dependencies, so it never reports "degraded" — a live process that
// cannot reach Kafka should be kept running, not restarted.
func (s *HealthService) Live() HealthStatus {
	return HealthStatus{
		Status:  StatusOK,
		Version: s.version,
		Uptime:  s.now().Sub(s.started),
	}
}

// Ready reports readiness: it probes each dependency and returns "degraded" if
// any check fails, so orchestrators can stop routing traffic until the
// dependency recovers.
func (s *HealthService) Ready(ctx context.Context) HealthStatus {
	status := HealthStatus{
		Status:  StatusOK,
		Version: s.version,
		Uptime:  s.now().Sub(s.started),
	}

	for _, d := range s.deps {
		ds := DependencyStatus{Name: d.Name(), Status: "ok"}

		cctx, cancel := context.WithTimeout(ctx, s.timeout)
		err := d.Check(cctx)
		cancel()

		if err != nil {
			ds.Status = "failed"
			ds.Error = err.Error()
			status.Status = StatusDegraded
		}
		status.Checks = append(status.Checks, ds)
	}

	return status
}
