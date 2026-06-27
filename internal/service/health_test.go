package service

import (
	"context"
	"errors"
	"testing"
	"time"
)

// stubDep is a configurable DependencyChecker for health tests.
type stubDep struct {
	name string
	err  error
}

func (s stubDep) Name() string                  { return s.name }
func (s stubDep) Check(_ context.Context) error { return s.err }

func TestHealthServiceCheck(t *testing.T) {
	base := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		deps        []DependencyChecker
		elapsed     time.Duration
		wantStatus  string
		wantChecks  int
		wantFailing string // name of the dependency expected to be "failed", if any
	}{
		{
			name:       "healthy with no dependencies",
			deps:       nil,
			elapsed:    90 * time.Second,
			wantStatus: StatusOK,
			wantChecks: 0,
		},
		{
			name:       "healthy with passing dependency",
			deps:       []DependencyChecker{stubDep{name: "kafka"}},
			elapsed:    5 * time.Second,
			wantStatus: StatusOK,
			wantChecks: 1,
		},
		{
			name:        "degraded when a dependency fails",
			deps:        []DependencyChecker{stubDep{name: "kafka", err: errors.New("dial tcp: refused")}},
			elapsed:     0,
			wantStatus:  StatusDegraded,
			wantChecks:  1,
			wantFailing: "kafka",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			now := base
			clock := func() time.Time { return now }

			svc := NewHealthService("v1.2.3", clock, time.Second, tt.deps...)
			now = base.Add(tt.elapsed)

			got := svc.Ready(context.Background())

			if got.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", got.Status, tt.wantStatus)
			}
			if got.Version != "v1.2.3" {
				t.Errorf("version = %q, want %q", got.Version, "v1.2.3")
			}
			if got.Uptime != tt.elapsed {
				t.Errorf("uptime = %s, want %s", got.Uptime, tt.elapsed)
			}
			if len(got.Checks) != tt.wantChecks {
				t.Fatalf("checks = %d, want %d", len(got.Checks), tt.wantChecks)
			}
			if tt.wantFailing != "" {
				if got.Checks[0].Name != tt.wantFailing || got.Checks[0].Status != "failed" || got.Checks[0].Error == "" {
					t.Errorf("expected failed check for %q, got %+v", tt.wantFailing, got.Checks[0])
				}
			}
		})
	}
}

func TestHealthServiceLiveIgnoresDependencies(t *testing.T) {
	// Even with a failing dependency, liveness stays "ok": a live process that
	// cannot reach Kafka should not be restarted.
	svc := NewHealthService("v1", nil, time.Second,
		stubDep{name: "kafka", err: errors.New("refused")})

	got := svc.Live()
	if got.Status != StatusOK {
		t.Errorf("status = %q, want %q", got.Status, StatusOK)
	}
	if len(got.Checks) != 0 {
		t.Errorf("liveness must not run dependency checks, got %v", got.Checks)
	}
}

func TestNewHealthServiceDefaultClock(t *testing.T) {
	svc := NewHealthService("dev", nil, time.Second)
	if svc.now == nil {
		t.Fatal("expected default clock to be set when nil is passed")
	}
}
