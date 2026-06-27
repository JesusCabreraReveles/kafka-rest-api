package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// stubReporter is a hand-written test double implementing HealthReporter.
type stubReporter struct {
	live  service.HealthStatus
	ready service.HealthStatus
}

func (s stubReporter) Live() service.HealthStatus                   { return s.live }
func (s stubReporter) Ready(_ context.Context) service.HealthStatus { return s.ready }

func TestHealthLive(t *testing.T) {
	// Liveness ignores dependencies and always reports 200 while serving.
	h := NewHealthHandler(stubReporter{
		live: service.HealthStatus{Status: service.StatusOK, Version: "v1", Uptime: 30 * time.Second},
	}, newTestLogger())

	rec := httptest.NewRecorder()
	h.Live(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("content-type = %q", ct)
	}

	var body healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != service.StatusOK {
		t.Errorf("status = %q, want %q", body.Status, service.StatusOK)
	}
	if len(body.Checks) != 0 {
		t.Errorf("liveness should not include dependency checks, got %v", body.Checks)
	}
}

func TestHealthReady(t *testing.T) {
	tests := []struct {
		name       string
		ready      service.HealthStatus
		wantCode   int
		wantStatus string
	}{
		{
			name: "ready returns 200",
			ready: service.HealthStatus{
				Status: service.StatusOK, Version: "v1",
				Checks: []service.DependencyStatus{{Name: "kafka", Status: "ok"}},
			},
			wantCode:   http.StatusOK,
			wantStatus: service.StatusOK,
		},
		{
			name: "degraded returns 503",
			ready: service.HealthStatus{
				Status: service.StatusDegraded, Version: "v1",
				Checks: []service.DependencyStatus{{Name: "kafka", Status: "failed", Error: "refused"}},
			},
			wantCode:   http.StatusServiceUnavailable,
			wantStatus: service.StatusDegraded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHealthHandler(stubReporter{ready: tt.ready}, newTestLogger())

			rec := httptest.NewRecorder()
			h.Ready(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d", rec.Code, tt.wantCode)
			}

			var body healthResponse
			if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body.Status != tt.wantStatus {
				t.Errorf("status = %q, want %q", body.Status, tt.wantStatus)
			}
			if len(body.Checks) != 1 {
				t.Errorf("expected one check, got %v", body.Checks)
			}
		})
	}
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
