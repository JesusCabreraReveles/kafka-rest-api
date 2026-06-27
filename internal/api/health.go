package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// HealthReporter reports application liveness and readiness. It is defined here,
// at the point of use, so the API layer depends on an abstraction rather than a
// concrete type (Dependency Inversion). The service layer satisfies it.
type HealthReporter interface {
	Live() service.HealthStatus
	Ready(ctx context.Context) service.HealthStatus
}

// HealthHandler serves the GET /health (liveness) and GET /ready (readiness)
// endpoints.
type HealthHandler struct {
	reporter HealthReporter
	logger   *slog.Logger
}

// NewHealthHandler wires a HealthHandler to its dependencies.
func NewHealthHandler(reporter HealthReporter, logger *slog.Logger) *HealthHandler {
	return &HealthHandler{reporter: reporter, logger: logger}
}

type healthCheck struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type healthResponse struct {
	Status        string        `json:"status"`
	Version       string        `json:"version"`
	UptimeSeconds float64       `json:"uptime_seconds"`
	Checks        []healthCheck `json:"checks,omitempty"`
}

// Live responds to liveness probes. It reflects only whether the process is
// serving and therefore always returns 200 while the server is up.
func (h *HealthHandler) Live(w http.ResponseWriter, _ *http.Request) {
	h.respond(w, h.reporter.Live())
}

// Ready responds to readiness probes. A non-ok overall status (e.g. a failed
// dependency probe) is reported as 503 so orchestrators can stop routing traffic.
func (h *HealthHandler) Ready(w http.ResponseWriter, r *http.Request) {
	h.respond(w, h.reporter.Ready(r.Context()))
}

func (h *HealthHandler) respond(w http.ResponseWriter, status service.HealthStatus) {
	code := http.StatusOK
	if status.Status != service.StatusOK {
		code = http.StatusServiceUnavailable
	}

	var checks []healthCheck
	if len(status.Checks) > 0 {
		checks = make([]healthCheck, len(status.Checks))
		for i, c := range status.Checks {
			checks[i] = healthCheck{Name: c.Name, Status: c.Status, Error: c.Error}
		}
	}

	respondJSON(w, h.logger, code, healthResponse{
		Status:        status.Status,
		Version:       status.Version,
		UptimeSeconds: status.Uptime.Seconds(),
		Checks:        checks,
	})
}
