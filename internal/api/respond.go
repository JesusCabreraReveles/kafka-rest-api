package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// respondJSON writes payload as JSON with the given status code. A nil payload
// writes only the status line and headers (useful for 204 responses).
func respondJSON(w http.ResponseWriter, logger *slog.Logger, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if payload == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// The status/headers are already written, so we can only log here.
		logger.Error("failed to encode JSON response", slog.Any("error", err))
	}
}
