package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
)

// errBadRequest marks handler-level client errors (e.g. malformed JSON) that
// should map to 400 but are not domain validation errors.
var errBadRequest = errors.New("bad request")

// errorResponse is the canonical error envelope returned to clients.
type errorResponse struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeError serializes err to the standard error envelope, choosing an HTTP
// status code based on the error's classification.
func writeError(w http.ResponseWriter, logger *slog.Logger, err error) {
	status, code := classify(err)

	// Server-side faults are worth logging; client faults are not.
	if status >= http.StatusInternalServerError || status == http.StatusBadGateway || status == http.StatusGatewayTimeout {
		logger.Error("request failed", slog.Int("status", status), slog.String("code", code), slog.Any("error", err))
	}

	respondJSON(w, logger, status, errorResponse{Error: errorBody{Code: code, Message: err.Error()}})
}

// classify maps an error to an HTTP status and a stable machine-readable code.
func classify(err error) (status int, code string) {
	switch {
	case errors.Is(err, service.ErrEmptyTopic),
		errors.Is(err, service.ErrEmptyValue),
		errors.Is(err, service.ErrNoMessages),
		errors.Is(err, errBadRequest):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, service.ErrTopicNotFound):
		return http.StatusNotFound, "topic_not_found"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "kafka_timeout"
	case errors.Is(err, context.Canceled):
		// Client went away; nothing useful to return, but be explicit.
		return 499, "client_closed_request"
	default:
		// Anything else is an upstream (Kafka) failure from the gateway's view.
		return http.StatusBadGateway, "kafka_error"
	}
}
