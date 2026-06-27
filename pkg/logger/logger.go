// Package logger provides a thin constructor around the standard library's
// structured logger (slog), configured from application settings. Keeping this
// in pkg/ lets other binaries or tools reuse the same logging conventions.
package logger

import (
	"io"
	"log/slog"
	"strings"
)

// Format identifies the output encoding for log records.
type Format string

const (
	// FormatJSON emits one JSON object per log record (machine friendly).
	FormatJSON Format = "json"
	// FormatText emits key=value pairs (human friendly, for local dev).
	FormatText Format = "text"
)

// New builds a *slog.Logger writing to w using the provided level and format.
// Unknown levels fall back to Info; unknown formats fall back to JSON.
func New(w io.Writer, level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var handler slog.Handler
	switch Format(strings.ToLower(format)) {
	case FormatText:
		handler = slog.NewTextHandler(w, opts)
	default:
		handler = slog.NewJSONHandler(w, opts)
	}

	return slog.New(handler)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
