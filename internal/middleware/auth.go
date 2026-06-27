package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// AuthConfig configures the JWT authentication middleware. The verification key
// is supplied as a jwt.Keyfunc, so the same middleware serves HS256, RS256 with
// a static key, or RS256 backed by a rotating JWKS (built via NewKeyfunc).
type AuthConfig struct {
	Keyfunc    jwt.Keyfunc // resolves the verification key for a token
	Algorithms []string    // accepted signing methods, e.g. ["HS256"] or ["RS256"]
	Issuer     string      // optional expected "iss" claim
	Audience   string      // optional expected "aud" claim
}

type contextKey string

const subjectKey contextKey = "auth.subject"

// Subject returns the authenticated subject ("sub" claim) from the context, or
// an empty string if the request was not authenticated.
func Subject(ctx context.Context) string {
	v, _ := ctx.Value(subjectKey).(string)
	return v
}

// JWTAuth returns middleware that validates a bearer JWT on each request using
// the configured key resolver and accepted algorithms. On success the token's
// subject is stored in the request context; on failure it responds 401 with the
// standard error envelope.
func JWTAuth(cfg AuthConfig, logger *slog.Logger) func(http.Handler) http.Handler {
	opts := []jwt.ParserOption{jwt.WithValidMethods(cfg.Algorithms)}
	if cfg.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	}
	if cfg.Audience != "" {
		opts = append(opts, jwt.WithAudience(cfg.Audience))
	}
	parser := jwt.NewParser(opts...)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, err := bearerToken(r)
			if err != nil {
				writeAuthError(w, logger, err.Error())
				return
			}

			var claims jwt.RegisteredClaims
			if _, err := parser.ParseWithClaims(raw, &claims, cfg.Keyfunc); err != nil {
				logger.Debug("rejected token", slog.Any("error", err))
				writeAuthError(w, logger, "invalid or expired token")
				return
			}

			ctx := context.WithValue(r.Context(), subjectKey, claims.Subject)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func bearerToken(r *http.Request) (string, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return "", errors.New("missing Authorization header")
	}
	scheme, token, found := strings.Cut(h, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", errors.New("Authorization header must be 'Bearer <token>'")
	}
	return strings.TrimSpace(token), nil
}

// authError mirrors the api package's error envelope shape, kept local to avoid
// a transport-layer import cycle.
type authError struct {
	Error authErrorBody `json:"error"`
}

type authErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeAuthError(w http.ResponseWriter, logger *slog.Logger, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusUnauthorized)
	if err := json.NewEncoder(w).Encode(authError{Error: authErrorBody{Code: "unauthorized", Message: message}}); err != nil {
		logger.Error("failed to encode auth error", slog.Any("error", err))
	}
}
