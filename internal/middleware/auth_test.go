package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// testSecret is the HMAC key used to sign JWTs in these tests. It is generated
// at init time rather than hardcoded, so no credential-shaped literal is
// committed to the repository.
var testSecret = newTestSecret()

func newTestSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("generate test secret: " + err.Error())
	}
	return hex.EncodeToString(b)
}

func sign(t *testing.T, claims jwt.RegisteredClaims, secret string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestJWTAuth(t *testing.T) {
	now := time.Now()
	valid := jwt.RegisteredClaims{
		Subject:   "user-1",
		Issuer:    "kra",
		Audience:  jwt.ClaimStrings{"clients"},
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Hour)),
	}

	tests := []struct {
		name        string
		authHeader  string
		wantCode    int
		wantSubject string
	}{
		{
			name:        "valid token passes",
			authHeader:  "Bearer " + signWith(t, valid, testSecret),
			wantCode:    http.StatusOK,
			wantSubject: "user-1",
		},
		{
			name:       "missing header rejected",
			authHeader: "",
			wantCode:   http.StatusUnauthorized,
		},
		{
			name:       "malformed scheme rejected",
			authHeader: "Token abc.def.ghi",
			wantCode:   http.StatusUnauthorized,
		},
		{
			name: "expired token rejected",
			authHeader: "Bearer " + signWith(t, jwt.RegisteredClaims{
				Subject:   "user-1",
				Issuer:    "kra",
				Audience:  jwt.ClaimStrings{"clients"},
				ExpiresAt: jwt.NewNumericDate(now.Add(-time.Hour)),
			}, testSecret),
			wantCode: http.StatusUnauthorized,
		},
		{
			name:       "wrong signature rejected",
			authHeader: "Bearer " + signWith(t, valid, "the-wrong-secret"),
			wantCode:   http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotSubject string
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotSubject = Subject(r.Context())
				w.WriteHeader(http.StatusOK)
			})

			mw := JWTAuth(hmacAuthConfig(t, "kra", "clients"), testLogger())

			req := httptest.NewRequest(http.MethodGet, "/topics", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			rec := httptest.NewRecorder()
			mw(next).ServeHTTP(rec, req)

			if rec.Code != tt.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantCode == http.StatusUnauthorized {
				var body authError
				if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if body.Error.Code != "unauthorized" {
					t.Errorf("error code = %q, want unauthorized", body.Error.Code)
				}
			}
			if gotSubject != tt.wantSubject {
				t.Errorf("subject = %q, want %q", gotSubject, tt.wantSubject)
			}
		})
	}
}

func TestJWTAuthRejectsWrongIssuer(t *testing.T) {
	claims := jwt.RegisteredClaims{
		Subject:   "user-1",
		Issuer:    "evil",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	mw := JWTAuth(hmacAuthConfig(t, "kra", ""), testLogger())

	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	req.Header.Set("Authorization", "Bearer "+signWith(t, claims, testSecret))
	rec := httptest.NewRecorder()
	mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code = %d, want 401", rec.Code)
	}
}

// signWith is a thin wrapper so the test table can build tokens inline.
func signWith(t *testing.T, claims jwt.RegisteredClaims, secret string) string {
	return sign(t, claims, secret)
}

// hmacAuthConfig builds an HS256 AuthConfig via the production NewKeyfunc path.
func hmacAuthConfig(t *testing.T, issuer, audience string) AuthConfig {
	t.Helper()
	kf, algs, err := NewKeyfunc(KeyfuncConfig{Algorithm: "hs256", Secret: testSecret})
	if err != nil {
		t.Fatalf("new keyfunc: %v", err)
	}
	return AuthConfig{Keyfunc: kf, Algorithms: algs, Issuer: issuer, Audience: audience}
}
