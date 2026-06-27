package middleware

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// rsaJWK builds the JWKS JSON entry for an RSA public key.
func rsaJWK(kid string, pub *rsa.PublicKey) jwk {
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	return jwk{
		Kty: "RSA",
		Kid: kid,
		Use: "sig",
		Alg: "RS256",
		N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(eBytes),
	}
}

// jwksServer serves a JWKS document that can be swapped to simulate rotation.
type jwksServer struct {
	*httptest.Server
	doc   atomic.Pointer[jwksDocument]
	calls atomic.Int64
}

func newJWKSServer(initial jwksDocument) *jwksServer {
	js := &jwksServer{}
	js.doc.Store(&initial)
	js.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		js.calls.Add(1)
		_ = json.NewEncoder(w).Encode(js.doc.Load())
	}))
	return js
}

func signRS256(t *testing.T, key *rsa.PrivateKey, kid string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Subject:   "user-1",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	tok.Header["kid"] = kid
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign rs256: %v", err)
	}
	return s
}

func parseWith(kf jwt.Keyfunc, token string) error {
	_, err := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"})).
		ParseWithClaims(token, &jwt.RegisteredClaims{}, kf)
	return err
}

func TestJWKSResolvesAndValidates(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	srv := newJWKSServer(jwksDocument{Keys: []jwk{rsaJWK("key-1", &key.PublicKey)}})
	defer srv.Close()

	jwks := NewJWKS(srv.URL, WithMinRefresh(0))

	// A token signed with the published key validates.
	if err := parseWith(jwks.Keyfunc, signRS256(t, key, "key-1")); err != nil {
		t.Fatalf("expected valid token, got %v", err)
	}

	// An unknown kid is rejected.
	if err := parseWith(jwks.Keyfunc, signRS256(t, key, "missing")); err == nil {
		t.Error("expected error for unknown kid")
	}
}

func TestJWKSPicksUpRotation(t *testing.T) {
	key1, _ := rsa.GenerateKey(rand.Reader, 2048)
	key2, _ := rsa.GenerateKey(rand.Reader, 2048)

	srv := newJWKSServer(jwksDocument{Keys: []jwk{rsaJWK("key-1", &key1.PublicKey)}})
	defer srv.Close()

	jwks := NewJWKS(srv.URL, WithMinRefresh(0))

	if err := parseWith(jwks.Keyfunc, signRS256(t, key1, "key-1")); err != nil {
		t.Fatalf("key-1 should validate: %v", err)
	}

	// Rotate: the endpoint now serves a different key under a new kid.
	srv.doc.Store(&jwksDocument{Keys: []jwk{rsaJWK("key-2", &key2.PublicKey)}})

	if err := parseWith(jwks.Keyfunc, signRS256(t, key2, "key-2")); err != nil {
		t.Errorf("rotated key-2 should validate after refresh: %v", err)
	}
}

func TestJWKSServesStaleOnFetchFailure(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := newJWKSServer(jwksDocument{Keys: []jwk{rsaJWK("key-1", &key.PublicKey)}})

	jwks := NewJWKS(srv.URL, WithMinRefresh(0), WithTTL(0)) // TTL 0 => always tries to refresh

	// Prime the cache.
	if err := parseWith(jwks.Keyfunc, signRS256(t, key, "key-1")); err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Endpoint goes down; the cached key must still validate.
	srv.Close()
	if err := parseWith(jwks.Keyfunc, signRS256(t, key, "key-1")); err != nil {
		t.Errorf("expected stale key to still validate, got %v", err)
	}
}

func TestNewKeyfuncRS256StaticFile(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	path := filepath.Join(t.TempDir(), "pub.pem")
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	kf, algs, err := NewKeyfunc(KeyfuncConfig{Algorithm: "rs256", PublicKeyFile: path})
	if err != nil {
		t.Fatalf("new keyfunc: %v", err)
	}
	if len(algs) != 1 || algs[0] != "RS256" {
		t.Errorf("algs = %v, want [RS256]", algs)
	}
	if err := parseWith(kf, signRS256(t, key, "ignored")); err != nil {
		t.Errorf("static RS256 token should validate: %v", err)
	}
}

func TestNewKeyfuncErrors(t *testing.T) {
	if _, _, err := NewKeyfunc(KeyfuncConfig{Algorithm: "es512"}); err == nil {
		t.Error("expected error for unsupported algorithm")
	}
	if _, _, err := NewKeyfunc(KeyfuncConfig{Algorithm: "rs256", PublicKeyFile: "/no/such/key.pem"}); err == nil {
		t.Error("expected error for missing public key file")
	}
}
