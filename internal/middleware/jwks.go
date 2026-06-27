package middleware

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// JWKS resolves RSA verification keys from a JSON Web Key Set endpoint. Keys are
// cached and refreshed lazily: a cache entry is reused while fresh, and a lookup
// for an unknown key id triggers a refresh (handling key rotation). On a refresh
// failure a cached key is still served if available.
type JWKS struct {
	url        string
	client     *http.Client
	ttl        time.Duration // how long a fetched set is considered fresh
	minRefresh time.Duration // floor between refreshes, to avoid hammering

	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time
}

// JWKSOption customizes a JWKS.
type JWKSOption func(*JWKS)

// WithHTTPClient overrides the HTTP client used to fetch the key set.
func WithHTTPClient(c *http.Client) JWKSOption {
	return func(j *JWKS) { j.client = c }
}

// WithTTL sets how long a fetched key set is considered fresh.
func WithTTL(d time.Duration) JWKSOption {
	return func(j *JWKS) { j.ttl = d }
}

// WithMinRefresh sets the minimum interval between refreshes (rate limiting
// against repeated unknown-key lookups).
func WithMinRefresh(d time.Duration) JWKSOption {
	return func(j *JWKS) { j.minRefresh = d }
}

// NewJWKS constructs a JWKS resolver for the given endpoint URL.
func NewJWKS(url string, opts ...JWKSOption) *JWKS {
	j := &JWKS{
		url:        url,
		client:     &http.Client{Timeout: 10 * time.Second},
		ttl:        5 * time.Minute,
		minRefresh: 30 * time.Second,
		keys:       make(map[string]*rsa.PublicKey),
	}
	for _, opt := range opts {
		opt(j)
	}
	return j
}

// Keyfunc is a jwt.Keyfunc that selects the RSA key matching the token's "kid".
func (j *JWKS) Keyfunc(token *jwt.Token) (any, error) {
	kid, _ := token.Header["kid"].(string)
	return j.key(kid)
}

func (j *JWKS) key(kid string) (*rsa.PublicKey, error) {
	j.mu.RLock()
	cached, ok := j.keys[kid]
	fresh := time.Since(j.fetchedAt) < j.ttl
	j.mu.RUnlock()

	if ok && fresh {
		return cached, nil
	}

	// Unknown kid or stale set: refresh. Serve a stale key if the refresh fails.
	if err := j.refresh(); err != nil {
		if ok {
			return cached, nil
		}
		return nil, err
	}

	j.mu.RLock()
	key, ok := j.keys[kid]
	j.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no JWKS key for kid %q", kid)
	}
	return key, nil
}

func (j *JWKS) refresh() error {
	j.mu.RLock()
	tooSoon := len(j.keys) > 0 && time.Since(j.fetchedAt) < j.minRefresh
	j.mu.RUnlock()
	if tooSoon {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), j.client.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, j.url, nil)
	if err != nil {
		return fmt.Errorf("build jwks request: %w", err)
	}
	resp, err := j.client.Do(req)
	if err != nil {
		return fmt.Errorf("fetch jwks: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch jwks: unexpected status %d", resp.StatusCode)
	}

	var doc jwksDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return fmt.Errorf("decode jwks: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, k := range doc.Keys {
		if !k.isRSASignatureKey() {
			continue
		}
		pub, err := k.rsaPublicKey()
		if err != nil {
			return fmt.Errorf("jwks key %q: %w", k.Kid, err)
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return fmt.Errorf("jwks at %s contained no usable RSA keys", j.url)
	}

	j.mu.Lock()
	j.keys = keys
	j.fetchedAt = time.Now()
	j.mu.Unlock()
	return nil
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

func (k jwk) isRSASignatureKey() bool {
	return k.Kty == "RSA" && (k.Use == "" || k.Use == "sig")
}

func (k jwk) rsaPublicKey() (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("decode modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("decode exponent: %w", err)
	}
	if len(nBytes) == 0 || len(eBytes) == 0 {
		return nil, fmt.Errorf("empty modulus or exponent")
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(new(big.Int).SetBytes(eBytes).Int64()),
	}, nil
}
