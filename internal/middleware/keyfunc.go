package middleware

import (
	"crypto/rsa"
	"fmt"
	"os"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

// KeyfuncConfig describes how to build a JWT verification key resolver.
type KeyfuncConfig struct {
	Algorithm     string // hs256 | rs256
	Secret        string // HS256 shared secret
	PublicKeyFile string // RS256 static PEM public key
	JWKSURL       string // RS256 via a JWKS endpoint
}

// NewKeyfunc builds a jwt.Keyfunc and the list of accepted signing algorithms
// for the configured strategy. Supported:
//   - hs256: shared secret.
//   - rs256 with a static PEM public key file.
//   - rs256 backed by a rotating JWKS endpoint.
func NewKeyfunc(cfg KeyfuncConfig) (jwt.Keyfunc, []string, error) {
	switch strings.ToLower(cfg.Algorithm) {
	case "", "hs256":
		secret := []byte(cfg.Secret)
		return func(*jwt.Token) (any, error) { return secret, nil }, []string{"HS256"}, nil

	case "rs256":
		if cfg.JWKSURL != "" {
			return NewJWKS(cfg.JWKSURL).Keyfunc, []string{"RS256"}, nil
		}
		pub, err := loadRSAPublicKey(cfg.PublicKeyFile)
		if err != nil {
			return nil, nil, err
		}
		return func(*jwt.Token) (any, error) { return pub, nil }, []string{"RS256"}, nil

	default:
		return nil, nil, fmt.Errorf("unsupported jwt algorithm %q (want hs256 or rs256)", cfg.Algorithm)
	}
}

func loadRSAPublicKey(path string) (*rsa.PublicKey, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read jwt public key: %w", err)
	}
	pub, err := jwt.ParseRSAPublicKeyFromPEM(pem)
	if err != nil {
		return nil, fmt.Errorf("parse jwt public key: %w", err)
	}
	return pub, nil
}
