package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMechanismSelection(t *testing.T) {
	tests := []struct {
		name     string
		sec      SecurityConfig
		wantName string // "" => nil mechanism
		wantErr  bool
	}{
		{
			name: "plaintext has no mechanism",
			sec:  SecurityConfig{Protocol: "plaintext"},
		},
		{
			name:     "scram-sha-256",
			sec:      SecurityConfig{Protocol: "sasl_ssl", SASLMechanism: "scram-sha-256", Username: "u", Password: "p"},
			wantName: "SCRAM-SHA-256",
		},
		{
			name:     "scram-sha-512",
			sec:      SecurityConfig{Protocol: "sasl_plaintext", SASLMechanism: "scram-sha-512", Username: "u", Password: "p"},
			wantName: "SCRAM-SHA-512",
		},
		{
			name:     "plain",
			sec:      SecurityConfig{Protocol: "sasl_plaintext", SASLMechanism: "plain", Username: "u", Password: "p"},
			wantName: "PLAIN",
		},
		{
			name:    "unknown mechanism",
			sec:     SecurityConfig{Protocol: "sasl_ssl", SASLMechanism: "kerberos", Username: "u", Password: "p"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mech, err := tt.sec.mechanism()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantName == "" {
				if mech != nil {
					t.Errorf("expected nil mechanism, got %v", mech.Name())
				}
				return
			}
			if mech == nil || mech.Name() != tt.wantName {
				t.Errorf("mechanism = %v, want %q", mech, tt.wantName)
			}
		})
	}
}

func TestTransportAndDialerPerProtocol(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		wantNil  bool
		wantSASL bool
		wantTLS  bool
	}{
		{name: "plaintext: default transport", protocol: "plaintext", wantNil: true},
		{name: "ssl: tls only", protocol: "ssl", wantTLS: true},
		{name: "sasl_plaintext: sasl only", protocol: "sasl_plaintext", wantSASL: true},
		{name: "sasl_ssl: both", protocol: "sasl_ssl", wantSASL: true, wantTLS: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sec := SecurityConfig{
				Protocol:      tt.protocol,
				SASLMechanism: "scram-sha-256",
				Username:      "u",
				Password:      "p",
				TLSSkipVerify: true,
			}

			tr, err := sec.transport()
			if err != nil {
				t.Fatalf("transport error: %v", err)
			}
			if tt.wantNil {
				if tr != nil {
					t.Fatal("expected nil transport for plaintext")
				}
				return
			}
			if tr == nil {
				t.Fatal("expected non-nil transport")
			}
			if (tr.SASL != nil) != tt.wantSASL {
				t.Errorf("SASL present = %v, want %v", tr.SASL != nil, tt.wantSASL)
			}
			if (tr.TLS != nil) != tt.wantTLS {
				t.Errorf("TLS present = %v, want %v", tr.TLS != nil, tt.wantTLS)
			}

			d, err := sec.dialer(time.Second)
			if err != nil {
				t.Fatalf("dialer error: %v", err)
			}
			if d == nil {
				t.Fatal("expected non-nil dialer")
			}
		})
	}
}

func TestTLSConfig(t *testing.T) {
	t.Run("skip verify and min version", func(t *testing.T) {
		cfg, err := SecurityConfig{Protocol: "ssl", TLSSkipVerify: true}.tlsConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("min version = %x, want TLS1.2", cfg.MinVersion)
		}
		if !cfg.InsecureSkipVerify {
			t.Error("expected InsecureSkipVerify to be true")
		}
	})

	t.Run("valid CA file loads root pool", func(t *testing.T) {
		caPath := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(caPath, selfSignedCertPEM(t), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := SecurityConfig{Protocol: "ssl", TLSCAFile: caPath}.tlsConfig()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.RootCAs == nil {
			t.Error("expected RootCAs to be set")
		}
	})

	t.Run("junk CA file rejected", func(t *testing.T) {
		caPath := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(caPath, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (SecurityConfig{Protocol: "ssl", TLSCAFile: caPath}).tlsConfig(); err == nil {
			t.Error("expected error for junk CA file")
		}
	})

	t.Run("missing CA file rejected", func(t *testing.T) {
		if _, err := (SecurityConfig{Protocol: "ssl", TLSCAFile: "/no/such/ca.pem"}).tlsConfig(); err == nil {
			t.Error("expected error for missing CA file")
		}
	})
}

// selfSignedCertPEM generates a throwaway self-signed certificate in PEM form.
func selfSignedCertPEM(t *testing.T) []byte {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
