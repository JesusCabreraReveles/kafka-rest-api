package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	segkafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

// SecurityConfig describes how to authenticate to and encrypt the connection
// with the Kafka brokers. It maps directly from config.KafkaSecurity.
type SecurityConfig struct {
	Protocol      string // plaintext | ssl | sasl_plaintext | sasl_ssl
	SASLMechanism string // scram-sha-256 | scram-sha-512 | plain
	Username      string
	Password      string
	TLSCAFile     string
	TLSCertFile   string
	TLSKeyFile    string
	TLSServerName string
	TLSSkipVerify bool
}

func (s SecurityConfig) usesSASL() bool {
	p := strings.ToLower(s.Protocol)
	return p == "sasl_plaintext" || p == "sasl_ssl"
}

func (s SecurityConfig) usesTLS() bool {
	p := strings.ToLower(s.Protocol)
	return p == "ssl" || p == "sasl_ssl"
}

// mechanism returns the SASL mechanism, or nil when SASL is not in use.
func (s SecurityConfig) mechanism() (sasl.Mechanism, error) {
	if !s.usesSASL() {
		return nil, nil
	}
	switch strings.ToLower(s.SASLMechanism) {
	case "scram-sha-256":
		return scram.Mechanism(scram.SHA256, s.Username, s.Password)
	case "scram-sha-512":
		return scram.Mechanism(scram.SHA512, s.Username, s.Password)
	case "plain":
		return plain.Mechanism{Username: s.Username, Password: s.Password}, nil
	default:
		return nil, fmt.Errorf("unsupported sasl mechanism %q", s.SASLMechanism)
	}
}

// tlsConfig returns the TLS configuration, or nil when TLS is not in use.
func (s SecurityConfig) tlsConfig() (*tls.Config, error) {
	if !s.usesTLS() {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         s.TLSServerName,
		InsecureSkipVerify: s.TLSSkipVerify, //nolint:gosec // opt-in via config for self-signed dev clusters
	}

	if s.TLSCAFile != "" {
		pem, err := os.ReadFile(s.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read kafka tls ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka tls ca %q contains no certificates", s.TLSCAFile)
		}
		cfg.RootCAs = pool
	}

	if s.TLSCertFile != "" && s.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(s.TLSCertFile, s.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load kafka tls keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	return cfg, nil
}

// transport builds a kafka.Transport with SASL/TLS applied. It returns nil when
// neither is configured, so callers fall back to the default transport.
func (s SecurityConfig) transport() (*segkafka.Transport, error) {
	mech, tlsCfg, err := s.build()
	if err != nil {
		return nil, err
	}
	if mech == nil && tlsCfg == nil {
		return nil, nil
	}
	return &segkafka.Transport{SASL: mech, TLS: tlsCfg}, nil
}

// dialer builds a kafka.Dialer with SASL/TLS applied, for use by readers. It
// returns nil when neither is configured.
func (s SecurityConfig) dialer(timeout time.Duration) (*segkafka.Dialer, error) {
	mech, tlsCfg, err := s.build()
	if err != nil {
		return nil, err
	}
	if mech == nil && tlsCfg == nil {
		return nil, nil
	}
	return &segkafka.Dialer{
		Timeout:       timeout,
		DualStack:     true,
		SASLMechanism: mech,
		TLS:           tlsCfg,
	}, nil
}

func (s SecurityConfig) build() (sasl.Mechanism, *tls.Config, error) {
	mech, err := s.mechanism()
	if err != nil {
		return nil, nil, err
	}
	tlsCfg, err := s.tlsConfig()
	if err != nil {
		return nil, nil, err
	}
	return mech, tlsCfg, nil
}
