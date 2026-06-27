// Package config loads and validates application configuration from
// environment variables. All settings are explicit and injected into the
// rest of the application; there is no global state.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// envPrefix namespaces every environment variable read by the service.
const envPrefix = "KRA_"

// Config is the fully-resolved application configuration.
type Config struct {
	Server ServerConfig
	Kafka  KafkaConfig
	Log    LogConfig
	Auth   AuthConfig
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Host            string
	Port            int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	HealthTimeout   time.Duration // per-dependency probe budget for /health
}

// Addr returns the host:port the server should bind to.
func (s ServerConfig) Addr() string {
	return fmt.Sprintf("%s:%d", s.Host, s.Port)
}

// KafkaConfig holds Kafka connectivity, producer, and security settings.
type KafkaConfig struct {
	Brokers         []string
	WriteTimeout    time.Duration
	BatchTimeout    time.Duration
	RequiredAcks    string // all | one | none
	MaxBatchSize    int    // maximum messages accepted in a single batch request
	AllowAutoCreate bool   // request broker-side topic auto-creation on publish
	ProduceMode     string // batched (default) | sync (returns per-record offsets)

	AdminTimeout        time.Duration // timeout for metadata / admin calls
	ConsumeDefaultLimit int           // default messages returned by consume
	ConsumeMaxLimit     int           // hard cap on consume limit
	ConsumeDefaultWait  time.Duration // default consume long-poll timeout
	ConsumeMaxWait      time.Duration // hard cap on consume timeout

	Security KafkaSecurity
}

// KafkaSecurity configures how the gateway authenticates to and encrypts its
// connection with the Kafka brokers.
type KafkaSecurity struct {
	// Protocol is one of: plaintext, ssl, sasl_plaintext, sasl_ssl.
	Protocol string

	// SASL settings (used when Protocol is sasl_*).
	SASLMechanism string // scram-sha-256 | scram-sha-512 | plain
	Username      string
	Password      string

	// TLS settings (used when Protocol is ssl or sasl_ssl).
	TLSCAFile     string
	TLSCertFile   string
	TLSKeyFile    string
	TLSServerName string
	TLSSkipVerify bool
}

// LogConfig holds structured-logging settings.
type LogConfig struct {
	Level  string
	Format string
}

// AuthConfig configures optional JWT authentication on the data-plane routes.
// When Enabled is false (the default) the API is open, but the wiring is in
// place so auth can be switched on without code changes.
type AuthConfig struct {
	Enabled   bool
	Algorithm string // hs256 (default) | rs256

	JWTSecret     string // HS256 shared secret
	PublicKeyFile string // RS256 static PEM public key
	JWKSURL       string // RS256 via a JWKS endpoint (key rotation)

	Issuer   string // optional expected "iss" claim
	Audience string // optional expected "aud" claim
}

// Load reads configuration from the environment, applies defaults, and
// validates the result. Every variable is prefixed with KRA_.
func Load() (*Config, error) {
	e := &reader{}

	cfg := &Config{
		Server: ServerConfig{
			Host:            e.str("SERVER_HOST", "0.0.0.0"),
			Port:            e.int("SERVER_PORT", 8080),
			ReadTimeout:     e.dur("SERVER_READ_TIMEOUT", 10*time.Second),
			WriteTimeout:    e.dur("SERVER_WRITE_TIMEOUT", 10*time.Second),
			IdleTimeout:     e.dur("SERVER_IDLE_TIMEOUT", 60*time.Second),
			ShutdownTimeout: e.dur("SERVER_SHUTDOWN_TIMEOUT", 15*time.Second),
			HealthTimeout:   e.dur("SERVER_HEALTH_TIMEOUT", 2*time.Second),
		},
		Kafka: KafkaConfig{
			Brokers:         e.csv("KAFKA_BROKERS", []string{"localhost:9092"}),
			WriteTimeout:    e.dur("KAFKA_WRITE_TIMEOUT", 10*time.Second),
			BatchTimeout:    e.dur("KAFKA_BATCH_TIMEOUT", 10*time.Millisecond),
			RequiredAcks:    e.str("KAFKA_REQUIRED_ACKS", "all"),
			MaxBatchSize:    e.int("KAFKA_MAX_BATCH_SIZE", 10000),
			AllowAutoCreate: e.boolean("KAFKA_ALLOW_AUTO_TOPIC_CREATION", false),
			ProduceMode:     e.str("KAFKA_PRODUCE_MODE", "batched"),

			AdminTimeout:        e.dur("KAFKA_ADMIN_TIMEOUT", 10*time.Second),
			ConsumeDefaultLimit: e.int("KAFKA_CONSUME_DEFAULT_LIMIT", 10),
			ConsumeMaxLimit:     e.int("KAFKA_CONSUME_MAX_LIMIT", 1000),
			ConsumeDefaultWait:  e.dur("KAFKA_CONSUME_DEFAULT_TIMEOUT", 5*time.Second),
			ConsumeMaxWait:      e.dur("KAFKA_CONSUME_MAX_TIMEOUT", 30*time.Second),

			Security: KafkaSecurity{
				Protocol:      e.str("KAFKA_SECURITY_PROTOCOL", "plaintext"),
				SASLMechanism: e.str("KAFKA_SASL_MECHANISM", "scram-sha-256"),
				Username:      e.str("KAFKA_SASL_USERNAME", ""),
				Password:      e.str("KAFKA_SASL_PASSWORD", ""),
				TLSCAFile:     e.str("KAFKA_TLS_CA_FILE", ""),
				TLSCertFile:   e.str("KAFKA_TLS_CERT_FILE", ""),
				TLSKeyFile:    e.str("KAFKA_TLS_KEY_FILE", ""),
				TLSServerName: e.str("KAFKA_TLS_SERVER_NAME", ""),
				TLSSkipVerify: e.boolean("KAFKA_TLS_INSECURE_SKIP_VERIFY", false),
			},
		},
		Log: LogConfig{
			Level:  e.str("LOG_LEVEL", "info"),
			Format: e.str("LOG_FORMAT", "json"),
		},
		Auth: AuthConfig{
			Enabled:       e.boolean("AUTH_ENABLED", false),
			Algorithm:     e.str("AUTH_ALGORITHM", "hs256"),
			JWTSecret:     e.str("AUTH_JWT_SECRET", ""),
			PublicKeyFile: e.str("AUTH_JWT_PUBLIC_KEY_FILE", ""),
			JWKSURL:       e.str("AUTH_JWKS_URL", ""),
			Issuer:        e.str("AUTH_JWT_ISSUER", ""),
			Audience:      e.str("AUTH_JWT_AUDIENCE", ""),
		},
	}

	if err := e.err(); err != nil {
		return nil, fmt.Errorf("parse environment: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	var errs []error

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		errs = append(errs, fmt.Errorf("server port %d out of range (1-65535)", c.Server.Port))
	}
	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, errors.New("at least one kafka broker is required"))
	}
	switch strings.ToLower(c.Kafka.RequiredAcks) {
	case "all", "one", "none":
	default:
		errs = append(errs, fmt.Errorf("invalid kafka required acks %q (want all, one, or none)", c.Kafka.RequiredAcks))
	}
	if c.Kafka.MaxBatchSize < 1 {
		errs = append(errs, fmt.Errorf("kafka max batch size %d must be >= 1", c.Kafka.MaxBatchSize))
	}
	switch strings.ToLower(c.Kafka.ProduceMode) {
	case "batched", "sync":
	default:
		errs = append(errs, fmt.Errorf("invalid kafka produce mode %q (want batched or sync)", c.Kafka.ProduceMode))
	}
	if c.Kafka.ConsumeDefaultLimit < 1 {
		errs = append(errs, fmt.Errorf("kafka consume default limit %d must be >= 1", c.Kafka.ConsumeDefaultLimit))
	}
	if c.Kafka.ConsumeMaxLimit < c.Kafka.ConsumeDefaultLimit {
		errs = append(errs, fmt.Errorf("kafka consume max limit %d must be >= default limit %d",
			c.Kafka.ConsumeMaxLimit, c.Kafka.ConsumeDefaultLimit))
	}
	if c.Kafka.ConsumeMaxWait < c.Kafka.ConsumeDefaultWait {
		errs = append(errs, fmt.Errorf("kafka consume max timeout %s must be >= default timeout %s",
			c.Kafka.ConsumeMaxWait, c.Kafka.ConsumeDefaultWait))
	}
	switch strings.ToLower(c.Log.Format) {
	case string(formatJSON), string(formatText):
	default:
		errs = append(errs, fmt.Errorf("invalid log format %q (want json or text)", c.Log.Format))
	}

	errs = append(errs, c.Kafka.Security.validate()...)
	errs = append(errs, c.Auth.validate()...)

	return errors.Join(errs...)
}

// formatJSON / formatText mirror the logger formats for validation only.
const (
	formatJSON = "json"
	formatText = "text"
)

// usesSASL reports whether the protocol requires SASL credentials.
func (s KafkaSecurity) usesSASL() bool {
	p := strings.ToLower(s.Protocol)
	return p == "sasl_plaintext" || p == "sasl_ssl"
}

func (s KafkaSecurity) validate() []error {
	var errs []error

	switch strings.ToLower(s.Protocol) {
	case "plaintext", "ssl", "sasl_plaintext", "sasl_ssl":
	default:
		errs = append(errs, fmt.Errorf("invalid kafka security protocol %q "+
			"(want plaintext, ssl, sasl_plaintext, or sasl_ssl)", s.Protocol))
		return errs // remaining checks depend on a valid protocol
	}

	if s.usesSASL() {
		switch strings.ToLower(s.SASLMechanism) {
		case "scram-sha-256", "scram-sha-512", "plain":
		default:
			errs = append(errs, fmt.Errorf("invalid kafka sasl mechanism %q "+
				"(want scram-sha-256, scram-sha-512, or plain)", s.SASLMechanism))
		}
		if s.Username == "" || s.Password == "" {
			errs = append(errs, errors.New("kafka sasl username and password are required for sasl_* protocols"))
		}
	}

	if (s.TLSCertFile == "") != (s.TLSKeyFile == "") {
		errs = append(errs, errors.New("kafka tls cert and key files must be set together"))
	}

	return errs
}

func (a AuthConfig) validate() []error {
	if !a.Enabled {
		return nil
	}

	var errs []error
	switch strings.ToLower(a.Algorithm) {
	case "hs256":
		if a.JWTSecret == "" {
			errs = append(errs, errors.New("auth hs256 requires AUTH_JWT_SECRET"))
		}
	case "rs256":
		switch {
		case a.PublicKeyFile == "" && a.JWKSURL == "":
			errs = append(errs, errors.New("auth rs256 requires AUTH_JWT_PUBLIC_KEY_FILE or AUTH_JWKS_URL"))
		case a.PublicKeyFile != "" && a.JWKSURL != "":
			errs = append(errs, errors.New("auth rs256: set only one of AUTH_JWT_PUBLIC_KEY_FILE or AUTH_JWKS_URL"))
		}
	default:
		errs = append(errs, fmt.Errorf("invalid auth algorithm %q (want hs256 or rs256)", a.Algorithm))
	}
	return errs
}

// reader pulls typed values from the environment, accumulating parse errors so
// the caller can report every misconfiguration at once.
type reader struct {
	errs []error
}

func (r *reader) lookup(key string) (string, bool) {
	v, ok := os.LookupEnv(envPrefix + key)
	if !ok {
		return "", false
	}
	return v, v != ""
}

func (r *reader) str(key, def string) string {
	if v, ok := r.lookup(key); ok {
		return v
	}
	return def
}

func (r *reader) int(key string, def int) int {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s%s: invalid integer %q: %w", envPrefix, key, v, err))
		return def
	}
	return n
}

func (r *reader) dur(key string, def time.Duration) time.Duration {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s%s: invalid duration %q: %w", envPrefix, key, v, err))
		return def
	}
	return d
}

func (r *reader) boolean(key string, def bool) bool {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		r.errs = append(r.errs, fmt.Errorf("%s%s: invalid boolean %q: %w", envPrefix, key, v, err))
		return def
	}
	return b
}

func (r *reader) csv(key string, def []string) []string {
	v, ok := r.lookup(key)
	if !ok {
		return def
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

func (r *reader) err() error {
	return errors.Join(r.errs...)
}
