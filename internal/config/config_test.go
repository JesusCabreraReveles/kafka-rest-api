package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		assert  func(t *testing.T, c *Config)
	}{
		{
			name: "defaults when no env set",
			env:  nil,
			assert: func(t *testing.T, c *Config) {
				if got, want := c.Server.Addr(), "0.0.0.0:8080"; got != want {
					t.Errorf("addr = %q, want %q", got, want)
				}
				if got, want := c.Server.ShutdownTimeout, 15*time.Second; got != want {
					t.Errorf("shutdown timeout = %s, want %s", got, want)
				}
				if got, want := len(c.Kafka.Brokers), 1; got != want {
					t.Fatalf("brokers len = %d, want %d", got, want)
				}
				if got, want := c.Kafka.Brokers[0], "localhost:9092"; got != want {
					t.Errorf("broker = %q, want %q", got, want)
				}
				if got, want := c.Log.Format, "json"; got != want {
					t.Errorf("log format = %q, want %q", got, want)
				}
			},
		},
		{
			name: "overrides from env",
			env: map[string]string{
				"KRA_SERVER_HOST":             "127.0.0.1",
				"KRA_SERVER_PORT":             "9000",
				"KRA_SERVER_SHUTDOWN_TIMEOUT": "5s",
				"KRA_KAFKA_BROKERS":           "a:9092, b:9092 ,c:9092",
				"KRA_LOG_FORMAT":              "text",
			},
			assert: func(t *testing.T, c *Config) {
				if got, want := c.Server.Addr(), "127.0.0.1:9000"; got != want {
					t.Errorf("addr = %q, want %q", got, want)
				}
				if got, want := c.Server.ShutdownTimeout, 5*time.Second; got != want {
					t.Errorf("shutdown timeout = %s, want %s", got, want)
				}
				if got, want := len(c.Kafka.Brokers), 3; got != want {
					t.Fatalf("brokers len = %d, want %d", got, want)
				}
				if got, want := c.Kafka.Brokers[1], "b:9092"; got != want {
					t.Errorf("broker[1] = %q (whitespace not trimmed?)", got)
				}
			},
		},
		{
			name:    "invalid port integer",
			env:     map[string]string{"KRA_SERVER_PORT": "notanumber"},
			wantErr: true,
		},
		{
			name:    "invalid duration",
			env:     map[string]string{"KRA_SERVER_READ_TIMEOUT": "10furlongs"},
			wantErr: true,
		},
		{
			name:    "port out of range",
			env:     map[string]string{"KRA_SERVER_PORT": "70000"},
			wantErr: true,
		},
		{
			name:    "invalid log format",
			env:     map[string]string{"KRA_LOG_FORMAT": "xml"},
			wantErr: true,
		},
		{
			name:    "invalid security protocol",
			env:     map[string]string{"KRA_KAFKA_SECURITY_PROTOCOL": "carrier-pigeon"},
			wantErr: true,
		},
		{
			name: "sasl without credentials",
			env: map[string]string{
				"KRA_KAFKA_SECURITY_PROTOCOL": "sasl_ssl",
			},
			wantErr: true,
		},
		{
			name: "valid sasl_ssl with credentials",
			env: map[string]string{
				"KRA_KAFKA_SECURITY_PROTOCOL": "sasl_ssl",
				"KRA_KAFKA_SASL_USERNAME":     "svc",
				"KRA_KAFKA_SASL_PASSWORD":     "pw",
			},
			assert: func(t *testing.T, c *Config) {
				if !c.Kafka.Security.usesSASL() {
					t.Error("expected usesSASL to be true for sasl_ssl")
				}
			},
		},
		{
			name: "tls cert without key",
			env: map[string]string{
				"KRA_KAFKA_SECURITY_PROTOCOL": "ssl",
				"KRA_KAFKA_TLS_CERT_FILE":     "/certs/client.pem",
			},
			wantErr: true,
		},
		{
			name: "auth enabled without secret",
			env: map[string]string{
				"KRA_AUTH_ENABLED": "true",
			},
			wantErr: true,
		},
		{
			name: "auth enabled with secret",
			env: map[string]string{
				"KRA_AUTH_ENABLED":    "true",
				"KRA_AUTH_JWT_SECRET": "shhh",
			},
			assert: func(t *testing.T, c *Config) {
				if !c.Auth.Enabled {
					t.Error("expected auth to be enabled")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			cfg, err := Load()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assert != nil {
				tt.assert(t, cfg)
			}
		})
	}
}
