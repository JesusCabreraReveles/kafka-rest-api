// Command server is the entrypoint for the Kafka REST API. It wires
// configuration, logging, the service layer, and the HTTP server together, and
// manages a graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	swgui "github.com/swaggest/swgui/v5emb"

	"github.com/JesusCabreraReveles/kafka-rest-api/docs"
	"github.com/JesusCabreraReveles/kafka-rest-api/internal/api"
	"github.com/JesusCabreraReveles/kafka-rest-api/internal/config"
	"github.com/JesusCabreraReveles/kafka-rest-api/internal/kafka"
	"github.com/JesusCabreraReveles/kafka-rest-api/internal/metrics"
	"github.com/JesusCabreraReveles/kafka-rest-api/internal/middleware"
	"github.com/JesusCabreraReveles/kafka-rest-api/internal/service"
	"github.com/JesusCabreraReveles/kafka-rest-api/pkg/logger"
)

// maxPublishBodyBytes caps the size of a publish request body (defense against
// oversized payloads). 16 MiB comfortably exceeds Kafka's default message size.
const maxPublishBodyBytes = 16 << 20

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		// The logger may not be initialized when config loading fails, so we
		// fall back to stderr for the fatal path.
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(os.Stdout, cfg.Log.Level, cfg.Log.Format)
	log.Info("starting kafka-rest-api",
		"version", version,
		"addr", cfg.Server.Addr(),
	)

	security := kafka.SecurityConfig{
		Protocol:      cfg.Kafka.Security.Protocol,
		SASLMechanism: cfg.Kafka.Security.SASLMechanism,
		Username:      cfg.Kafka.Security.Username,
		Password:      cfg.Kafka.Security.Password,
		TLSCAFile:     cfg.Kafka.Security.TLSCAFile,
		TLSCertFile:   cfg.Kafka.Security.TLSCertFile,
		TLSKeyFile:    cfg.Kafka.Security.TLSKeyFile,
		TLSServerName: cfg.Kafka.Security.TLSServerName,
		TLSSkipVerify: cfg.Kafka.Security.TLSSkipVerify,
	}
	log.Info("kafka security", "protocol", cfg.Kafka.Security.Protocol)

	producer, closeProducer, err := newProducer(cfg.Kafka, security)
	if err != nil {
		return fmt.Errorf("init kafka producer: %w", err)
	}
	defer func() {
		if cerr := closeProducer(); cerr != nil {
			log.Error("closing kafka producer", "error", cerr)
		}
	}()
	log.Info("kafka producer", "mode", cfg.Kafka.ProduceMode)

	client, err := kafka.NewClient(kafka.ClientConfig{
		Brokers:      cfg.Kafka.Brokers,
		AdminTimeout: cfg.Kafka.AdminTimeout,
		Security:     security,
	})
	if err != nil {
		return fmt.Errorf("init kafka client: %w", err)
	}

	// Observability: a dedicated registry (with Go/process collectors) and the
	// application metrics that decorate the Kafka use cases.
	registry := metrics.NewRegistry()
	appMetrics := metrics.New(registry)

	healthSvc := service.NewHealthService(version, time.Now, cfg.Server.HealthTimeout, client)
	publisherSvc := service.NewPublisherService(metrics.NewInstrumentedProducer(producer, appMetrics), log)
	consumerSvc := service.NewConsumerService(metrics.NewInstrumentedReader(client, appMetrics), service.ConsumerConfig{
		DefaultLimit: cfg.Kafka.ConsumeDefaultLimit,
		MaxLimit:     cfg.Kafka.ConsumeMaxLimit,
		DefaultWait:  cfg.Kafka.ConsumeDefaultWait,
		MaxWait:      cfg.Kafka.ConsumeMaxWait,
	}, log)
	topicSvc := service.NewTopicService(metrics.NewInstrumentedAdmin(client, appMetrics), log)

	var authMiddleware func(http.Handler) http.Handler
	if cfg.Auth.Enabled {
		keyfunc, algorithms, kerr := middleware.NewKeyfunc(middleware.KeyfuncConfig{
			Algorithm:     cfg.Auth.Algorithm,
			Secret:        cfg.Auth.JWTSecret,
			PublicKeyFile: cfg.Auth.PublicKeyFile,
			JWKSURL:       cfg.Auth.JWKSURL,
		})
		if kerr != nil {
			return fmt.Errorf("init jwt auth: %w", kerr)
		}
		authMiddleware = middleware.JWTAuth(middleware.AuthConfig{
			Keyfunc:    keyfunc,
			Algorithms: algorithms,
			Issuer:     cfg.Auth.Issuer,
			Audience:   cfg.Auth.Audience,
		}, log)
		log.Info("jwt authentication enabled for /topics routes", "algorithms", algorithms)
	}

	router := api.NewRouter(api.RouterConfig{
		Handlers: api.Handlers{
			Health:  api.NewHealthHandler(healthSvc, log),
			Publish: api.NewPublishHandler(publisherSvc, log, maxPublishBodyBytes, cfg.Kafka.MaxBatchSize),
			Consume: api.NewConsumeHandler(consumerSvc, log),
			Topics:  api.NewTopicsHandler(topicSvc, log),
		},
		Logger:            log,
		MetricsMiddleware: appMetrics.Middleware,
		MetricsHandler:    promhttp.HandlerFor(registry, promhttp.HandlerOpts{}),
		AuthMiddleware:    authMiddleware,
		OpenAPISpec:       docs.OpenAPISpec,
		DocsUI:            swgui.NewHandler("Kafka REST API", "/openapi.yaml", "/docs"),
	})

	srv := &http.Server{
		Addr:         cfg.Server.Addr(),
		Handler:      router.Handler(),
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received, draining connections")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}

	log.Info("server stopped cleanly")
	return nil
}

// newProducer builds the producer for the configured mode and returns it along
// with a close function. "batched" uses the high-throughput Writer; "sync" uses
// the per-record-offset SyncProducer (see ADR 0001).
func newProducer(cfg config.KafkaConfig, security kafka.SecurityConfig) (service.Producer, func() error, error) {
	if strings.EqualFold(cfg.ProduceMode, "sync") {
		p, err := kafka.NewSyncProducer(kafka.SyncProducerConfig{
			Brokers:      cfg.Brokers,
			WriteTimeout: cfg.WriteTimeout,
			RequiredAcks: cfg.RequiredAcks,
			Security:     security,
		})
		if err != nil {
			return nil, nil, err
		}
		return p, p.Close, nil
	}

	w, err := kafka.NewWriter(kafka.WriterConfig{
		Brokers:         cfg.Brokers,
		WriteTimeout:    cfg.WriteTimeout,
		BatchTimeout:    cfg.BatchTimeout,
		RequiredAcks:    cfg.RequiredAcks,
		AllowAutoCreate: cfg.AllowAutoCreate,
		Security:        security,
	})
	if err != nil {
		return nil, nil, err
	}
	return w, w.Close, nil
}
