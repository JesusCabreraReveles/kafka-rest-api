package api

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/JesusCabreraReveles/kafka-rest-api/internal/middleware"
)

// Handlers groups the route handlers the router serves.
type Handlers struct {
	Health  *HealthHandler
	Publish *PublishHandler
	Consume *ConsumeHandler
	Topics  *TopicsHandler
}

// RouterConfig holds everything needed to build the HTTP handler. Grouping the
// dependencies keeps the constructor signature stable as handlers are added.
type RouterConfig struct {
	Handlers          Handlers
	Logger            *slog.Logger
	MetricsMiddleware func(http.Handler) http.Handler // optional
	MetricsHandler    http.Handler                    // optional, served at /metrics
	AuthMiddleware    func(http.Handler) http.Handler // optional, guards /topics routes
	OpenAPISpec       []byte                          // optional, served at /openapi.yaml
	DocsUI            http.Handler                    // optional, Swagger UI mounted at /docs
}

// Router builds the application's HTTP handler from its route handlers and the
// shared middleware stack.
type Router struct {
	cfg RouterConfig
}

// NewRouter constructs a Router from its configuration.
func NewRouter(cfg RouterConfig) *Router {
	return &Router{cfg: cfg}
}

// Handler assembles the middleware chain and routes, returning an http.Handler
// ready to be served.
func (rt *Router) Handler() http.Handler {
	h := rt.cfg.Handlers
	r := chi.NewRouter()

	r.Use(chimw.RequestID)
	r.Use(chimw.RealIP)
	r.Use(middleware.RequestLogger(rt.cfg.Logger))
	if rt.cfg.MetricsMiddleware != nil {
		r.Use(rt.cfg.MetricsMiddleware)
	}
	r.Use(chimw.Recoverer)

	r.Get("/health", h.Health.Live)
	r.Get("/ready", h.Health.Ready)
	if rt.cfg.MetricsHandler != nil {
		r.Method(http.MethodGet, "/metrics", rt.cfg.MetricsHandler)
	}

	// API documentation (always public, even when auth is enabled).
	if rt.cfg.OpenAPISpec != nil {
		spec := rt.cfg.OpenAPISpec
		r.Get("/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/yaml")
			_, _ = w.Write(spec)
		})
	}
	if rt.cfg.DocsUI != nil {
		r.Mount("/docs", rt.cfg.DocsUI)
	}

	r.Route("/topics", func(r chi.Router) {
		if rt.cfg.AuthMiddleware != nil {
			r.Use(rt.cfg.AuthMiddleware)
		}
		r.Get("/", h.Topics.List)
		r.Route("/{topic}", func(r chi.Router) {
			r.Get("/", h.Topics.Describe)
			r.Get("/consume", h.Consume.Consume)
			r.Post("/publish", h.Publish.PublishOne)
			r.Post("/publish/batch", h.Publish.PublishBatch)
		})
	})

	return r
}
