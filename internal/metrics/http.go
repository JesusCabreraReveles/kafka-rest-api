package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
)

// Middleware records request count and latency for each HTTP request, labeled
// by method, matched route pattern, and status. It must be installed inside a
// chi router so the route pattern is available.
func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()

		defer func() {
			status := ww.Status()
			if status == 0 {
				status = http.StatusOK // handler wrote body without explicit code
			}

			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unmatched"
			}

			m.httpRequests.WithLabelValues(r.Method, route, strconv.Itoa(status)).Inc()
			m.httpDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
		}()

		next.ServeHTTP(ww, r)
	})
}
