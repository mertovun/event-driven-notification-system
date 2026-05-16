// Package observability provides slog handlers, Prometheus registry, OTel SDK init,
// and the /livez /readyz health endpoints.
package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the registered handle set, owned at the process level.
// Constructed once in main; passed by pointer to anything that records.
type Metrics struct {
	Registry *prometheus.Registry

	// HTTP server side
	HTTPRequestsTotal       *prometheus.CounterVec
	HTTPRequestDurationSecs *prometheus.HistogramVec
	HTTPRequestsInFlight    prometheus.Gauge

	// Domain — accepted from the API
	NotificationsCreatedTotal *prometheus.CounterVec

	// Worker — outcomes
	NotificationsDeliveredTotal *prometheus.CounterVec
	DeliveryDurationSecs        *prometheus.HistogramVec
	ProviderDurationSecs        *prometheus.HistogramVec
	NotificationRetryTotal      *prometheus.CounterVec
	WorkerActive                *prometheus.GaugeVec
	RateLimitThrottledTotal     *prometheus.CounterVec
	CircuitBreakerState         *prometheus.GaugeVec

	// Queue depth — sampled periodically (Phase 5.4 optional)
	QueueDepth *prometheus.GaugeVec

	// Idempotency replay (rare but high signal)
	IdempotencyReplayTotal *prometheus.CounterVec

	// Build info — set once at startup
	BuildInfo *prometheus.GaugeVec
}

// NewMetrics constructs and registers the full metric set on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	// Standard Go / process collectors so /metrics is useful out of the box.
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{Registry: reg}

	m.HTTPRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "HTTP requests served, labeled by method, route, status.",
	}, []string{"method", "route", "status"})

	m.HTTPRequestDurationSecs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP server-side request latency.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"method", "route"})

	m.HTTPRequestsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "http_requests_in_flight",
		Help: "Currently-executing HTTP handlers.",
	})

	m.NotificationsCreatedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_created_total",
		Help: "Notifications accepted via the API.",
	}, []string{"channel", "priority"})

	m.NotificationsDeliveredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_delivered_total",
		Help: "Worker delivery outcomes. status in {success, failure, dead_letter}.",
	}, []string{"channel", "status"})

	// Buckets justified for ~50ms..10s SLO range.
	m.DeliveryDurationSecs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_delivery_duration_seconds",
		Help:    "End-to-end worker delivery latency.",
		Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"channel"})

	m.ProviderDurationSecs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "notification_provider_duration_seconds",
		Help:    "Outbound HTTP-only latency to the external provider.",
		Buckets: []float64{0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"channel"})

	m.NotificationRetryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notification_retry_total",
		Help: "Worker retries by attempt number (label capped at 5+).",
	}, []string{"channel", "attempt"})

	m.WorkerActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_active",
		Help: "In-flight worker handlers per channel.",
	}, []string{"channel"})

	m.RateLimitThrottledTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "rate_limit_throttled_total",
		Help: "Token-bucket denials per channel.",
	}, []string{"channel"})

	m.CircuitBreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "Circuit breaker state: 0=closed, 1=open, 2=half-open.",
	}, []string{"channel"})

	m.QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "queue_depth",
		Help: "Sampled RabbitMQ queue message count.",
	}, []string{"queue", "priority"})

	m.IdempotencyReplayTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "idempotency_replay_total",
		Help: "Idempotency-Key replays returned verbatim.",
	}, []string{"endpoint"})

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "build_info",
		Help: "Build metadata, set to 1 at startup.",
	}, []string{"version", "commit", "go_version"})

	for _, c := range []prometheus.Collector{
		m.HTTPRequestsTotal,
		m.HTTPRequestDurationSecs,
		m.HTTPRequestsInFlight,
		m.NotificationsCreatedTotal,
		m.NotificationsDeliveredTotal,
		m.DeliveryDurationSecs,
		m.ProviderDurationSecs,
		m.NotificationRetryTotal,
		m.WorkerActive,
		m.RateLimitThrottledTotal,
		m.CircuitBreakerState,
		m.QueueDepth,
		m.IdempotencyReplayTotal,
		m.BuildInfo,
	} {
		reg.MustRegister(c)
	}

	return m
}

// SetBuildInfo records the build metadata once at startup.
func (m *Metrics) SetBuildInfo(version, commit, goVersion string) {
	m.BuildInfo.WithLabelValues(version, commit, goVersion).Set(1)
}

// Handler returns an http.Handler suitable for mounting at /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}
