package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Collector struct {
	cacheRequests       *prometheus.CounterVec
	cacheLatency        prometheus.Histogram
	middlewareRequest   *prometheus.HistogramVec
	invalidationEvents  *prometheus.CounterVec
	invalidationLag     prometheus.Histogram
	circuitBreakerState prometheus.Gauge
	dbFallback          prometheus.Counter
	singleflightDedup   prometheus.Counter
	dlqWriteFailures    prometheus.Counter
}

func NewCollector() *Collector {
	return &Collector{
		cacheRequests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_requests_total",
			Help: "Total number of cache requests by status",
		}, []string{"status"}),

		cacheLatency: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "cache_latency_seconds",
			Help:    "Latency of cache operations in seconds",
			Buckets: []float64{0.00025, 0.0005, 0.00075, 0.001, 0.00125, 0.0015, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1},
		}),

		middlewareRequest: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "middleware_request_duration_seconds",
			Help:    "Latency of middleware HTTP handler from request entry to response completion in seconds",
			Buckets: []float64{0.00025, 0.0005, 0.00075, 0.001, 0.00125, 0.0015, 0.002, 0.003, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.1},
		}, []string{"route", "cache_result", "status"}),

		invalidationEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "invalidation_events_total",
			Help: "Total invalidation events processed by result",
		}, []string{"result"}),

		invalidationLag: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "invalidation_lag_seconds",
			Help:    "Lag between event timestamp and cache invalidation",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		}),

		circuitBreakerState: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Current circuit breaker state (0=closed, 1=open, 2=half-open)",
		}),

		dbFallback: promauto.NewCounter(prometheus.CounterOpts{
			Name: "db_fallback_total",
			Help: "Total direct DB requests when cache is bypassed",
		}),

		singleflightDedup: promauto.NewCounter(prometheus.CounterOpts{
			Name: "singleflight_dedup_total",
			Help: "Total deduplicated concurrent requests via singleflight",
		}),

		dlqWriteFailures: promauto.NewCounter(prometheus.CounterOpts{
			Name: "dlq_write_failures_total",
			Help: "Total failed writes to dead letter queue",
		}),
	}
}

func (c *Collector) RecordCacheHit() {
	c.cacheRequests.WithLabelValues("hit").Inc()
}

func (c *Collector) RecordCacheMiss() {
	c.cacheRequests.WithLabelValues("miss").Inc()
}

func (c *Collector) RecordCacheBypass() {
	c.cacheRequests.WithLabelValues("bypass").Inc()
}

func (c *Collector) RecordCacheError() {
	c.cacheRequests.WithLabelValues("error").Inc()
}

func (c *Collector) RecordCacheLatency(d time.Duration) {
	c.cacheLatency.Observe(d.Seconds())
}

func (c *Collector) RecordMiddlewareRequest(route, cacheResult, status string, d time.Duration) {
	c.middlewareRequest.WithLabelValues(route, cacheResult, status).Observe(d.Seconds())
}

func (c *Collector) RecordInvalidationSuccess(lag time.Duration) {
	c.invalidationEvents.WithLabelValues("success").Inc()
	c.invalidationLag.Observe(lag.Seconds())
}

func (c *Collector) RecordInvalidationFailure() {
	c.invalidationEvents.WithLabelValues("failure").Inc()
}

func (c *Collector) RecordInvalidationDLQ() {
	c.invalidationEvents.WithLabelValues("dlq").Inc()
}

func (c *Collector) RecordInvalidationRetrySuccess(attempt int) {
	c.invalidationEvents.WithLabelValues("success").Inc()
}

func (c *Collector) RecordKafkaError() {
	c.invalidationEvents.WithLabelValues("kafka_error").Inc()
}

func (c *Collector) SetCircuitBreakerState(state float64) {
	c.circuitBreakerState.Set(state)
}

func (c *Collector) RecordDBFallback() {
	c.dbFallback.Inc()
}

func (c *Collector) RecordSingleflightDedup() {
	c.singleflightDedup.Inc()
}

func (c *Collector) RecordDLQWriteFailure() {
	c.dlqWriteFailures.Inc()
}

func (c *Collector) RecordReconciliation(total, stale int) {
}
