package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type ProfileServiceCollector struct {
	requests        *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	outboxEvents    *prometheus.CounterVec
}

func NewProfileServiceCollector() *ProfileServiceCollector {
	return &ProfileServiceCollector{
		requests: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "profile_service_requests_total",
			Help: "Total HTTP requests handled by profile-service",
		}, []string{"method", "endpoint", "status"}),

		requestDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "profile_service_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		}, []string{"method", "endpoint"}),

		outboxEvents: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "profile_service_outbox_events_total",
			Help: "Total outbox events committed by profile-service",
		}, []string{"event_type"}),
	}
}

func (c *ProfileServiceCollector) RecordHTTPRequest(method, endpoint string, status int, duration time.Duration) {
	c.requests.WithLabelValues(method, endpoint, strconv.Itoa(status)).Inc()
	c.requestDuration.WithLabelValues(method, endpoint).Observe(duration.Seconds())
}

func (c *ProfileServiceCollector) RecordOutboxEvent(eventType string) {
	c.outboxEvents.WithLabelValues(eventType).Inc()
}
