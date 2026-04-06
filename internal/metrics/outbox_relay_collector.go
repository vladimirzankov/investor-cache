package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type OutboxRelayCollector struct {
	publishedTotal     prometheus.Counter
	publishErrorsTotal prometheus.Counter
	lagSeconds         prometheus.Histogram
	unpublishedCount   prometheus.Gauge
}

func NewOutboxRelayCollector() *OutboxRelayCollector {
	return &OutboxRelayCollector{
		publishedTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "outbox_relay_published_total",
			Help: "Total outbox events successfully published to Kafka",
		}),

		publishErrorsTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "outbox_relay_publish_errors_total",
			Help: "Total Kafka publish errors encountered by the relay",
		}),

		lagSeconds: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "outbox_relay_lag_seconds",
			Help:    "Time between outbox.created_at and successful Kafka publish",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
		}),

		unpublishedCount: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_relay_unpublished_count",
			Help: "Current number of rows in the outbox table with published = false",
		}),
	}
}

func (c *OutboxRelayCollector) RecordPublished(lag time.Duration) {
	c.publishedTotal.Inc()
	c.lagSeconds.Observe(lag.Seconds())
}

func (c *OutboxRelayCollector) RecordPublishError() {
	c.publishErrorsTotal.Inc()
}

func (c *OutboxRelayCollector) SetUnpublishedCount(n int64) {
	c.unpublishedCount.Set(float64(n))
}
