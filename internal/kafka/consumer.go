package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

const maxRetries = 5

type InvalidationEvent struct {
	InvestorID  string   `json:"investor_id"`
	EventType   string   `json:"event_type"`
	TimestampMs int64    `json:"timestamp"`
	Version     int64    `json:"version"`
	InvestorIDs []string `json:"investor_ids,omitempty"`
}

func (e *InvalidationEvent) Timestamp() time.Time {
	return time.UnixMilli(e.TimestampMs)
}

type InvalidationConsumer struct {
	reader    *kafka.Reader
	cache     domain.CacheStore
	metrics   *metrics.Collector
	dlqWriter *kafka.Writer
}

func NewInvalidationConsumer(cfg *config.KafkaConfig, cache domain.CacheStore, m *metrics.Collector) *InvalidationConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        500 * time.Millisecond,
	})

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.DLQTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}

	return &InvalidationConsumer{
		reader:    reader,
		cache:     cache,
		metrics:   m,
		dlqWriter: dlqWriter,
	}
}

func (c *InvalidationConsumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return c.close()
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			c.metrics.RecordKafkaError()
			continue
		}

		var event InvalidationEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("invalid event payload: %v", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}

		if err := c.processEvent(ctx, &event); err != nil {
			c.handleFailure(ctx, msg, &event, err)
			continue
		}

		_ = c.reader.CommitMessages(ctx, msg)
		c.metrics.RecordInvalidationSuccess(time.Since(event.Timestamp()))
	}
}

func (c *InvalidationConsumer) processEvent(ctx context.Context, event *InvalidationEvent) error {
	switch event.EventType {
	case "update", "delete":
		key := fmt.Sprintf("investor:%s", event.InvestorID)
		return c.cache.Delete(ctx, key)
	case "bulk_update":
		return c.processBulkInvalidation(ctx, event.InvestorIDs)
	default:
		log.Printf("unknown event type: %s", event.EventType)
		return nil
	}
}

func (c *InvalidationConsumer) processBulkInvalidation(ctx context.Context, ids []string) error {
	const batchSize = 1000
	for i := 0; i < len(ids); i += batchSize {
		end := i + batchSize
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[i:end]

		keys := make([]string, len(batch))
		for j, id := range batch {
			keys[j] = fmt.Sprintf("investor:%s", id)
		}
		if err := c.cache.DeleteBatch(ctx, keys); err != nil {
			return fmt.Errorf("bulk invalidation failed at batch %d: %w", i/batchSize, err)
		}
	}
	return nil
}

func (c *InvalidationConsumer) handleFailure(ctx context.Context,
	msg kafka.Message, event *InvalidationEvent, originalErr error) {

	backoff := 100 * time.Millisecond
	for attempt := 1; attempt <= maxRetries; attempt++ {
		time.Sleep(backoff)
		if err := c.processEvent(ctx, event); err == nil {
			_ = c.reader.CommitMessages(ctx, msg)
			c.metrics.RecordInvalidationRetrySuccess(attempt)
			return
		}
		backoff *= 2
	}

	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: []kafka.Header{
			{Key: "original-error", Value: []byte(originalErr.Error())},
			{Key: "retry-count", Value: []byte(fmt.Sprintf("%d", maxRetries))},
		},
	}
	if err := c.dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
		log.Printf("CRITICAL: DLQ write failed for %s: %v; offset NOT committed",
			event.InvestorID, err)
		c.metrics.RecordDLQWriteFailure()
		return
	}
	_ = c.reader.CommitMessages(ctx, msg)
	c.metrics.RecordInvalidationDLQ()
}

func (c *InvalidationConsumer) close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("failed to close kafka reader: %w", err)
	}
	if err := c.dlqWriter.Close(); err != nil {
		return fmt.Errorf("failed to close dlq writer: %w", err)
	}
	return nil
}
