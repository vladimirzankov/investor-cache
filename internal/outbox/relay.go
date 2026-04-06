package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type publisher interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type relayMetrics interface {
	RecordPublished(lag time.Duration)
	RecordPublishError()
	SetUnpublishedCount(n int64)
}

type Relay struct {
	repo         *Repository
	writer       publisher
	metrics      relayMetrics
	pollInterval time.Duration
	batchSize    int
}

func NewRelay(repo *Repository, writer publisher, m relayMetrics, pollInterval time.Duration, batchSize int) *Relay {
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Relay{
		repo:         repo,
		writer:       writer,
		metrics:      m,
		pollInterval: pollInterval,
		batchSize:    batchSize,
	}
}

func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.runCycle(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Printf("outbox relay cycle error: %v", err)
			}
		}
	}
}

func (r *Relay) runCycle(ctx context.Context) error {
	tx, err := r.repo.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
				log.Printf("outbox relay rollback: %v", rbErr)
			}
		}
	}()

	events, err := r.repo.FetchUnpublished(ctx, tx, r.batchSize)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit empty cycle: %w", err)
		}
		committed = true
		r.refreshQueueDepth(ctx)
		return nil
	}

	successIDs := make([]int64, 0, len(events))
	for _, e := range events {
		msg := kafka.Message{
			Key:   []byte(e.AggregateID),
			Value: e.Payload,
			Time:  e.CreatedAt,
		}
		if err := r.writer.WriteMessages(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			log.Printf("outbox relay publish failed for id=%d: %v", e.ID, err)
			if recErr := r.repo.RecordError(ctx, tx, e.ID, err.Error()); recErr != nil {
				log.Printf("outbox relay record error failed for id=%d: %v", e.ID, recErr)
			}
			if r.metrics != nil {
				r.metrics.RecordPublishError()
			}
			break
		}
		successIDs = append(successIDs, e.ID)
		if r.metrics != nil {
			r.metrics.RecordPublished(time.Since(e.CreatedAt))
		}
	}

	if err := r.repo.MarkPublished(ctx, tx, successIDs); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publish cycle: %w", err)
	}
	committed = true

	r.refreshQueueDepth(ctx)
	return nil
}

func (r *Relay) refreshQueueDepth(ctx context.Context) {
	if r.metrics == nil {
		return
	}
	n, err := r.repo.CountUnpublished(ctx)
	if err != nil {
		log.Printf("outbox relay count unpublished: %v", err)
		return
	}
	r.metrics.SetUnpublishedCount(n)
}
