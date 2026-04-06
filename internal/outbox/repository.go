package outbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) DB() *sql.DB {
	return r.db
}

func (r *Repository) AddEvent(ctx context.Context, tx *sql.Tx, e Event) error {
	const query = `
		INSERT INTO outbox (aggregate_id, aggregate_type, event_type, payload)
		VALUES ($1, $2, $3, $4::jsonb)
	`
	if _, err := tx.ExecContext(ctx, query,
		e.AggregateID, e.AggregateType, e.EventType, string(e.Payload),
	); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	return nil
}

func (r *Repository) FetchUnpublished(ctx context.Context, tx *sql.Tx, limit int) ([]Event, error) {
	const query = `
		SELECT id, aggregate_id, aggregate_type, event_type, payload, created_at, retry_count
		FROM outbox
		WHERE published = false
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`
	rows, err := tx.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("select unpublished: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		var e Event
		var payload []byte
		if err := rows.Scan(
			&e.ID,
			&e.AggregateID,
			&e.AggregateType,
			&e.EventType,
			&payload,
			&e.CreatedAt,
			&e.RetryCount,
		); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		e.Payload = payload
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return events, nil
}

func (r *Repository) MarkPublished(ctx context.Context, tx *sql.Tx, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	const query = `
		UPDATE outbox
		SET published = true, published_at = now()
		WHERE id = ANY($1)
	`
	if _, err := tx.ExecContext(ctx, query, pq.Array(ids)); err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

func (r *Repository) RecordError(ctx context.Context, tx *sql.Tx, id int64, errMsg string) error {
	const query = `
		UPDATE outbox
		SET retry_count = retry_count + 1, last_error = $2
		WHERE id = $1
	`
	if _, err := tx.ExecContext(ctx, query, id, errMsg); err != nil {
		return fmt.Errorf("record outbox error: %w", err)
	}
	return nil
}

func (r *Repository) CountUnpublished(ctx context.Context) (int64, error) {
	const query = `SELECT count(*) FROM outbox WHERE published = false`
	var n int64
	if err := r.db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return 0, fmt.Errorf("count unpublished: %w", err)
	}
	return n, nil
}
