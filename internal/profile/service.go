package profile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/internal/outbox"
)

type writeRepo interface {
	Update(ctx context.Context, tx *sql.Tx, id string, patch UpdateProfilePatch) (*domain.InvestorProfile, error)
}

type outboxRepo interface {
	AddEvent(ctx context.Context, tx *sql.Tx, e outbox.Event) error
}

type metricsSink interface {
	RecordOutboxEvent(eventType string)
}

type Service struct {
	db          *sql.DB
	profileRepo writeRepo
	outboxRepo  outboxRepo
	metrics     metricsSink
}

func NewService(db *sql.DB, profileRepo writeRepo, outboxRepo outboxRepo, m metricsSink) *Service {
	return &Service{
		db:          db,
		profileRepo: profileRepo,
		outboxRepo:  outboxRepo,
		metrics:     m,
	}
}

func (s *Service) UpdateProfile(
	ctx context.Context,
	id string,
	patch UpdateProfilePatch,
) (*domain.InvestorProfile, error) {
	if err := ValidatePatch(patch); err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			log.Printf("rollback after failed UpdateProfile for %s: %v", id, rbErr)
		}
	}()

	profile, err := s.profileRepo.Update(ctx, tx, id, patch)
	if err != nil {
		return nil, err
	}

	event, err := outbox.NewProfileUpdatedEvent(profile, patch.ChangedFields())
	if err != nil {
		return nil, fmt.Errorf("build profile_updated event: %w", err)
	}

	if err := s.outboxRepo.AddEvent(ctx, tx, event); err != nil {
		return nil, fmt.Errorf("insert outbox event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	if s.metrics != nil {
		s.metrics.RecordOutboxEvent(event.EventType)
	}

	return profile, nil
}
