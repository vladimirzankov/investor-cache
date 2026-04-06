package repository

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(cfg *config.DBConfig) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &PostgresRepository{db: db}, nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, id string) (*domain.InvestorProfile, error) {
	const query = `
		SELECT investor_id, full_name, email, risk_profile, kyc_status,
		       portfolio_value::text, preferences::text,
		       qualified_investor, investment_horizon,
		       updated_at::text, cache_version
		FROM investors
		WHERE investor_id = $1`

	profile := &domain.InvestorProfile{}
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&profile.InvestorID,
		&profile.FullName,
		&profile.Email,
		&profile.RiskProfile,
		&profile.KYCStatus,
		&profile.PortfolioValue,
		&profile.Preferences,
		&profile.QualifiedInvestor,
		&profile.InvestmentHorizon,
		&profile.UpdatedAt,
		&profile.CacheVersion,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("investor %s not found", id)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query investor %s: %w", id, err)
	}
	return profile, nil
}

func (r *PostgresRepository) Close() error {
	return r.db.Close()
}
