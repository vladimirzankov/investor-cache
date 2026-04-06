package domain

import (
	"context"
	"time"
)

type InvestorProfile struct {
	InvestorID        string `json:"investor_id" db:"investor_id"`
	FullName          string `json:"full_name" db:"full_name"`
	Email             string `json:"email" db:"email"`
	RiskProfile       string `json:"risk_profile" db:"risk_profile"`
	KYCStatus         string `json:"kyc_status" db:"kyc_status"`
	PortfolioValue    string `json:"portfolio_value" db:"portfolio_value"`
	Preferences       string `json:"preferences" db:"preferences"`
	QualifiedInvestor bool   `json:"qualified_investor" db:"qualified_investor"`
	InvestmentHorizon string `json:"investment_horizon" db:"investment_horizon"`
	UpdatedAt         string `json:"updated_at" db:"updated_at"`
	CacheVersion      int64  `json:"cache_version" db:"cache_version"`
}

type ProfileRepository interface {
	GetByID(ctx context.Context, id string) (*InvestorProfile, error)
}

type CacheStore interface {
	Get(ctx context.Context, key string) (*InvestorProfile, error)
	SetVersioned(ctx context.Context, key string, profile *InvestorProfile, ttl time.Duration) (bool, error)
	Delete(ctx context.Context, key string) error
	DeleteBatch(ctx context.Context, keys []string) error
}
