package profile

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/lib/pq"
	"github.com/vladimirzankov/investor-cache/internal/domain"
)

var (
	ErrNotFound    = errors.New("investor not found")
	ErrEmailExists = errors.New("email already exists")
)

type WriteRepository struct{}

func NewWriteRepository() *WriteRepository {
	return &WriteRepository{}
}

func (r *WriteRepository) Update(
	ctx context.Context,
	tx *sql.Tx,
	id string,
	patch UpdateProfilePatch,
) (*domain.InvestorProfile, error) {
	if patch.IsEmpty() {
		return nil, &ValidationError{Message: "patch is empty"}
	}

	setClauses := make([]string, 0, 6)
	args := make([]any, 0, 7)
	next := 1

	if patch.FullName != nil {
		setClauses = append(setClauses, "full_name = $"+strconv.Itoa(next))
		args = append(args, *patch.FullName)
		next++
	}
	if patch.Email != nil {
		setClauses = append(setClauses, "email = $"+strconv.Itoa(next))
		args = append(args, *patch.Email)
		next++
	}
	if patch.RiskProfile != nil {
		setClauses = append(setClauses, "risk_profile = $"+strconv.Itoa(next))
		args = append(args, *patch.RiskProfile)
		next++
	}
	if patch.KYCStatus != nil {
		setClauses = append(setClauses, "kyc_status = $"+strconv.Itoa(next))
		args = append(args, *patch.KYCStatus)
		next++
	}
	if patch.PortfolioValue != nil {
		setClauses = append(setClauses, "portfolio_value = $"+strconv.Itoa(next)+"::numeric")
		args = append(args, *patch.PortfolioValue)
		next++
	}
	if patch.Preferences != nil {
		setClauses = append(setClauses, "preferences = $"+strconv.Itoa(next)+"::jsonb")
		args = append(args, string(*patch.Preferences))
		next++
	}

	args = append(args, id)
	query := fmt.Sprintf(`
		UPDATE investors
		SET %s
		WHERE investor_id = $%d
		RETURNING investor_id, full_name, email, risk_profile, kyc_status,
		          portfolio_value::text, preferences::text, updated_at::text, cache_version
	`, strings.Join(setClauses, ", "), next)

	profile := &domain.InvestorProfile{}
	err := tx.QueryRowContext(ctx, query, args...).Scan(
		&profile.InvestorID,
		&profile.FullName,
		&profile.Email,
		&profile.RiskProfile,
		&profile.KYCStatus,
		&profile.PortfolioValue,
		&profile.Preferences,
		&profile.UpdatedAt,
		&profile.CacheVersion,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return nil, ErrEmailExists
		}
		return nil, fmt.Errorf("update investor %s: %w", id, err)
	}
	return profile, nil
}
