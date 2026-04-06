package profile

import (
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
)

var (
	emailRegex = regexp.MustCompile(`^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$`)

	allowedRiskProfiles = map[string]struct{}{
		"conservative": {},
		"moderate":     {},
		"aggressive":   {},
	}
	allowedKYCStatuses = map[string]struct{}{
		"pending":  {},
		"verified": {},
		"rejected": {},
	}
)

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return e.Field + ": " + e.Message
}

type UpdateProfilePatch struct {
	FullName       *string          `json:"full_name,omitempty"`
	Email          *string          `json:"email,omitempty"`
	RiskProfile    *string          `json:"risk_profile,omitempty"`
	KYCStatus      *string          `json:"kyc_status,omitempty"`
	PortfolioValue *string          `json:"portfolio_value,omitempty"`
	Preferences    *json.RawMessage `json:"preferences,omitempty"`
}

func (p UpdateProfilePatch) IsEmpty() bool {
	return p.FullName == nil && p.Email == nil && p.RiskProfile == nil &&
		p.KYCStatus == nil && p.PortfolioValue == nil && p.Preferences == nil
}

func (p UpdateProfilePatch) ChangedFields() []string {
	fields := make([]string, 0, 6)
	if p.FullName != nil {
		fields = append(fields, "full_name")
	}
	if p.Email != nil {
		fields = append(fields, "email")
	}
	if p.RiskProfile != nil {
		fields = append(fields, "risk_profile")
	}
	if p.KYCStatus != nil {
		fields = append(fields, "kyc_status")
	}
	if p.PortfolioValue != nil {
		fields = append(fields, "portfolio_value")
	}
	if p.Preferences != nil {
		fields = append(fields, "preferences")
	}
	return fields
}

func ValidatePatch(p UpdateProfilePatch) error {
	if p.IsEmpty() {
		return &ValidationError{Message: "request body must contain at least one field"}
	}

	if p.FullName != nil {
		name := strings.TrimSpace(*p.FullName)
		if name == "" {
			return &ValidationError{Field: "full_name", Message: "must not be empty"}
		}
		if len(name) > 255 {
			return &ValidationError{Field: "full_name", Message: "must be at most 255 characters"}
		}
	}

	if p.Email != nil {
		email := strings.TrimSpace(*p.Email)
		if email == "" {
			return &ValidationError{Field: "email", Message: "must not be empty"}
		}
		if len(email) > 255 {
			return &ValidationError{Field: "email", Message: "must be at most 255 characters"}
		}
		if !emailRegex.MatchString(email) {
			return &ValidationError{Field: "email", Message: "must be a valid email address"}
		}
	}

	if p.RiskProfile != nil {
		if _, ok := allowedRiskProfiles[*p.RiskProfile]; !ok {
			return &ValidationError{
				Field:   "risk_profile",
				Message: "must be one of: conservative, moderate, aggressive",
			}
		}
	}

	if p.KYCStatus != nil {
		if _, ok := allowedKYCStatuses[*p.KYCStatus]; !ok {
			return &ValidationError{
				Field:   "kyc_status",
				Message: "must be one of: pending, verified, rejected",
			}
		}
	}

	if p.PortfolioValue != nil {
		v, err := strconv.ParseFloat(*p.PortfolioValue, 64)
		if err != nil {
			return &ValidationError{
				Field:   "portfolio_value",
				Message: "must be a decimal number",
			}
		}
		if v < 0 {
			return &ValidationError{
				Field:   "portfolio_value",
				Message: "must be non-negative",
			}
		}
	}

	if p.Preferences != nil {
		if !json.Valid(*p.Preferences) {
			return &ValidationError{Field: "preferences", Message: "must be a valid JSON value"}
		}
	}

	return nil
}

func IsValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}
