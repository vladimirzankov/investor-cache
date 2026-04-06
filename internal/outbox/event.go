package outbox

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/vladimirzankov/investor-cache/internal/domain"
)

const (
	AggregateTypeInvestor   = "investor"
	EventTypeProfileUpdated = "profile_updated"
)

type Event struct {
	ID            int64
	AggregateID   string
	AggregateType string
	EventType     string
	Payload       []byte
	CreatedAt     time.Time
	RetryCount    int
}

type KafkaPayload struct {
	InvestorID    string   `json:"investor_id"`
	EventType     string   `json:"event_type"`
	TimestampMs   int64    `json:"timestamp"`
	Version       int64    `json:"version"`
	ChangedFields []string `json:"changed_fields,omitempty"`
}

func NewProfileUpdatedEvent(profile *domain.InvestorProfile, changedFields []string) (Event, error) {
	if profile == nil {
		return Event{}, fmt.Errorf("nil profile")
	}
	payload := KafkaPayload{
		InvestorID:    profile.InvestorID,
		EventType:     "update",
		TimestampMs:   time.Now().UnixMilli(),
		Version:       profile.CacheVersion,
		ChangedFields: changedFields,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal kafka payload: %w", err)
	}
	return Event{
		AggregateID:   profile.InvestorID,
		AggregateType: AggregateTypeInvestor,
		EventType:     EventTypeProfileUpdated,
		Payload:       raw,
	}, nil
}
