package circuitbreaker

import (
	"errors"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
)

func NewCircuitBreaker(m *metrics.Collector) *gobreaker.CircuitBreaker[*domain.InvestorProfile] {
	return gobreaker.NewCircuitBreaker[*domain.InvestorProfile](gobreaker.Settings{
		Name:        "redis-circuit-breaker",
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     30 * time.Second,
		IsSuccessful: func(err error) bool {
			return err == nil || errors.Is(err, redis.Nil)
		},
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < 10 {
				return false
			}
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRatio >= 0.5
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Printf("circuit breaker %s: %s -> %s", name, from, to)
			switch to {
			case gobreaker.StateClosed:
				m.SetCircuitBreakerState(0)
			case gobreaker.StateOpen:
				m.SetCircuitBreakerState(1)
			case gobreaker.StateHalfOpen:
				m.SetCircuitBreakerState(2)
			}
		},
	})
}
