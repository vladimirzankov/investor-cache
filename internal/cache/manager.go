package cache

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker/v2"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
	"golang.org/x/sync/singleflight"
)

type CacheManager struct {
	cache      domain.CacheStore
	repo       domain.ProfileRepository
	sfGroup    singleflight.Group
	cb         *gobreaker.CircuitBreaker[*domain.InvestorProfile]
	metrics    *metrics.Collector
	ttl        time.Duration
	cbDisabled bool
}

func NewCacheManager(
	cache domain.CacheStore,
	repo domain.ProfileRepository,
	cb *gobreaker.CircuitBreaker[*domain.InvestorProfile],
	m *metrics.Collector,
	ttl time.Duration,
) *CacheManager {
	cbDisabled := os.Getenv("CACHE_DISABLE_CB") == "true"
	if cbDisabled {
		log.Println("WARNING: Circuit Breaker is DISABLED on the hot path (CACHE_DISABLE_CB=true)")
	}
	return &CacheManager{
		cache:      cache,
		repo:       repo,
		cb:         cb,
		metrics:    m,
		ttl:        ttl,
		cbDisabled: cbDisabled,
	}
}

func (m *CacheManager) GetProfile(ctx context.Context, id string) (*domain.InvestorProfile, CacheResult, error) {
	key := fmt.Sprintf("investor:%s", id)

	var profile *domain.InvestorProfile
	var err error

	if m.cbDisabled {
		start := time.Now()
		profile, err = m.cache.Get(ctx, key)
		m.metrics.RecordCacheLatency(time.Since(start))
	} else {
		profile, err = m.cb.Execute(func() (*domain.InvestorProfile, error) {
			start := time.Now()
			p, err := m.cache.Get(ctx, key)
			m.metrics.RecordCacheLatency(time.Since(start))
			return p, err
		})
	}

	if err == nil && profile != nil {
		m.metrics.RecordCacheHit()
		return profile, CacheResultHit, nil
	}

	if err != nil && !errors.Is(err, redis.Nil) {
		result := CacheResultError
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			result = CacheResultCBOpen
		} else {
			m.metrics.RecordCacheError()
		}
		m.metrics.RecordDBFallback()
		log.Printf("cache error for key %s (result=%s), falling back to DB: %v", key, result, err)
		dbProfile, dbErr := m.repo.GetByID(ctx, id)
		if dbErr != nil {
			return nil, result, dbErr
		}
		return dbProfile, result, nil
	}

	m.metrics.RecordCacheMiss()

	v, err, shared := m.sfGroup.Do(key, func() (interface{}, error) {
		return m.loadAndCache(ctx, id, key)
	})
	if err != nil {
		return nil, CacheResultMiss, err
	}
	if shared {
		m.metrics.RecordSingleflightDedup()
	}
	return v.(*domain.InvestorProfile), CacheResultMiss, nil
}

func (m *CacheManager) loadAndCache(ctx context.Context, id, key string) (*domain.InvestorProfile, error) {
	profile, err := m.repo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	if _, err := m.cache.SetVersioned(ctx, key, profile, m.ttl); err != nil {
		log.Printf("failed to cache profile %s: %v", id, err)
	}

	return profile, nil
}
