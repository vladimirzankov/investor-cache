package reconciler

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
)

type Reconciler struct {
	redisClient *redis.ClusterClient
	cache       domain.CacheStore
	repo        domain.ProfileRepository
	metrics     *metrics.Collector
	interval    time.Duration
	sampleSize  int
}

func NewReconciler(
	redisClient *redis.ClusterClient,
	cache domain.CacheStore,
	repo domain.ProfileRepository,
	m *metrics.Collector,
	interval time.Duration,
	sampleSize int,
) *Reconciler {
	return &Reconciler{
		redisClient: redisClient,
		cache:       cache,
		repo:        repo,
		metrics:     m,
		interval:    interval,
		sampleSize:  sampleSize,
	}
}

func (r *Reconciler) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reconcile(ctx)
		}
	}
}

func (r *Reconciler) reconcile(ctx context.Context) {
	keys, err := r.sampleKeys(ctx, r.sampleSize)
	if err != nil {
		log.Printf("reconciliation: sample keys error: %v", err)
		return
	}

	stale := 0
	for _, key := range keys {
		id := strings.TrimPrefix(key, "investor:")
		cached, err := r.cache.Get(ctx, key)
		if err != nil {
			continue
		}
		current, err := r.repo.GetByID(ctx, id)
		if err != nil {
			continue
		}
		if cached.CacheVersion < current.CacheVersion {
			_ = r.cache.Delete(ctx, key)
			stale++
		}
	}

	r.metrics.RecordReconciliation(len(keys), stale)
	if stale > 0 {
		log.Printf("reconciliation: checked %d keys, evicted %d stale entries", len(keys), stale)
	}
}

func (r *Reconciler) sampleKeys(ctx context.Context, count int) ([]string, error) {
	var allKeys []string

	err := r.redisClient.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		var cursor uint64
		for len(allKeys) < count {
			keys, nextCursor, err := client.Scan(ctx, cursor, "investor:*", int64(count)).Result()
			if err != nil {
				return err
			}
			allKeys = append(allKeys, keys...)
			cursor = nextCursor
			if cursor == 0 {
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(allKeys) > count {
		allKeys = allKeys[:count]
	}
	return allKeys, nil
}
