package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vladimirzankov/investor-cache/internal/domain"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

const luaVersionedSet = `
local current = redis.call('HGET', KEYS[1], 'cache_version')
if current and tonumber(current) >= tonumber(ARGV[1]) then
    return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 3))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`

type RedisStore struct {
	client        *redis.ClusterClient
	versionScript *redis.Script
}

func NewRedisStore(cfg *config.RedisConfig) (*RedisStore, error) {
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        cfg.Addrs,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
		ReadOnly:     false,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	}); err != nil {
		return nil, fmt.Errorf("redis cluster health check failed: %w", err)
	}

	return &RedisStore{
		client:        client,
		versionScript: redis.NewScript(luaVersionedSet),
	}, nil
}

func (s *RedisStore) Get(ctx context.Context, key string) (*domain.InvestorProfile, error) {
	result, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis HGETALL failed: %w", err)
	}
	if len(result) == 0 {
		return nil, redis.Nil
	}
	return mapToProfile(result)
}

func (s *RedisStore) SetVersioned(ctx context.Context, key string, profile *domain.InvestorProfile, ttl time.Duration) (bool, error) {
	args := []interface{}{
		profile.CacheVersion,
		int(ttl.Seconds()),
		"investor_id", profile.InvestorID,
		"full_name", profile.FullName,
		"email", profile.Email,
		"risk_profile", profile.RiskProfile,
		"kyc_status", profile.KYCStatus,
		"portfolio_value", profile.PortfolioValue,
		"preferences", profile.Preferences,
		"qualified_investor", strconv.FormatBool(profile.QualifiedInvestor),
		"investment_horizon", profile.InvestmentHorizon,
		"updated_at", profile.UpdatedAt,
		"cache_version", profile.CacheVersion,
	}

	result, err := s.versionScript.Run(ctx, s.client, []string{key}, args...).Int64()
	if err != nil {
		return false, fmt.Errorf("redis versioned set failed: %w", err)
	}
	return result == 1, nil
}

func (s *RedisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

func (s *RedisStore) DeleteBatch(ctx context.Context, keys []string) error {
	pipe := s.client.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) Client() *redis.ClusterClient {
	return s.client
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func mapToProfile(m map[string]string) (*domain.InvestorProfile, error) {
	version, err := strconv.ParseInt(m["cache_version"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid cache_version: %w", err)
	}
	qualified, err := strconv.ParseBool(m["qualified_investor"])
	if err != nil {
		return nil, fmt.Errorf("invalid qualified_investor: %w", err)
	}
	return &domain.InvestorProfile{
		InvestorID:        m["investor_id"],
		FullName:          m["full_name"],
		Email:             m["email"],
		RiskProfile:       m["risk_profile"],
		KYCStatus:         m["kyc_status"],
		PortfolioValue:    m["portfolio_value"],
		Preferences:       m["preferences"],
		QualifiedInvestor: qualified,
		InvestmentHorizon: m["investment_horizon"],
		UpdatedAt:         m["updated_at"],
		CacheVersion:      version,
	}, nil
}
