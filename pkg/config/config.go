package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Server         ServerConfig
	Redis          RedisConfig
	DB             DBConfig
	Kafka          KafkaConfig
	Cache          CacheConfig
	ProfileService ProfileServiceConfig
	OutboxRelay    OutboxRelayConfig
}

type ServerConfig struct {
	Port         string
	MetricsPort  string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type RedisConfig struct {
	Addrs        []string
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int
}

type DBConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type KafkaConfig struct {
	Brokers []string
	Topic   string
	GroupID string
	DLQTopic string
}

type CacheConfig struct {
	TTL                time.Duration
	ReconcileInterval  time.Duration
	ReconcileSampleSize int
}

type ProfileServiceConfig struct {
	Port         string
	MetricsPort  string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

type OutboxRelayConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MetricsPort  string
}

func Load() *Config {
	return &Config{
		Server: ServerConfig{
			Port:         envOrDefault("SERVER_PORT", "8080"),
			MetricsPort:  envOrDefault("METRICS_PORT", "9091"),
			ReadTimeout:  envDurationOrDefault("SERVER_READ_TIMEOUT", 5*time.Second),
			WriteTimeout: envDurationOrDefault("SERVER_WRITE_TIMEOUT", 5*time.Second),
			IdleTimeout:  envDurationOrDefault("SERVER_IDLE_TIMEOUT", 120*time.Second),
		},
		Redis: RedisConfig{
			Addrs:        strings.Split(envOrDefault("REDIS_ADDRS", "redis-node-1:7001,redis-node-2:7002,redis-node-3:7003,redis-node-4:7004,redis-node-5:7005,redis-node-6:7006"), ","),
			MaxRetries:   envIntOrDefault("REDIS_MAX_RETRIES", 3),
			DialTimeout:  envDurationOrDefault("REDIS_DIAL_TIMEOUT", 5*time.Second),
			ReadTimeout:  envDurationOrDefault("REDIS_READ_TIMEOUT", 1*time.Second),
			WriteTimeout: envDurationOrDefault("REDIS_WRITE_TIMEOUT", 1*time.Second),
			PoolSize:     envIntOrDefault("REDIS_POOL_SIZE", 100),
			MinIdleConns: envIntOrDefault("REDIS_MIN_IDLE_CONNS", 20),
		},
		DB: DBConfig{
			DSN:             envOrDefault("DATABASE_DSN", "postgres://investor:investor@postgres:5432/investordb?sslmode=disable"),
			MaxOpenConns:    envIntOrDefault("DB_MAX_OPEN_CONNS", 25),
			MaxIdleConns:    envIntOrDefault("DB_MAX_IDLE_CONNS", 10),
			ConnMaxLifetime: envDurationOrDefault("DB_CONN_MAX_LIFETIME", 5*time.Minute),
		},
		Kafka: KafkaConfig{
			Brokers:  strings.Split(envOrDefault("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092"), ","),
			Topic:    envOrDefault("KAFKA_TOPIC", "profile-updates"),
			GroupID:  envOrDefault("KAFKA_GROUP_ID", "cache-invalidation-group"),
			DLQTopic: envOrDefault("KAFKA_DLQ_TOPIC", "profile-updates-dlq"),
		},
		Cache: CacheConfig{
			TTL:                 envDurationOrDefault("CACHE_TTL", 3600*time.Second),
			ReconcileInterval:   envDurationOrDefault("RECONCILE_INTERVAL", 5*time.Minute),
			ReconcileSampleSize: envIntOrDefault("RECONCILE_SAMPLE_SIZE", 100),
		},
		ProfileService: ProfileServiceConfig{
			Port:         envOrDefault("SERVER_PORT", "8081"),
			MetricsPort:  envOrDefault("METRICS_PORT", "9092"),
			ReadTimeout:  envDurationOrDefault("SERVER_READ_TIMEOUT", 5*time.Second),
			WriteTimeout: envDurationOrDefault("SERVER_WRITE_TIMEOUT", 5*time.Second),
			IdleTimeout:  envDurationOrDefault("SERVER_IDLE_TIMEOUT", 120*time.Second),
		},
		OutboxRelay: OutboxRelayConfig{
			PollInterval: envDurationOrDefault("OUTBOX_POLL_INTERVAL", 100*time.Millisecond),
			BatchSize:    envIntOrDefault("OUTBOX_BATCH_SIZE", 100),
			MetricsPort:  envOrDefault("METRICS_PORT", "9093"),
		},
	}
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envIntOrDefault(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func envDurationOrDefault(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}
