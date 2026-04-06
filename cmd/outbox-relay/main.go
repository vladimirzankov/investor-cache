package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"

	"github.com/vladimirzankov/investor-cache/internal/metrics"
	"github.com/vladimirzankov/investor-cache/internal/outbox"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

func main() {
	cfg := config.Load()

	db, err := openDB(&cfg.DB)
	if err != nil {
		log.Fatalf("failed to open postgres: %v", err)
	}
	defer db.Close()

	collector := metrics.NewOutboxRelayCollector()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Kafka.Brokers...),
		Topic:                  cfg.Kafka.Topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		AllowAutoTopicCreation: false,
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
	}
	defer func() {
		if err := writer.Close(); err != nil {
			log.Printf("kafka writer close error: %v", err)
		}
	}()

	repo := outbox.NewRepository(db)
	relay := outbox.NewRelay(repo, writer, collector, cfg.OutboxRelay.PollInterval, cfg.OutboxRelay.BatchSize)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + cfg.OutboxRelay.MetricsPort,
		Handler: metricsMux,
	}

	go func() {
		log.Printf("outbox-relay metrics listening on :%s", cfg.OutboxRelay.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server error: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	relayDone := make(chan struct{})
	go func() {
		log.Printf("outbox-relay polling outbox every %s (batch=%d)",
			cfg.OutboxRelay.PollInterval, cfg.OutboxRelay.BatchSize)
		if err := relay.Run(ctx); err != nil {
			log.Printf("relay run error: %v", err)
		}
		close(relayDone)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %s, shutting down", sig)

	cancel()
	<-relayDone

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}

	log.Println("shutdown complete")
}

func openDB(cfg *config.DBConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.DSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
