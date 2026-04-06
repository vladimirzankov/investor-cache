package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/vladimirzankov/investor-cache/internal/cache"
	kafkaconsumer "github.com/vladimirzankov/investor-cache/internal/kafka"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

func main() {
	cfg := config.Load()

	collector := metrics.NewCollector()

	redisStore, err := cache.NewRedisStore(&cfg.Redis)
	if err != nil {
		log.Fatalf("failed to init redis: %v", err)
	}
	defer redisStore.Close()

	consumer := kafkaconsumer.NewInvalidationConsumer(&cfg.Kafka, redisStore, collector)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + cfg.Server.MetricsPort,
		Handler: metricsMux,
	}

	go func() {
		log.Printf("invalidation-worker metrics listening on :%s", cfg.Server.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server error: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerDone := make(chan struct{})
	go func() {
		log.Println("invalidation-worker consuming profile-updates")
		if err := consumer.Run(ctx); err != nil {
			log.Printf("kafka consumer error: %v", err)
		}
		close(consumerDone)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %s, shutting down", sig)

	cancel()
	<-consumerDone

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}

	log.Println("shutdown complete")
}
