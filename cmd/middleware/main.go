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
	"github.com/vladimirzankov/investor-cache/internal/circuitbreaker"
	"github.com/vladimirzankov/investor-cache/internal/handler"
	"github.com/vladimirzankov/investor-cache/internal/metrics"
	"github.com/vladimirzankov/investor-cache/internal/reconciler"
	"github.com/vladimirzankov/investor-cache/internal/repository"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

func main() {
	cfg := config.Load()

	collector := metrics.NewCollector()

	repo, err := repository.NewPostgresRepository(&cfg.DB)
	if err != nil {
		log.Fatalf("failed to init postgres: %v", err)
	}
	defer repo.Close()

	redisStore, err := cache.NewRedisStore(&cfg.Redis)
	if err != nil {
		log.Fatalf("failed to init redis: %v", err)
	}
	defer redisStore.Close()

	cb := circuitbreaker.NewCircuitBreaker(collector)

	cacheManager := cache.NewCacheManager(redisStore, repo, cb, collector, cfg.Cache.TTL)

	profileHandler := handler.NewProfileHandler(cacheManager, repo, collector)
	router := handler.NewRouter(profileHandler)

	appServer := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + cfg.Server.MetricsPort,
		Handler: metricsMux,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := reconciler.NewReconciler(
		redisStore.Client(),
		redisStore,
		repo,
		collector,
		cfg.Cache.ReconcileInterval,
		cfg.Cache.ReconcileSampleSize,
	)
	go rec.Run(ctx)

	go func() {
		log.Printf("metrics server listening on :%s", cfg.Server.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server error: %v", err)
		}
	}()

	go func() {
		log.Printf("app server listening on :%s", cfg.Server.Port)
		if err := appServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("app server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %s, shutting down", sig)

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := appServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("app server shutdown error: %v", err)
	}
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("metrics server shutdown error: %v", err)
	}

	log.Println("shutdown complete")
}
