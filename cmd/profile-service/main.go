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

	"github.com/vladimirzankov/investor-cache/internal/metrics"
	"github.com/vladimirzankov/investor-cache/internal/outbox"
	"github.com/vladimirzankov/investor-cache/internal/profile"
	"github.com/vladimirzankov/investor-cache/pkg/config"
)

func main() {
	cfg := config.Load()

	db, err := openDB(&cfg.DB)
	if err != nil {
		log.Fatalf("failed to open postgres: %v", err)
	}
	defer db.Close()

	collector := metrics.NewProfileServiceCollector()

	writeRepo := profile.NewWriteRepository()
	outboxRepo := outbox.NewRepository(db)
	service := profile.NewService(db, writeRepo, outboxRepo, collector)
	handler := profile.NewHandler(service, collector)
	router := profile.NewRouter(handler)

	appServer := &http.Server{
		Addr:         ":" + cfg.ProfileService.Port,
		Handler:      router,
		ReadTimeout:  cfg.ProfileService.ReadTimeout,
		WriteTimeout: cfg.ProfileService.WriteTimeout,
		IdleTimeout:  cfg.ProfileService.IdleTimeout,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    ":" + cfg.ProfileService.MetricsPort,
		Handler: metricsMux,
	}

	go func() {
		log.Printf("profile-service metrics listening on :%s", cfg.ProfileService.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics server error: %v", err)
		}
	}()

	go func() {
		log.Printf("profile-service listening on :%s", cfg.ProfileService.Port)
		if err := appServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("app server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("received signal %s, shutting down", sig)

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
