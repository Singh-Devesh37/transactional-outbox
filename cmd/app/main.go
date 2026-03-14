package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"transactional-outbox-go/internal/cleanup"
	"transactional-outbox-go/internal/config"
	"transactional-outbox-go/internal/kafka"
	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/outbox"
	"transactional-outbox-go/internal/persistence"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Application version for tracking
const Version = "1.0.0"

func main() {

	logger.Init(true)

	cfg, err := config.LoadConfig("./")
	if err != nil {
		logger.L().Fatal("Failed to load Config", zap.Error(err))
	}

	logger.L().Info("Config Loaded", zap.String("config", fmt.Sprintf("%+v", cfg)))

	database, err := persistence.NewDBPool(cfg)

	if err != nil {
		logger.L().Fatal("Failed to create DB Pool", zap.Error(err))
	}

	defer database.Close()

	repo := persistence.NewOutboxRepository(database.Pool)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	kafkaProducer, err := kafka.NewProducer(cfg)

	if err != nil {
		logger.L().Fatal("Failed to create Kafka Producer", zap.Error(err))
	}
	defer kafkaProducer.Close()

	pollerCfg := outbox.NewPollerConfigFromAppConfig(&cfg.App)
	poller := outbox.NewPoller(repo, kafkaProducer, pollerCfg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		poller.Start(ctx)
	}()

	cleanup := cleanup.NewCleanupWorker(
		repo,
		time.Duration(cfg.App.CleanupInterval)*time.Hour,
		time.Duration(cfg.App.CleanupSentThreshold)*24*time.Hour,
		time.Duration(cfg.App.CleanupDeadThreshold)*24*time.Hour,
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		cleanup.Start(ctx)
	}()

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		logger.L().Info("Health Check Done", zap.String("method", r.Method), zap.String("remote addr", r.RemoteAddr), zap.String("path", r.URL.Path))
	})

	http.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr: ":" + cfg.App.Port,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L().Fatal("Error on Port", zap.String("port", cfg.App.Port), zap.Error(err))
		}
	}()

	logger.L().Info("Server started on Port", zap.String("port", cfg.App.Port))

	<-ctx.Done()
	logger.L().Info("Server shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.L().Fatal("Server shutdown failed", zap.String("port", cfg.App.Port), zap.Error(err))
	}

	wg.Wait()
	logger.L().Info("Application Stopped")
}
