package cleanup

import (
	"context"
	"time"

	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/metrics"
	"transactional-outbox-go/internal/persistence"

	"go.uber.org/zap"
)

// CleanupRepository defines the interface needed by CleanupWorker
type CleanupRepository interface {
	DeleteOldSentEntries(ctx context.Context, days time.Time) (int64, error)
	DeleteOldDeadEntries(ctx context.Context, days time.Time) (int64, error)
}

type CleanupWorker struct {
	repo           CleanupRepository
	interval       time.Duration
	sentRetention  time.Duration
	retryRetention time.Duration
	stopChan       chan struct{}
}

// NewCleanupWorker creates a new cleanup worker
func NewCleanupWorker(repo CleanupRepository, interval time.Duration, sentRetention time.Duration, retryRetention time.Duration) *CleanupWorker {
	return &CleanupWorker{
		repo:           repo,
		interval:       interval,
		sentRetention:  sentRetention,
		retryRetention: retryRetention,
		stopChan:       make(chan struct{}),
	}
}

// Ensure OutboxRepository implements CleanupRepository
var _ CleanupRepository = (*persistence.OutboxRepository)(nil)

func (c *CleanupWorker) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)

	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.runCleanup(ctx)
		case <-ctx.Done():
			logger.L().Info("Cleanup Worker shutting down")
			return
		case <-c.stopChan:
			logger.L().Info("Cleanup Worker stopped manually")
			return
		}
	}
}

func (c *CleanupWorker) runCleanup(ctx context.Context) {
	now := time.Now()

	sentBefore := now.Add(-c.sentRetention)
	retryBefore := now.Add(-c.retryRetention)

	sentDeleted, err := c.repo.DeleteOldSentEntries(ctx, sentBefore)

	if err != nil {
		logger.L().Error("Failed to delete old SENT events", zap.Error(err))
	} else if sentDeleted > 0 {
		logger.L().Info("Deleted Entries", zap.Int64("count", sentDeleted))
	}

	metrics.CleanupSentDeletedCounter.Add(float64(sentDeleted))

	deadDeleted, err := c.repo.DeleteOldDeadEntries(ctx, retryBefore)

	if err != nil {
		logger.L().Error("Failed to delete old DEAD events", zap.Error(err))
	} else if deadDeleted > 0 {
		logger.L().Info("Deleted Entries", zap.Int64("count", deadDeleted))
	}

	metrics.CleanupDeadDeletedCounter.Add(float64(deadDeleted))
}

func (c *CleanupWorker) Stop() {
	close(c.stopChan)
}
