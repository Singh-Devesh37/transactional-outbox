package cleanup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/mocks"
)

func init() {
	// Initialize logger for tests
	logger.Init(false)
}

func TestNewCleanupWorker(t *testing.T) {
	repo := mocks.NewMockRepository()

	worker := NewCleanupWorker(
		repo,
		1*time.Hour,
		7*24*time.Hour,
		30*24*time.Hour,
	)

	if worker.interval != 1*time.Hour {
		t.Errorf("Expected interval of 1 hour, got %v", worker.interval)
	}
	if worker.sentRetention != 7*24*time.Hour {
		t.Errorf("Expected sent retention of 7 days, got %v", worker.sentRetention)
	}
	if worker.retryRetention != 30*24*time.Hour {
		t.Errorf("Expected retry retention of 30 days, got %v", worker.retryRetention)
	}
}

func TestCleanupWorker_RunCleanup_Success(t *testing.T) {
	repo := mocks.NewMockRepository()

	var sentDeleteCalled, deadDeleteCalled bool

	repo.DeleteOldSentEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		sentDeleteCalled = true
		return 5, nil
	}

	repo.DeleteOldDeadEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		deadDeleteCalled = true
		return 3, nil
	}

	worker := NewCleanupWorker(
		repo,
		1*time.Hour,
		7*24*time.Hour,
		30*24*time.Hour,
	)

	ctx := context.Background()
	worker.runCleanup(ctx)

	if !sentDeleteCalled {
		t.Error("DeleteOldSentEntries was not called")
	}
	if !deadDeleteCalled {
		t.Error("DeleteOldDeadEntries was not called")
	}

	// Verify the correct thresholds were used
	if len(repo.DeleteOldSentEntriesCalls) != 1 {
		t.Errorf("Expected 1 DeleteOldSentEntries call, got %d", len(repo.DeleteOldSentEntriesCalls))
	}
	if len(repo.DeleteOldDeadEntriesCalls) != 1 {
		t.Errorf("Expected 1 DeleteOldDeadEntries call, got %d", len(repo.DeleteOldDeadEntriesCalls))
	}
}

func TestCleanupWorker_RunCleanup_HandlesErrors(t *testing.T) {
	repo := mocks.NewMockRepository()

	repo.DeleteOldSentEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		return 0, errors.New("database error")
	}

	repo.DeleteOldDeadEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		return 0, errors.New("database error")
	}

	worker := NewCleanupWorker(
		repo,
		1*time.Hour,
		7*24*time.Hour,
		30*24*time.Hour,
	)

	ctx := context.Background()
	// Should not panic, just log errors
	worker.runCleanup(ctx)

	// Both functions should still be called despite errors
	if len(repo.DeleteOldSentEntriesCalls) != 1 {
		t.Error("DeleteOldSentEntries should be called even if it returns error")
	}
	if len(repo.DeleteOldDeadEntriesCalls) != 1 {
		t.Error("DeleteOldDeadEntries should be called even after sent delete fails")
	}
}

func TestCleanupWorker_Start_PeriodicExecution(t *testing.T) {
	repo := mocks.NewMockRepository()

	var callCount int32

	repo.DeleteOldSentEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		atomic.AddInt32(&callCount, 1)
		return 0, nil
	}

	// Very short interval for testing
	worker := NewCleanupWorker(
		repo,
		50*time.Millisecond,
		7*24*time.Hour,
		30*24*time.Hour,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go worker.Start(ctx)

	// Wait for context to expire
	<-ctx.Done()

	// Should have run at least twice in 200ms with 50ms interval
	// (initial run + at least one more)
	count := atomic.LoadInt32(&callCount)
	if count < 2 {
		t.Errorf("Expected at least 2 cleanup runs, got %d", count)
	}
}

func TestCleanupWorker_Stop_Manual(t *testing.T) {
	repo := mocks.NewMockRepository()

	var callCount int32

	repo.DeleteOldSentEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		atomic.AddInt32(&callCount, 1)
		return 0, nil
	}

	worker := NewCleanupWorker(
		repo,
		10*time.Millisecond,
		7*24*time.Hour,
		30*24*time.Hour,
	)

	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		worker.Start(ctx)
		close(done)
	}()

	// Let it run a bit
	time.Sleep(50 * time.Millisecond)

	// Stop manually
	worker.Stop()

	// Wait for goroutine to finish
	select {
	case <-done:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Worker did not stop in time")
	}
}

func TestCleanupWorker_RetentionThresholds(t *testing.T) {
	repo := mocks.NewMockRepository()

	var sentThreshold, deadThreshold time.Time

	repo.DeleteOldSentEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		sentThreshold = days
		return 0, nil
	}

	repo.DeleteOldDeadEntriesFunc = func(ctx context.Context, days time.Time) (int64, error) {
		deadThreshold = days
		return 0, nil
	}

	sentRetention := 7 * 24 * time.Hour
	deadRetention := 30 * 24 * time.Hour

	worker := NewCleanupWorker(
		repo,
		1*time.Hour,
		sentRetention,
		deadRetention,
	)

	ctx := context.Background()
	worker.runCleanup(ctx)

	now := time.Now()

	// Verify thresholds are approximately correct (within 1 second)
	expectedSentThreshold := now.Add(-sentRetention)
	if sentThreshold.Sub(expectedSentThreshold).Abs() > time.Second {
		t.Errorf("Sent threshold mismatch: expected ~%v, got %v", expectedSentThreshold, sentThreshold)
	}

	expectedDeadThreshold := now.Add(-deadRetention)
	if deadThreshold.Sub(expectedDeadThreshold).Abs() > time.Second {
		t.Errorf("Dead threshold mismatch: expected ~%v, got %v", expectedDeadThreshold, deadThreshold)
	}
}
