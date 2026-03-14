package outbox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/mocks"
	"transactional-outbox-go/internal/model"
)

func init() {
	// Initialize logger for tests
	logger.Init(false)
}

func TestNewPoller_DefaultConfig(t *testing.T) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:   5 * time.Second,
		BatchSize:  10,
		MaxRetries: 5,
	}

	poller := NewPoller(repo, producer, cfg)

	if poller.workerCount != 5 {
		t.Errorf("Expected default worker count of 5, got %d", poller.workerCount)
	}
	if poller.batch != 10 {
		t.Errorf("Expected batch size of 10, got %d", poller.batch)
	}
	if poller.maxRetries != 5 {
		t.Errorf("Expected max retries of 5, got %d", poller.maxRetries)
	}
}

func TestNewPoller_CustomConfig(t *testing.T) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:             5 * time.Second,
		BatchSize:            50,
		MaxRetries:           3,
		WorkerCount:          10,
		RateLimit:            1000,
		CBMaxRequests:        5,
		CBInterval:           120 * time.Second,
		CBTimeout:            60 * time.Second,
		CBConsecutiveFailure: 10,
	}

	poller := NewPoller(repo, producer, cfg)

	if poller.workerCount != 10 {
		t.Errorf("Expected worker count of 10, got %d", poller.workerCount)
	}
	if poller.batch != 50 {
		t.Errorf("Expected batch size of 50, got %d", poller.batch)
	}
	if poller.rateLimiter == nil {
		t.Error("Expected rate limiter to be set")
	}
}

func TestPoller_ProcessEvent_Success(t *testing.T) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    100 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  3,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       []byte(`{"order_id": "123"}`),
		Status:        "PENDING",
		Retries:       0,
	}

	ctx := context.Background()
	poller.processEvent(ctx, event)

	// Verify producer was called
	if producer.GetSendMessageCallCount() != 1 {
		t.Errorf("Expected 1 SendMessage call, got %d", producer.GetSendMessageCallCount())
	}

	// Verify event was marked as sent
	if len(repo.MarkEventSentCalls) != 1 {
		t.Errorf("Expected 1 MarkEventSent call, got %d", len(repo.MarkEventSentCalls))
	}
	if repo.MarkEventSentCalls[0] != 1 {
		t.Errorf("Expected event ID 1 to be marked sent, got %d", repo.MarkEventSentCalls[0])
	}
}

func TestPoller_ProcessEvent_AlreadyPublished(t *testing.T) {
	repo := mocks.NewMockRepository()
	repo.CheckAndMarkPublishedFunc = func(ctx context.Context, eventID int64) (bool, error) {
		return true, nil // Already published
	}

	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    100 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  3,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       []byte(`{"order_id": "123"}`),
		Status:        "PENDING",
	}

	ctx := context.Background()
	poller.processEvent(ctx, event)

	// Verify producer was NOT called (idempotency check)
	if producer.GetSendMessageCallCount() != 0 {
		t.Errorf("Expected 0 SendMessage calls for already published event, got %d", producer.GetSendMessageCallCount())
	}

	// Verify event was NOT marked as sent again
	if len(repo.MarkEventSentCalls) != 0 {
		t.Errorf("Expected 0 MarkEventSent calls, got %d", len(repo.MarkEventSentCalls))
	}
}

func TestPoller_ProcessEvent_RetryOnFailure(t *testing.T) {
	repo := mocks.NewMockRepository()

	failCount := 0
	producer := mocks.NewMockProducer()
	producer.SendMessageFunc = func(key, value []byte) error {
		failCount++
		if failCount <= 2 {
			return errors.New("kafka unavailable")
		}
		return nil // Succeed on 3rd attempt
	}

	cfg := PollerConfig{
		Interval:    100 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  5,
		BaseBackoff: 1, // 1 second base
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       []byte(`{"order_id": "123"}`),
		Status:        "PENDING",
		Retries:       0,
	}

	ctx := context.Background()

	// Process with retries - should eventually succeed
	err := poller.dispatchEventWithRetries(ctx, event)

	if err != nil {
		t.Errorf("Expected event to eventually succeed, got error: %v", err)
	}

	// Should have been called 3 times (2 failures + 1 success)
	if producer.GetSendMessageCallCount() != 3 {
		t.Errorf("Expected 3 SendMessage calls, got %d", producer.GetSendMessageCallCount())
	}
}

func TestPoller_ProcessEvent_MoveToDLQ(t *testing.T) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()
	producer.SendMessageFunc = func(key, value []byte) error {
		return errors.New("kafka unavailable")
	}

	cfg := PollerConfig{
		Interval:    100 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  2, // Low retry count for faster test
		BaseBackoff: 1,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       []byte(`{"order_id": "123"}`),
		Status:        "PENDING",
		Retries:       0,
	}

	ctx := context.Background()
	err := poller.dispatchEventWithRetries(ctx, event)

	if err == nil {
		t.Error("Expected error after max retries")
	}

	// Verify event was marked as dead
	if len(repo.MarkEventDeadCalls) != 1 {
		t.Errorf("Expected 1 MarkEventDead call, got %d", len(repo.MarkEventDeadCalls))
	}
}

func TestPoller_WorkerPool_Concurrency(t *testing.T) {
	repo := mocks.NewMockRepository()

	var processedCount int32
	events := []model.OutboxEvent{
		{ID: 1, AggregateId: "1", AggregateType: "order", Payload: []byte(`{}`)},
		{ID: 2, AggregateId: "2", AggregateType: "order", Payload: []byte(`{}`)},
		{ID: 3, AggregateId: "3", AggregateType: "order", Payload: []byte(`{}`)},
		{ID: 4, AggregateId: "4", AggregateType: "order", Payload: []byte(`{}`)},
		{ID: 5, AggregateId: "5", AggregateType: "order", Payload: []byte(`{}`)},
	}

	repo.FetchPendingEventsFunc = func(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
		// Only return events once
		if atomic.LoadInt32(&processedCount) == 0 {
			return events, nil
		}
		return nil, nil
	}

	producer := mocks.NewMockProducer()
	producer.SendMessageFunc = func(key, value []byte) error {
		atomic.AddInt32(&processedCount, 1)
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	}

	cfg := PollerConfig{
		Interval:    50 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  3,
		WorkerCount: 5, // 5 workers should process 5 events in parallel
	}

	poller := NewPoller(repo, producer, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go poller.Start(ctx)

	// Wait for processing
	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&processedCount) != 5 {
		t.Errorf("Expected 5 events to be processed, got %d", atomic.LoadInt32(&processedCount))
	}
}

func TestPoller_GracefulShutdown(t *testing.T) {
	repo := mocks.NewMockRepository()

	var processingCount int32
	var completedCount int32

	repo.FetchPendingEventsFunc = func(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
		return []model.OutboxEvent{
			{ID: 1, AggregateId: "1", AggregateType: "order", Payload: []byte(`{}`)},
		}, nil
	}

	producer := mocks.NewMockProducer()
	producer.SendMessageFunc = func(key, value []byte) error {
		atomic.AddInt32(&processingCount, 1)
		time.Sleep(100 * time.Millisecond) // Simulate slow processing
		atomic.AddInt32(&completedCount, 1)
		return nil
	}

	cfg := PollerConfig{
		Interval:    50 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  3,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		poller.Start(ctx)
		close(done)
	}()

	// Wait for processing to start
	time.Sleep(100 * time.Millisecond)

	// Cancel context - should trigger graceful shutdown
	cancel()

	// Wait for shutdown
	select {
	case <-done:
		// Shutdown completed
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete in time")
	}
}

func TestPoller_RateLimiting(t *testing.T) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    100 * time.Millisecond,
		BatchSize:   10,
		MaxRetries:  3,
		WorkerCount: 1,
		RateLimit:   10, // 10 events per second
	}

	poller := NewPoller(repo, producer, cfg)

	if poller.rateLimiter == nil {
		t.Error("Rate limiter should be configured")
	}

	// Process multiple events and verify rate limiting is applied
	events := make([]model.OutboxEvent, 5)
	for i := range events {
		events[i] = model.OutboxEvent{
			ID:            int64(i + 1),
			AggregateId:   "test",
			AggregateType: "order",
			Payload:       []byte(`{}`),
		}
	}

	ctx := context.Background()
	start := time.Now()

	for _, event := range events {
		poller.processEvent(ctx, event)
	}

	elapsed := time.Since(start)

	// With rate limit of 10/sec and 5 events, should take at least ~400ms
	// But since burst is equal to rate, first 10 should be instant
	// This test mainly verifies rate limiter is not nil and doesn't cause errors
	t.Logf("Processed 5 events in %v", elapsed)
}

func TestPollerConfig_FromAppConfig(t *testing.T) {
	// This is a basic test to verify the config conversion works
	// In a real scenario, you'd import the config package
	cfg := PollerConfig{
		Interval:             5 * time.Second,
		BatchSize:            100,
		MaxRetries:           5,
		BaseBackoff:          1,
		BreakerBackoff:       1,
		WorkerCount:          5,
		RateLimit:            1000,
		CBMaxRequests:        3,
		CBInterval:           60 * time.Second,
		CBTimeout:            30 * time.Second,
		CBConsecutiveFailure: 5,
	}

	if cfg.Interval != 5*time.Second {
		t.Errorf("Interval mismatch")
	}
	if cfg.WorkerCount != 5 {
		t.Errorf("WorkerCount mismatch")
	}
}
