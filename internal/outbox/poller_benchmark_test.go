package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/mocks"
	"transactional-outbox-go/internal/model"
)

func init() {
	logger.Init(false)
}

// BenchmarkEventDispatch measures the throughput of event dispatching
func BenchmarkEventDispatch(b *testing.B) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    5 * time.Second,
		BatchSize:   100,
		MaxRetries:  3,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       []byte(`{"order_id": "123", "amount": 99.99}`),
		Status:        "PENDING",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = poller.dispatchEvent(event)
	}
}

// BenchmarkEventDispatchWithRetries measures dispatch with retry logic
func BenchmarkEventDispatchWithRetries(b *testing.B) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    5 * time.Second,
		BatchSize:   100,
		MaxRetries:  3,
		BaseBackoff: 1,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       []byte(`{"order_id": "123", "amount": 99.99}`),
		Status:        "PENDING",
		Retries:       0,
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = poller.dispatchEventWithRetries(ctx, event)
	}
}

// BenchmarkProcessEvent measures full event processing (idempotency + dispatch + mark sent)
func BenchmarkProcessEvent(b *testing.B) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    5 * time.Second,
		BatchSize:   100,
		MaxRetries:  3,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		event := model.OutboxEvent{
			ID:            int64(i),
			AggregateId:   "order-123",
			AggregateType: "order",
			EventType:     "created",
			Payload:       []byte(`{"order_id": "123", "amount": 99.99}`),
			Status:        "PENDING",
		}
		poller.processEvent(ctx, event)
	}
}

// BenchmarkWorkerPool_1Worker measures throughput with 1 worker
func BenchmarkWorkerPool_1Worker(b *testing.B) {
	benchmarkWorkerPool(b, 1)
}

// BenchmarkWorkerPool_5Workers measures throughput with 5 workers
func BenchmarkWorkerPool_5Workers(b *testing.B) {
	benchmarkWorkerPool(b, 5)
}

// BenchmarkWorkerPool_10Workers measures throughput with 10 workers
func BenchmarkWorkerPool_10Workers(b *testing.B) {
	benchmarkWorkerPool(b, 10)
}

// BenchmarkWorkerPool_20Workers measures throughput with 20 workers
func BenchmarkWorkerPool_20Workers(b *testing.B) {
	benchmarkWorkerPool(b, 20)
}

func benchmarkWorkerPool(b *testing.B, workerCount int) {
	repo := mocks.NewMockRepository()

	var processedCount int64
	eventBatch := make([]model.OutboxEvent, 100)
	for i := range eventBatch {
		eventBatch[i] = model.OutboxEvent{
			ID:            int64(i + 1),
			AggregateId:   "order",
			AggregateType: "order",
			Payload:       []byte(`{"test": true}`),
		}
	}

	repo.FetchPendingEventsFunc = func(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
		return eventBatch, nil
	}

	producer := mocks.NewMockProducer()
	producer.SendMessageFunc = func(key, value []byte) error {
		atomic.AddInt64(&processedCount, 1)
		return nil
	}

	cfg := PollerConfig{
		Interval:    100 * time.Millisecond,
		BatchSize:   100,
		MaxRetries:  3,
		WorkerCount: workerCount,
	}

	poller := NewPoller(repo, producer, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start poller
	go poller.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	// Wait for b.N events to be processed
	for atomic.LoadInt64(&processedCount) < int64(b.N) {
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()
	cancel()
}

// BenchmarkLargePayload measures performance with large event payloads
func BenchmarkLargePayload(b *testing.B) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    5 * time.Second,
		BatchSize:   100,
		MaxRetries:  3,
		WorkerCount: 1,
	}

	poller := NewPoller(repo, producer, cfg)

	// Create a ~10KB payload
	largePayload := make([]byte, 10*1024)
	for i := range largePayload {
		largePayload[i] = byte('a' + (i % 26))
	}

	event := model.OutboxEvent{
		ID:            1,
		AggregateId:   "order-123",
		AggregateType: "order",
		EventType:     "created",
		Payload:       largePayload,
		Status:        "PENDING",
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		poller.processEvent(ctx, event)
	}
}

// BenchmarkCircuitBreaker measures overhead of circuit breaker
func BenchmarkCircuitBreaker_Closed(b *testing.B) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:             5 * time.Second,
		BatchSize:            100,
		MaxRetries:           3,
		WorkerCount:          1,
		CBMaxRequests:        3,
		CBInterval:           60 * time.Second,
		CBTimeout:            30 * time.Second,
		CBConsecutiveFailure: 5,
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = poller.dispatchEvent(event)
	}
}

// BenchmarkWithRateLimiting measures performance with rate limiting enabled
func BenchmarkWithRateLimiting(b *testing.B) {
	repo := mocks.NewMockRepository()
	producer := mocks.NewMockProducer()

	cfg := PollerConfig{
		Interval:    5 * time.Second,
		BatchSize:   100,
		MaxRetries:  3,
		WorkerCount: 1,
		RateLimit:   10000, // High limit to not actually slow down
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

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		poller.processEvent(ctx, event)
	}
}
