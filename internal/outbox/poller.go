package outbox

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"transactional-outbox-go/internal/config"
	"transactional-outbox-go/internal/kafka"
	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/metrics"
	"transactional-outbox-go/internal/model"
	"transactional-outbox-go/internal/persistence"

	"github.com/sony/gobreaker"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Poller handles fetching and dispatching outbox events with worker pool
type Poller struct {
	repo        persistence.Repository
	producer    kafka.MessageProducer
	interval    time.Duration
	batch       int
	maxRetries  int
	baseBackoff int
	stopChan    chan struct{}

	breaker        *gobreaker.CircuitBreaker
	breakerBackoff int

	// Worker pool
	workerCount int
	jobChan     chan model.OutboxEvent
	wg          sync.WaitGroup

	// Rate limiting
	rateLimiter *rate.Limiter

	// Graceful shutdown
	drainWg sync.WaitGroup
}

// PollerConfig holds all configuration for the poller
type PollerConfig struct {
	Interval       time.Duration
	BatchSize      int
	MaxRetries     int
	BaseBackoff    int
	BreakerBackoff int
	WorkerCount    int
	RateLimit      int // events per second, 0 = unlimited

	// Circuit breaker settings
	CBMaxRequests        uint32
	CBInterval           time.Duration
	CBTimeout            time.Duration
	CBConsecutiveFailure uint32
}

// NewPollerConfigFromAppConfig creates PollerConfig from AppConfig
func NewPollerConfigFromAppConfig(cfg *config.AppConfig) PollerConfig {
	return PollerConfig{
		Interval:             time.Duration(cfg.PollerInterval) * time.Second,
		BatchSize:            cfg.BatchSize,
		MaxRetries:           cfg.MaxRetries,
		BaseBackoff:          cfg.BaseBackoff,
		BreakerBackoff:       cfg.BreakerBackoff,
		WorkerCount:          cfg.WorkerCount,
		RateLimit:            cfg.RateLimitPerSecond,
		CBMaxRequests:        uint32(cfg.CBMaxRequests),
		CBInterval:           time.Duration(cfg.CBInterval) * time.Second,
		CBTimeout:            time.Duration(cfg.CBTimeout) * time.Second,
		CBConsecutiveFailure: uint32(cfg.CBConsecutiveFailure),
	}
}

// NewPoller creates a new Poller with worker pool and rate limiting
func NewPoller(repo persistence.Repository, producer kafka.MessageProducer, cfg PollerConfig) *Poller {
	// Set defaults if not configured
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 5
	}
	if cfg.CBMaxRequests == 0 {
		cfg.CBMaxRequests = 3
	}
	if cfg.CBInterval == 0 {
		cfg.CBInterval = 60 * time.Second
	}
	if cfg.CBTimeout == 0 {
		cfg.CBTimeout = 30 * time.Second
	}
	if cfg.CBConsecutiveFailure == 0 {
		cfg.CBConsecutiveFailure = 5
	}

	st := gobreaker.Settings{
		Name:        "kafka_publisher",
		MaxRequests: cfg.CBMaxRequests,
		Interval:    cfg.CBInterval,
		Timeout:     cfg.CBTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= cfg.CBConsecutiveFailure
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.L().Warn("Circuit Breaker state changed",
				zap.String("name", name),
				zap.String("from", from.String()),
				zap.String("to", to.String()))
			var val float64
			switch to {
			case gobreaker.StateClosed:
				val = 0
			case gobreaker.StateHalfOpen:
				val = 1
			case gobreaker.StateOpen:
				val = 2
			}
			metrics.CircuitBreakerState.WithLabelValues(name).Set(val)
		},
	}

	// Create rate limiter (0 = unlimited)
	var limiter *rate.Limiter
	if cfg.RateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(cfg.RateLimit), cfg.RateLimit) // burst = rate limit
	}

	p := &Poller{
		repo:           repo,
		producer:       producer,
		interval:       cfg.Interval,
		batch:          cfg.BatchSize,
		maxRetries:     cfg.MaxRetries,
		baseBackoff:    cfg.BaseBackoff,
		breakerBackoff: cfg.BreakerBackoff,
		stopChan:       make(chan struct{}),
		breaker:        gobreaker.NewCircuitBreaker(st),
		workerCount:    cfg.WorkerCount,
		jobChan:        make(chan model.OutboxEvent, cfg.BatchSize),
		rateLimiter:    limiter,
	}

	return p
}

// Start begins the polling loop and worker pool
func (p *Poller) Start(ctx context.Context) {
	// Start worker pool
	for i := 0; i < p.workerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}

	logger.L().Info("Poller started",
		zap.String("interval", p.interval.String()),
		zap.Int("workers", p.workerCount),
		zap.Int("batch_size", p.batch))

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.processPendingEvents(ctx)
		case <-ctx.Done():
			logger.L().Info("Poller shutting down, draining in-flight events...")
			p.gracefulShutdown()
			return
		case <-p.stopChan:
			logger.L().Info("Poller stopped manually, draining in-flight events...")
			p.gracefulShutdown()
			return
		}
	}
}

// gracefulShutdown waits for in-flight events to complete
func (p *Poller) gracefulShutdown() {
	// Close job channel to signal workers to stop accepting new jobs
	close(p.jobChan)

	// Wait for all workers to finish processing current jobs
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		p.drainWg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
		logger.L().Info("All in-flight events processed successfully")
	case <-time.After(30 * time.Second):
		logger.L().Warn("Graceful shutdown timeout, some events may not have been processed")
	}
}

// Stop signals the poller to stop
func (p *Poller) Stop() {
	close(p.stopChan)
}

// worker is a goroutine that processes events from the job channel
func (p *Poller) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	logger.L().Debug("Worker started", zap.Int("worker_id", id))

	for event := range p.jobChan {
		p.drainWg.Add(1)
		p.processEvent(ctx, event)
		p.drainWg.Done()
	}

	logger.L().Debug("Worker stopped", zap.Int("worker_id", id))
}

// processEvent handles a single event with retries
func (p *Poller) processEvent(ctx context.Context, event model.OutboxEvent) {
	// Apply rate limiting
	if p.rateLimiter != nil {
		if err := p.rateLimiter.Wait(ctx); err != nil {
			logger.L().Warn("Rate limiter cancelled", zap.Int64("event_id", event.ID), zap.Error(err))
			return
		}
	}

	// Check idempotency
	alreadyPublished, err := p.repo.CheckAndMarkPublished(ctx, event.ID)
	if err != nil {
		logger.L().Error("Failed to check if event is already published",
			zap.Int64("event_id", event.ID), zap.Error(err))
		return
	}

	if alreadyPublished {
		logger.L().Debug("Event already published, skipping", zap.Int64("event_id", event.ID))
		return
	}

	dispatchStart := time.Now()

	if err := p.dispatchEventWithRetries(ctx, event); err != nil {
		logger.L().Error("Failed to dispatch event",
			zap.Int64("event_id", event.ID), zap.Error(err))
		metrics.RetryEventsCounter.Inc()
		return
	}

	if err := p.repo.MarkEventSent(ctx, event.ID); err != nil {
		logger.L().Error("Failed to mark event SENT",
			zap.Int64("event_id", event.ID), zap.Error(err))
		return
	}

	metrics.SentEventsCounter.Inc()
	metrics.EventDispatchDuration.Observe(time.Since(dispatchStart).Seconds())
	logger.L().Info("Event marked as SENT", zap.Int64("event_id", event.ID))
}

func (p *Poller) dispatchEvent(event model.OutboxEvent) error {
	key := []byte(fmt.Sprintf("%s:%s", event.AggregateType, event.AggregateId))
	value := event.Payload

	_, err := p.breaker.Execute(func() (any, error) {
		return nil, p.producer.SendMessage(key, value)
	})

	if err != nil {
		logger.L().Error("Failed to publish event, sending to DLQ",
			zap.Int64("event_id", event.ID),
			zap.String("aggregate_type", event.AggregateType),
			zap.Error(err))
		if dlqErr := p.producer.SendDLQMessage(key, value); dlqErr != nil {
			logger.L().Error("Failed to publish to DLQ", zap.Error(dlqErr))
		}
		return fmt.Errorf("failed to write event %d to kafka: %w", event.ID, err)
	}

	logger.L().Debug("Event dispatched",
		zap.Int64("event_id", event.ID),
		zap.String("aggregate_type", event.AggregateType),
		zap.String("aggregate_id", event.AggregateId))
	return nil
}

func (p *Poller) dispatchEventWithRetries(ctx context.Context, event model.OutboxEvent) error {
	var lastErr error
	for attempt := event.Retries + 1; attempt <= p.maxRetries; attempt++ {
		// Check if circuit breaker is open
		if p.breaker.State() == gobreaker.StateOpen {
			lastErr = fmt.Errorf("circuit breaker is open, skipping publishing for event %d", event.ID)

			jitter := time.Duration(p.breakerBackoff) * time.Second
			if retryErr := p.repo.MarkEventRetry(ctx, event.ID, attempt, jitter, lastErr.Error()); retryErr != nil {
				logger.L().Error("Failed to mark event for retry due to breaker open",
					zap.Int64("event_id", event.ID), zap.Error(retryErr))
			}
			return lastErr
		}

		if err := p.dispatchEvent(event); err == nil {
			return nil
		} else {
			lastErr = err
			// Exponential Backoff with Jitter
			maxWait := p.baseBackoff * (1 << attempt)
			jitter := time.Duration(rand.Int63n(int64(maxWait))) * time.Second

			// Persist retry info in DB (retries + next_attempt_at)
			if retryErr := p.repo.MarkEventRetry(ctx, event.ID, attempt, jitter, err.Error()); retryErr != nil {
				logger.L().Error("Failed to mark event for retry",
					zap.Int64("event_id", event.ID), zap.Error(retryErr))
			}

			logger.L().Info("Retrying Event",
				zap.Int64("event_id", event.ID),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", p.maxRetries),
				zap.Duration("backoff", jitter))

			select {
			case <-time.After(jitter):
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retries: %w", ctx.Err())
			}
		}
	}

	// Attempts Exhausted! Moving to DLQ
	if dlqErr := p.repo.MarkEventDead(ctx, event.ID, lastErr.Error()); dlqErr != nil {
		logger.L().Error("Failed to move Event to DLQ",
			zap.Int64("event_id", event.ID), zap.Error(dlqErr))
	}
	metrics.DLQEventsCounter.Inc()
	return fmt.Errorf("event %d moved to DLQ after %d retries: %w", event.ID, p.maxRetries, lastErr)
}

func (p *Poller) processPendingEvents(ctx context.Context) {
	events, err := p.repo.FetchPendingEvents(ctx, p.batch)
	if err != nil {
		logger.L().Error("Failed to fetch pending events", zap.Error(err))
		return
	}

	if len(events) == 0 {
		return
	}

	metrics.PendingEventsGauge.Set(float64(len(events)))
	startTime := time.Now()

	// Dispatch events to workers via job channel
	for _, event := range events {
		select {
		case p.jobChan <- event:
			// Event sent to worker
		case <-ctx.Done():
			logger.L().Warn("Context cancelled while dispatching events to workers")
			return
		default:
			// Channel full, process directly (backpressure)
			logger.L().Warn("Worker pool saturated, processing event directly",
				zap.Int64("event_id", event.ID))
			p.processEvent(ctx, event)
		}
	}

	metrics.PollerProcessingDuration.Observe(time.Since(startTime).Seconds())
}
