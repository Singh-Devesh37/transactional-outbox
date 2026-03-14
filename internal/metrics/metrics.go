package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	PendingEventsGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_events_pending_total",
		Help: "Number of pending events in outbox",
	})

	SentEventsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_events_sent_total",
		Help: "Number of events sent to kafka",
	})

	RetryEventsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_events_retry_total",
		Help: "Number of retry attempts for events",
	})

	DLQEventsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_events_dlq_total",
		Help: "Number of events moved to DLQ",
	})

	CleanupSentDeletedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cleanup_sent_deleted_total",
		Help: "Number of old SENT events deleted",
	})

	CleanupDeadDeletedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cleanup_dead_deleted_total",
		Help: "Number of old DEAD events deleted",
	})

	PollerProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "poller_processing_duration_seconds",
		Help:    "Time taken to process a batch of events",
		Buckets: prometheus.DefBuckets,
	})

	EventDispatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "event_dispatch_duration_seconds",
		Help:    "Time taken to dispatch an event to Kafka",
		Buckets: prometheus.DefBuckets,
	})

	CircuitBreakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Circuit breaker state (0=closed,1=half-open,2=open)",
		},
		[]string{"name"},
	)

	WorkerPoolActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "worker_pool_active_workers",
		Help: "Number of currently active workers processing events",
	})

	WorkerPoolQueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "worker_pool_queue_size",
		Help: "Current number of events waiting in the worker queue",
	})

	RateLimitWaitDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "rate_limit_wait_duration_seconds",
		Help:    "Time spent waiting for rate limiter",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1},
	})
)
