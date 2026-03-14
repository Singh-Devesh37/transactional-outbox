# Transactional Outbox Pattern - Go Implementation

A production-ready implementation of the **Transactional Outbox Pattern** in Go, ensuring reliable event delivery to Kafka in distributed systems.

## Overview

The Transactional Outbox Pattern solves the dual-write problem in microservices by:
1. Writing events to a database table (outbox) within the same transaction as business data
2. Asynchronously polling and publishing events to Kafka
3. Guaranteeing at-least-once delivery with idempotency checks

This implementation includes circuit breaker protection, exponential backoff with jitter, dead letter queue handling, and comprehensive observability.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Application Service                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   Poller     │    │   Cleanup    │    │  HTTP Server │                   │
│  │  (goroutine) │    │   Worker     │    │  :8080       │                   │
│  └──────┬───────┘    └──────────────┘    └──────────────┘                   │
│         │                                      │                            │
│         ▼                                      ▼                            │
│  ┌──────────────┐                       /healthz, /metrics                  │
│  │   Circuit    │                                                           │
│  │   Breaker    │                                                           │
│  └──────┬───────┘                                                           │
│         │                                                                   │
└─────────┼───────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────┐              ┌─────────────────┐
│   PostgreSQL    │              │     Kafka       │
│   ┌───────────┐ │              │  ┌───────────┐  │
│   │  outbox   │ │─────────────▶│  │  events   │  │
│   │  _event   │ │              │  │  topic    │  │
│   └───────────┘ │              │  └───────────┘  │
│   ┌───────────┐ │              │  ┌───────────┐  │
│   │ published │ │              │  │    DLQ    │  │
│   │  _event   │ │              │  │   topic   │  │
│   └───────────┘ │              │  └───────────┘  │
└─────────────────┘              └─────────────────┘
```

## Features

### Core Functionality
- **Transactional Outbox Pattern**: Events stored atomically with business data
- **Reliable Delivery**: At-least-once delivery with idempotency tracking
- **Async Processing**: Non-blocking event publishing via background poller

### Resilience
- **Circuit Breaker**: Sony gobreaker protecting Kafka producer (configurable thresholds)
- **Exponential Backoff**: Configurable retry delays with jitter to prevent thundering herd
- **Dead Letter Queue**: Failed events after max retries sent to DLQ topic
- **Graceful Shutdown**: Context-based cancellation with in-flight event draining
- **Rate Limiting**: Configurable rate limiter to prevent Kafka overload

### Concurrency
- **Worker Pool**: Configurable concurrent workers for parallel event processing
- **Backpressure Handling**: Automatic throttling when workers are saturated

### Observability
- **Prometheus Metrics**: 11 metrics including counters, gauges, and histograms
- **Grafana Dashboard**: Pre-built dashboard with throughput, latency, and health panels
- **Structured Logging**: Uber Zap for high-performance JSON logging
- **Health Endpoint**: `/healthz` for liveness probes

### Operations
- **Automated Cleanup**: Configurable retention for SENT (7 days) and DEAD (30 days) events
- **Docker Support**: Multi-stage Dockerfile and Docker Compose orchestration
- **Configuration**: YAML config with environment variable overrides

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Go 1.23+ (for local development)

### Run with Docker Compose

```bash
# Clone the repository
git clone https://github.com/deveshsingh3721/transactional-outbox-go.git
cd transactional-outbox-go

# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f app

# View metrics
curl http://localhost:8080/metrics
```

### Local Development

```bash
# Start dependencies
docker-compose up -d postgres kafka

# Run the application
go run cmd/app/main.go
```

## Configuration

### config.yaml

```yaml
app:
  port: "8080"
  poller_interval: 5        # Seconds between polling cycles
  batch_size: 10            # Events per polling cycle
  max_retries: 5            # Max retry attempts before DLQ
  base_backoff: 1           # Base backoff in seconds
  breaker_backoff: 1        # Backoff when circuit breaker open
  cleanup_interval: 1       # Hours between cleanup runs
  cleanup_sent_threshold: 7 # Days to retain SENT events
  cleanup_dead_threshold: 30 # Days to retain DEAD events

db:
  host: postgres
  port: 5432
  user: postgres
  password: postgres
  dbname: outboxdb
  sslmode: disable

kafka:
  brokers:
    - "kafka:9092"
  topic: "outbox_events"
  dlq_topic: "outbox_dlq_events"
  acks: "all"
  retries: 5
  compression: "snappy"
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | PostgreSQL host | postgres |
| `DB_PORT` | PostgreSQL port | 5432 |
| `KAFKA_BROKERS` | Kafka broker addresses | kafka:9092 |

## Database Schema

### outbox_event
Stores events pending publication to Kafka.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL | Primary key |
| aggregate_id | TEXT | Entity identifier |
| aggregate_type | TEXT | Entity type (e.g., "order", "user") |
| event_type | TEXT | Event type (e.g., "created", "updated") |
| payload | JSONB | Event data |
| status | TEXT | PENDING, RETRY, SENT, DEAD |
| retries | INT | Current retry count |
| last_error | TEXT | Last error message |
| next_attempt_at | TIMESTAMPTZ | Next retry timestamp |
| sent_at | TIMESTAMPTZ | When successfully published |
| dlq_at | TIMESTAMPTZ | When moved to DLQ |

### published_event
Idempotency tracking table.

| Column | Type | Description |
|--------|------|-------------|
| event_id | BIGINT | Reference to outbox_event.id |
| published_at | TIMESTAMPTZ | Publication timestamp |

## Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `outbox_events_pending_total` | Gauge | Current pending events |
| `outbox_events_sent_total` | Counter | Total events sent |
| `outbox_events_retry_total` | Counter | Total retry attempts |
| `outbox_events_dlq_total` | Counter | Total events sent to DLQ |
| `circuit_breaker_state` | Gauge | Circuit breaker state (0=closed, 1=half-open, 2=open) |
| `poller_processing_duration_seconds` | Histogram | Time per polling cycle |
| `event_dispatch_duration_seconds` | Histogram | Time per event dispatch |
| `cleanup_sent_deleted_total` | Counter | SENT events cleaned up |
| `cleanup_dead_deleted_total` | Counter | DEAD events cleaned up |

## Event Lifecycle

```
PENDING ──────────────────────────────────────────────────▶ SENT
    │                                                         │
    │ (failure)                                               │
    ▼                                                         ▼
  RETRY ──(retries < max)──▶ PENDING                    published_event
    │                                                    (idempotency)
    │ (retries >= max)
    ▼
   DEAD ──────────────────────────────────────────────▶ DLQ Topic
```

## Project Structure

```
.
├── cmd/app/main.go           # Application entry point
├── internal/
│   ├── config/               # Configuration loading (Viper)
│   ├── kafka/                # Kafka producer wrapper
│   ├── logger/               # Zap logger initialization
│   ├── metrics/              # Prometheus metrics definitions
│   ├── mocks/                # Mock implementations for testing
│   ├── model/                # Data models (OutboxEvent)
│   ├── outbox/               # Core poller logic, worker pool, circuit breaker
│   ├── cleanup/              # Cleanup worker
│   └── persistence/          # Database repository
├── grafana/
│   ├── dashboards/           # Pre-built Grafana dashboards
│   └── provisioning/         # Dashboard auto-provisioning config
├── docker/init/              # SQL initialization scripts
├── config.yaml               # Application configuration
├── docker-compose.yml        # Service orchestration
├── Dockerfile                # Multi-stage build
└── README.md
```

## Testing

```bash
# Run all tests
go test ./... -v

# Run with coverage
go test ./... -cover

# Run benchmarks
go test -bench=. -benchmem ./internal/outbox/...
```

Current test coverage focuses on:
- Poller unit tests (worker pool, retry logic, graceful shutdown)
- Cleanup worker tests (periodic execution, error handling)
- Mock implementations for Kafka producer and Repository

## Key Design Decisions

### Why Polling Over CDC?
- Simpler to implement and debug
- No additional infrastructure (Debezium, Kafka Connect)
- Sufficient for moderate event volumes (~1000 events/sec with tuning)

### Why Circuit Breaker?
- Prevents cascading failures when Kafka is unavailable
- Allows system to recover gracefully
- Provides backpressure without losing events

### Why Exponential Backoff with Jitter?
- Prevents thundering herd problem
- Spreads retry load over time
- Industry standard approach (AWS, Google recommendations)

## Performance Characteristics

### Benchmark Results

Benchmarks run on Apple M-series chip with mocked Kafka/DB:

| Benchmark | Operations/sec | Latency (ns/op) | Allocs/op |
|-----------|---------------|-----------------|-----------|
| Event Dispatch | ~2,000,000 | 496 | 5 |
| Dispatch with Retries | ~2,300,000 | 542 | 5 |
| Full Event Processing | ~1,400,000 | 812 | 7 |
| Circuit Breaker Overhead | ~2,600,000 | 453 | 5 |

### Configuration Defaults

| Metric | Value | Notes |
|--------|-------|-------|
| Polling Interval | 5 seconds | Configurable |
| Batch Size | 100 events | Configurable |
| Worker Count | 5 workers | Configurable |
| Rate Limit | 1000 events/sec | Configurable (0 = unlimited) |
| Circuit Breaker Threshold | 5 failures | Opens circuit |
| Max Retries | 5 attempts | Before DLQ |

### Throughput Estimates

With default configuration:
- **Single Worker**: ~1,000 events/second (limited by polling interval)
- **5 Workers**: ~5,000 events/second theoretical max
- **Production**: Actual throughput depends on Kafka/DB latency

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| Language | Go 1.23 | Main implementation |
| Database | PostgreSQL 15 | Event persistence |
| Message Broker | Apache Kafka 7.4 (KRaft) | Event streaming |
| Kafka Client | confluent-kafka-go | Kafka producer |
| Database Driver | pgx/v5 | PostgreSQL driver with connection pooling |
| Configuration | Viper | Config management |
| Logging | Zap | Structured logging |
| Metrics | Prometheus client | Observability |
| Circuit Breaker | gobreaker | Fault tolerance |

## License

MIT License - see [LICENSE](LICENSE) for details.

## References

- [Microservices.io - Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
- [Designing Data-Intensive Applications](https://dataintensive.net/) - Martin Kleppmann
- [Kafka: The Definitive Guide](https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/)
