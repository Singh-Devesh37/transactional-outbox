# Architecture Documentation

This document provides a deep dive into the architecture, design decisions, and implementation details of the Transactional Outbox Pattern service.

## Table of Contents
1. [System Overview](#system-overview)
2. [The Dual-Write Problem](#the-dual-write-problem)
3. [Component Architecture](#component-architecture)
4. [Data Flow](#data-flow)
5. [Resilience Patterns](#resilience-patterns)
6. [Concurrency Model](#concurrency-model)
7. [Database Design](#database-design)
8. [Observability](#observability)
9. [Deployment Architecture](#deployment-architecture)
10. [Trade-offs & Alternatives](#trade-offs--alternatives)

---

## System Overview

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              TRANSACTIONAL OUTBOX SERVICE                            │
├─────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                      │
│   ┌────────────────────────────────────────────────────────────────────────────┐    │
│   │                           main.go (Orchestrator)                            │    │
│   │   - Initializes all components                                              │    │
│   │   - Manages lifecycle with sync.WaitGroup                                   │    │
│   │   - Handles graceful shutdown (SIGINT/SIGTERM)                             │    │
│   └────────────────────────────────────────────────────────────────────────────┘    │
│                    │                    │                    │                       │
│                    ▼                    ▼                    ▼                       │
│   ┌──────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐        │
│   │      POLLER          │ │   CLEANUP WORKER     │ │    HTTP SERVER       │        │
│   │   (Goroutine #1)     │ │   (Goroutine #2)     │ │   (Goroutine #3)     │        │
│   │                      │ │                      │ │                      │        │
│   │ - Ticker: 5s         │ │ - Ticker: 1h         │ │ - Port: 8080         │        │
│   │ - Batch: 10 events   │ │ - SENT: 7 days       │ │ - /healthz           │        │
│   │ - Circuit Breaker    │ │ - DEAD: 30 days      │ │ - /metrics           │        │
│   │ - Retry Logic        │ │                      │ │                      │        │
│   └──────────┬───────────┘ └──────────┬───────────┘ └──────────────────────┘        │
│              │                        │                                              │
│              ▼                        ▼                                              │
│   ┌────────────────────────────────────────────────────────────────────────────┐    │
│   │                          OUTBOX REPOSITORY                                  │    │
│   │   - FetchPendingEvents()    - MarkEventSent()    - DeleteOldSentEntries()  │    │
│   │   - InsertEvent()           - MarkEventRetry()   - DeleteOldDeadEntries()  │    │
│   │   - CheckAndMarkPublished() - MarkEventDead()                              │    │
│   └────────────────────────────────────────────────────────────────────────────┘    │
│                                        │                                             │
└────────────────────────────────────────┼─────────────────────────────────────────────┘
                                         │
                   ┌─────────────────────┴─────────────────────┐
                   ▼                                           ▼
        ┌───────────────────┐                       ┌───────────────────┐
        │    PostgreSQL     │                       │      Kafka        │
        │   ┌───────────┐   │                       │   ┌───────────┐   │
        │   │  outbox   │   │                       │   │  outbox   │   │
        │   │  _event   │   │──────────────────────▶│   │  _events  │   │
        │   └───────────┘   │                       │   └───────────┘   │
        │   ┌───────────┐   │                       │   ┌───────────┐   │
        │   │ published │   │                       │   │  outbox   │   │
        │   │  _event   │   │                       │   │ _dlq_evts │   │
        │   └───────────┘   │                       │   └───────────┘   │
        └───────────────────┘                       └───────────────────┘
```

---

## The Dual-Write Problem

### Problem Statement

In distributed systems, when a service needs to both update its database AND publish an event to a message broker, we face the **dual-write problem**:

```
┌─────────────┐     1. Write     ┌─────────────┐
│   Service   │ ───────────────▶ │  Database   │  ✓ Success
└─────────────┘                  └─────────────┘
      │
      │ 2. Publish
      ▼
┌─────────────┐
│   Kafka     │  ✗ Failure (network issue, broker down)
└─────────────┘

Result: Database updated, but event never published
        → Data inconsistency between systems
```

### Failure Scenarios Without Outbox

| Scenario | Database | Kafka | Result |
|----------|----------|-------|--------|
| Both succeed | Updated | Published | Consistent |
| DB fails first | Rolled back | Not attempted | Consistent |
| Kafka fails after DB | Updated | Not published | **Inconsistent** |
| Service crashes between | Updated | Not published | **Inconsistent** |

### Solution: Transactional Outbox

```
┌─────────────┐                  ┌─────────────────────────┐
│   Service   │                  │       Database          │
└──────┬──────┘                  │  ┌─────────────────┐    │
       │                         │  │  Business Data  │    │
       │  Single Transaction     │  └─────────────────┘    │
       │ ─────────────────────▶  │  ┌─────────────────┐    │
       │  1. Update data         │  │   Outbox Event  │    │
       │  2. Insert event        │  └─────────────────┘    │
       │                         └─────────────────────────┘
                                            │
       ┌────────────────────────────────────┘
       │
       ▼ (Async Poller)
┌─────────────┐     Publish      ┌─────────────┐
│   Poller    │ ───────────────▶ │   Kafka     │
└─────────────┘                  └─────────────┘

Guarantee: Event is published if and only if transaction committed
```

---

## Component Architecture

### 1. Poller (`internal/outbox/poller.go`)

The core component responsible for event dispatch.

```
┌─────────────────────────────────────────────────────────────────────┐
│                            POLLER                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │    Ticker    │───▶│  Fetch Batch │───▶│  Process     │           │
│  │   (5 sec)    │    │  (10 events) │    │  Events      │           │
│  └──────────────┘    └──────────────┘    └──────┬───────┘           │
│                                                  │                   │
│                                                  ▼                   │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │                    FOR EACH EVENT                            │    │
│  │                                                              │    │
│  │   ┌────────────┐    ┌────────────┐    ┌────────────┐        │    │
│  │   │ Idempotency│───▶│  Circuit   │───▶│  Dispatch  │        │    │
│  │   │   Check    │    │  Breaker   │    │  to Kafka  │        │    │
│  │   └────────────┘    └────────────┘    └─────┬──────┘        │    │
│  │                                              │               │    │
│  │         ┌────────────────────────────────────┴───────┐      │    │
│  │         ▼                                            ▼      │    │
│  │   ┌──────────┐                               ┌──────────┐   │    │
│  │   │ SUCCESS  │                               │ FAILURE  │   │    │
│  │   │ Mark SENT│                               │ Retry or │   │    │
│  │   │          │                               │ DLQ      │   │    │
│  │   └──────────┘                               └──────────┘   │    │
│  │                                                              │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 2. Circuit Breaker (`gobreaker`)

Protects the system from cascading failures.

```
                    ┌──────────────────────────────────────────────┐
                    │              CIRCUIT BREAKER                  │
                    │                                               │
                    │   ┌─────────┐  5 failures  ┌─────────┐       │
                    │   │ CLOSED  │─────────────▶│  OPEN   │       │
                    │   │(normal) │              │(failing)│       │
                    │   └────┬────┘              └────┬────┘       │
                    │        │                        │            │
                    │        │ success               │ 60s timeout│
                    │        │                        │            │
                    │        │    ┌────────────┐     │            │
                    │        └────│ HALF-OPEN  │◀────┘            │
                    │             │ (testing)  │                   │
                    │             └────────────┘                   │
                    │               │        │                     │
                    │          success│    failure                 │
                    │               │        │                     │
                    │               ▼        ▼                     │
                    │            CLOSED    OPEN                    │
                    │                                               │
                    └──────────────────────────────────────────────┘

Configuration:
- MaxRequests: 3 (in half-open state)
- Interval: 60 seconds
- Timeout: 30 seconds
- Ready to Trip: 5 consecutive failures
```

### 3. Kafka Producer (`internal/kafka/producer.go`)

```
┌─────────────────────────────────────────────────────────────────┐
│                      KAFKA PRODUCER                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌────────────────────────────────────────────────────────┐    │
│   │                  Configuration                          │    │
│   │  - bootstrap.servers: kafka:9092                        │    │
│   │  - acks: all (wait for all replicas)                   │    │
│   │  - retries: 5                                          │    │
│   │  - compression.type: snappy                            │    │
│   │  - batch.size: 10000                                   │    │
│   │  - linger.ms: 10                                       │    │
│   └────────────────────────────────────────────────────────┘    │
│                                                                  │
│   ┌──────────────────┐        ┌──────────────────┐              │
│   │  SendMessage()   │        │ SendDLQMessage() │              │
│   │                  │        │                  │              │
│   │  Topic: outbox   │        │  Topic: outbox   │              │
│   │         _events  │        │         _dlq_evts│              │
│   │                  │        │                  │              │
│   │  Key: aggregate  │        │  Key: aggregate  │              │
│   │       _type:id   │        │       _type:id   │              │
│   └──────────────────┘        └──────────────────┘              │
│                                                                  │
│   ┌────────────────────────────────────────────────────────┐    │
│   │              Async Delivery Reporting                   │    │
│   │  - Background goroutine monitors Events() channel       │    │
│   │  - Logs delivery success/failure                        │    │
│   └────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Event Lifecycle State Machine

```
                              ┌─────────────────────────────────────┐
                              │         EVENT LIFECYCLE              │
                              └─────────────────────────────────────┘

    ┌─────────┐                                              ┌─────────┐
    │ INSERT  │                                              │ KAFKA   │
    │ EVENT   │                                              │ TOPIC   │
    └────┬────┘                                              └────▲────┘
         │                                                        │
         ▼                                                        │
    ┌─────────┐         ┌─────────┐         ┌─────────┐          │
    │ PENDING │────────▶│  RETRY  │────────▶│  RETRY  │──────────┤
    │         │ failure │   #1    │ failure │   #2    │ ... #5   │
    │         │         │         │         │         │  success │
    └────┬────┘         └─────────┘         └────┬────┘          │
         │                                       │                │
         │ success                               │ max retries    │
         │                                       ▼                │
         │                                  ┌─────────┐           │
         │                                  │  DEAD   │           │
         │                                  │         │           │
         │                                  └────┬────┘           │
         │                                       │                │
         │                                       ▼                │
         │                                  ┌─────────┐           │
         │                                  │   DLQ   │           │
         │                                  │  TOPIC  │           │
         │                                  └─────────┘           │
         │                                                        │
         └────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                            ┌─────────┐         ┌─────────────┐
                            │  SENT   │────────▶│  PUBLISHED  │
                            │         │         │   _EVENT    │
                            └─────────┘         │ (idempotent)│
                                                └─────────────┘
```

### Retry Flow with Exponential Backoff

```
Attempt 0: Immediate
     │
     ▼ failure
Attempt 1: wait 0-1s  (base_backoff * 2^0 = 1s max, random jitter)
     │
     ▼ failure
Attempt 2: wait 0-2s  (base_backoff * 2^1 = 2s max, random jitter)
     │
     ▼ failure
Attempt 3: wait 0-4s  (base_backoff * 2^2 = 4s max, random jitter)
     │
     ▼ failure
Attempt 4: wait 0-8s  (base_backoff * 2^3 = 8s max, random jitter)
     │
     ▼ failure
Attempt 5: wait 0-16s (base_backoff * 2^4 = 16s max, random jitter)
     │
     ▼ failure
     │
     ▼
  DEAD → Send to DLQ
```

**Why Jitter?**
- Prevents thundering herd when many events fail simultaneously
- Spreads retry load over time
- Formula: `jitter = rand.Int63n(baseBackoff * (1 << attempt))`

---

## Resilience Patterns

### Pattern 1: Circuit Breaker

**Purpose**: Fail fast when downstream is unavailable

```go
// Circuit breaker wraps Kafka produce calls
result, err := p.circuitBreaker.Execute(func() (interface{}, error) {
    return nil, p.producer.SendMessage(topic, key, value)
})

// If breaker is OPEN:
// - Immediate failure (ErrOpenState)
// - Event scheduled for retry with breaker_backoff
// - No load on failing Kafka

// If breaker is CLOSED:
// - Normal operation
// - Failures counted toward threshold
```

### Pattern 2: Idempotency

**Purpose**: Prevent duplicate event publishing

```
┌─────────────────────────────────────────────────────────────┐
│                    IDEMPOTENCY CHECK                         │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│   1. Before dispatch, check published_event table:          │
│      SELECT 1 FROM published_event WHERE event_id = ?       │
│                                                              │
│   2. If exists → skip (already published)                   │
│      If not exists → proceed with dispatch                  │
│                                                              │
│   3. After successful dispatch (in transaction):            │
│      - UPDATE outbox_event SET status='SENT'                │
│      - INSERT INTO published_event (event_id, published_at) │
│                                                              │
│   Race Condition Handling:                                   │
│      INSERT ... ON CONFLICT DO NOTHING                       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Pattern 3: Dead Letter Queue

**Purpose**: Preserve failed events for investigation

```
Event Processing Failure Path:
─────────────────────────────────

     [Event fails max_retries times]
                 │
                 ▼
     ┌───────────────────────┐
     │  Mark status = DEAD   │
     │  Set dlq_at = NOW()   │
     └───────────┬───────────┘
                 │
                 ▼
     ┌───────────────────────┐
     │  Send to DLQ Topic    │
     │  (outbox_dlq_events)  │
     └───────────┬───────────┘
                 │
                 ▼
     ┌───────────────────────┐
     │  Increment metric:    │
     │  outbox_events_dlq    │
     └───────────────────────┘

DLQ allows:
- Manual investigation of failures
- Replay after fixing issues
- Audit trail of problematic events
```

---

## Concurrency Model

### Goroutine Architecture

```
main()
   │
   ├──▶ goroutine #1: Poller.Start(ctx)
   │        └── Ticker loop, processes events sequentially
   │
   ├──▶ goroutine #2: CleanupWorker.Start(ctx)
   │        └── Ticker loop, deletes old records
   │
   └──▶ goroutine #3: http.ListenAndServe()
            └── HTTP server for health/metrics

sync.WaitGroup coordinates shutdown:
- wg.Add(3) before starting goroutines
- wg.Done() called in each goroutine's defer
- wg.Wait() blocks until all complete
```

### Context Propagation

```
┌─────────────────────────────────────────────────────────────────┐
│                     GRACEFUL SHUTDOWN                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   SIGINT/SIGTERM received                                        │
│          │                                                       │
│          ▼                                                       │
│   cancel() called → ctx.Done() triggered                         │
│          │                                                       │
│          ├──▶ Poller: exits ticker loop                          │
│          ├──▶ Cleanup: exits ticker loop                         │
│          └──▶ HTTP Server: Shutdown(shutdownCtx)                 │
│                                                                  │
│   shutdownCtx has 5-second timeout:                              │
│   - Allows in-flight requests to complete                        │
│   - Forces exit if cleanup takes too long                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Database Design

### Schema Relationships

```
┌─────────────────────────────────────────────────────────────────┐
│                      outbox_event                                │
├─────────────────────────────────────────────────────────────────┤
│ id (PK)              SERIAL                                      │
│ aggregate_id         TEXT NOT NULL                               │
│ aggregate_type       TEXT NOT NULL                               │
│ event_type           TEXT NOT NULL                               │
│ payload              JSONB NOT NULL                              │
│ status               TEXT DEFAULT 'PENDING'                      │
│ retries              INT DEFAULT 0                               │
│ last_error           TEXT                                        │
│ next_attempt_at      TIMESTAMPTZ DEFAULT now()                   │
│ sent_at              TIMESTAMPTZ                                 │
│ dlq_at               TIMESTAMPTZ                                 │
│ created              TIMESTAMPTZ DEFAULT now()                   │
│ updated              TIMESTAMPTZ DEFAULT now()                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ 1:1
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     published_event                              │
├─────────────────────────────────────────────────────────────────┤
│ event_id (PK, FK)    BIGINT → outbox_event.id                   │
│ published_at         TIMESTAMPTZ DEFAULT now()                   │
└─────────────────────────────────────────────────────────────────┘
```

### Index Strategy

```sql
-- Fast lookup for polling: status + next_attempt_at
CREATE INDEX idx_outbox_status_created ON outbox_event(status, created);

-- Fast lookup for SENT cleanup
CREATE INDEX idx_outbox_status_sentat ON outbox_event(status, sent_at);

-- Fast lookup for DEAD cleanup
CREATE INDEX idx_outbox_status_dlqat ON outbox_event(status, dlq_at);

-- Query patterns:
-- Polling:  WHERE status IN ('PENDING', 'RETRY') AND next_attempt_at <= NOW()
-- Cleanup:  WHERE status = 'SENT' AND sent_at < NOW() - INTERVAL '7 days'
-- Cleanup:  WHERE status = 'DEAD' AND dlq_at < NOW() - INTERVAL '30 days'
```

---

## Observability

### Metrics Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           OUTBOX METRICS DASHBOARD                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────────────┐  ┌──────────────────────────┐                 │
│  │   Events Pending (Gauge) │  │  Circuit Breaker State   │                 │
│  │        ████████ 42       │  │     ● CLOSED (0)        │                 │
│  │                          │  │     ○ HALF-OPEN (1)     │                 │
│  │                          │  │     ○ OPEN (2)          │                 │
│  └──────────────────────────┘  └──────────────────────────┘                 │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    Events Processed (Rate)                           │    │
│  │   150 ┤                                                              │    │
│  │       │     ╭─╮                                                      │    │
│  │   100 ┤    ╭╯ ╰╮     ╭╮                                              │    │
│  │       │   ╭╯   ╰─────╯╰──╮                                           │    │
│  │    50 ┤ ──╯               ╰────────────────────                      │    │
│  │       │                                                              │    │
│  │     0 ┼───────────────────────────────────────────▶ time            │    │
│  │       ── Sent    ── Retry    ── DLQ                                 │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
│  ┌──────────────────────────┐  ┌──────────────────────────┐                 │
│  │  Processing Duration     │  │  Dispatch Duration       │                 │
│  │  p50: 45ms  p99: 250ms  │  │  p50: 12ms  p99: 85ms   │                 │
│  │                          │  │                          │                 │
│  │  ▁▂▃▅▇▅▃▂▁              │  │  ▁▂▃▄▅▄▃▂▁              │                 │
│  └──────────────────────────┘  └──────────────────────────┘                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Alerting Rules (Recommended)

```yaml
# High pending events (backlog building)
- alert: OutboxBacklogHigh
  expr: outbox_events_pending_total > 1000
  for: 5m

# Circuit breaker open (Kafka issues)
- alert: CircuitBreakerOpen
  expr: circuit_breaker_state == 2
  for: 1m

# High DLQ rate (systemic failures)
- alert: HighDLQRate
  expr: rate(outbox_events_dlq_total[5m]) > 0.1
  for: 5m

# Processing latency high
- alert: HighProcessingLatency
  expr: histogram_quantile(0.99, poller_processing_duration_seconds) > 5
  for: 5m
```

---

## Deployment Architecture

### Docker Compose (Development)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Docker Network: bridge                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│   │    app       │    │   postgres   │    │    kafka     │      │
│   │  (outbox)    │───▶│   (15)       │    │   (KRaft)    │      │
│   │              │    │              │    │              │      │
│   │  Port: 8080  │    │  Port: 5432  │    │  Port: 9092  │      │
│   └──────────────┘    └──────────────┘    └──────────────┘      │
│          │                                       ▲               │
│          └───────────────────────────────────────┘               │
│                                                                  │
│   Dependencies:                                                  │
│   - app depends_on: postgres (healthy), kafka (healthy)          │
│   - Healthchecks ensure services ready before app starts         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Production Deployment (Kubernetes - Suggested)

```
┌─────────────────────────────────────────────────────────────────┐
│                      Kubernetes Cluster                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌────────────────────────────────────────────────────────┐    │
│   │                    Namespace: outbox                    │    │
│   │                                                         │    │
│   │   ┌─────────────┐    ┌─────────────┐                   │    │
│   │   │ Deployment  │    │   Service   │                   │    │
│   │   │  replicas:1 │◀───│  ClusterIP  │                   │    │
│   │   │             │    │  port: 8080 │                   │    │
│   │   └─────────────┘    └─────────────┘                   │    │
│   │          │                                              │    │
│   │          ▼                                              │    │
│   │   ┌─────────────┐    ┌─────────────┐                   │    │
│   │   │ ConfigMap   │    │   Secret    │                   │    │
│   │   │ config.yaml │    │  DB creds   │                   │    │
│   │   └─────────────┘    └─────────────┘                   │    │
│   │                                                         │    │
│   └────────────────────────────────────────────────────────┘    │
│                                                                  │
│   External:                                                      │
│   - PostgreSQL (RDS/CloudSQL)                                   │
│   - Kafka (MSK/Confluent Cloud)                                 │
│   - Prometheus (for scraping /metrics)                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

Note: Single replica recommended - multiple pollers would cause
duplicate processing without distributed locking implementation.
```

---

## Trade-offs & Alternatives

### Polling vs Change Data Capture (CDC)

| Aspect | Polling (This Implementation) | CDC (Debezium) |
|--------|-------------------------------|----------------|
| Latency | Higher (poll interval) | Near real-time |
| Complexity | Lower | Higher (more infra) |
| Throughput | ~1000 events/sec | ~10000+ events/sec |
| Infrastructure | Just app + DB | Kafka Connect, Debezium |
| Ordering | Batch order (configurable) | Transaction order |
| DB Load | Periodic queries | Minimal (log reading) |

**When to use Polling:**
- Moderate event volumes
- Simpler operational requirements
- Team familiar with application-level patterns

**When to use CDC:**
- High event volumes
- Strict ordering requirements
- Near real-time requirements

### At-Least-Once vs Exactly-Once

| Delivery Guarantee | Implementation | Trade-offs |
|-------------------|----------------|------------|
| At-Least-Once | This implementation | Simpler, requires idempotent consumers |
| Exactly-Once | Kafka transactions | Complex, performance overhead |

**This implementation choice:**
- At-least-once with idempotency (published_event table)
- Consumers must handle duplicates (recommended anyway)
- Simpler producer implementation

### Single Poller vs Distributed Polling

| Approach | Pros | Cons |
|----------|------|------|
| Single Poller | Simple, no coordination | Single point of failure |
| Distributed | HA, horizontal scale | Requires distributed locking |

**Current: Single Poller**
- Suitable for moderate volumes
- Add distributed locking (Redis/DB) for HA in production

---

## Future Enhancements

1. **Parallel Event Processing**: Use worker pool for concurrent dispatch
2. **Distributed Locking**: Enable multiple poller instances
3. **Partitioned Outbox**: Shard by aggregate_type for horizontal scale
4. **CDC Integration**: Optional Debezium connector for high-volume scenarios
5. **Admin API**: REST endpoints for event management and replay
6. **Grafana Dashboards**: Pre-built dashboards for the defined metrics
