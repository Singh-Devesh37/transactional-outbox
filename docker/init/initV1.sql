CREATE TABLE IF NOT EXISTS outbox_event (
    id SERIAL PRIMARY KEY,

    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,

    status TEXT  NOT NULL DEFAULT 'PENDING',
    retries INT NOT NULL DEFAULT 0,
    last_error TEXT,
    
    next_attempt_at TIMESTAMPTZ DEFAULT now(),
    sent_at TIMESTAMPTZ,
    dlq_at TIMESTAMPTZ,

    created TIMESTAMPTZ DEFAULT now(),
    updated TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_created ON outbox_event(status, created);

CREATE INDEX IF NOT EXISTS idx_outbox_status_sentat ON outbox_event (status, sent_at);

CREATE INDEX IF NOT EXISTS idx_outbox_status_dlqat ON outbox_event (status, dlq_at);

CREATE TABLE IF NOT EXISTS published_event (
    event_id BIGINT PRIMARY KEY,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
)