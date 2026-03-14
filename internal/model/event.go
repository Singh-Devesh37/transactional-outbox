package model

import (
	"time"
)

type OutboxEvent struct {
	ID int64 `db:"id"`

	AggregateId   string `db:"aggregate_id"`
	AggregateType string `db:"aggregate_type"`
	EventType     string `db:"event_type"`
	Payload       []byte `db:"payload"`

	Status    string  `db:"status"`
	Retries   int     `db:"retries"`
	LastError *string `db:"last_error"`

	NextAttemptAt time.Time  `db:"next_attempt_at"`
	SentAt        *time.Time `db:"sent_at"`
	DlqAt         *time.Time `db:"dlq_at"`

	Created time.Time `db:"created"`
	Updated time.Time `db:"updated"`
}
