package persistence

import (
	"context"
	"time"

	"transactional-outbox-go/internal/model"
)

// Repository defines the interface for outbox event persistence operations
// This allows for easy mocking in tests
type Repository interface {
	InsertEvent(ctx context.Context, event model.OutboxEvent) error
	FetchPendingEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	MarkEventSent(ctx context.Context, id int64) error
	MarkEventRetry(ctx context.Context, id int64, retries int, delay time.Duration, errMsg string) error
	MarkEventDead(ctx context.Context, id int64, errMsg string) error
	CheckAndMarkPublished(ctx context.Context, eventID int64) (bool, error)
	DeleteOldSentEntries(ctx context.Context, days time.Time) (int64, error)
	DeleteOldDeadEntries(ctx context.Context, days time.Time) (int64, error)
}

// Ensure OutboxRepository implements Repository
var _ Repository = (*OutboxRepository)(nil)
