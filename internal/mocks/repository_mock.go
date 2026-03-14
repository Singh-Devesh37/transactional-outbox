package mocks

import (
	"context"
	"sync"
	"time"

	"transactional-outbox-go/internal/model"
)

// MockRepository is a mock implementation of persistence.Repository for testing
type MockRepository struct {
	mu sync.Mutex

	// Function overrides for custom behavior
	InsertEventFunc           func(ctx context.Context, event model.OutboxEvent) error
	FetchPendingEventsFunc    func(ctx context.Context, limit int) ([]model.OutboxEvent, error)
	MarkEventSentFunc         func(ctx context.Context, id int64) error
	MarkEventRetryFunc        func(ctx context.Context, id int64, retries int, delay time.Duration, errMsg string) error
	MarkEventDeadFunc         func(ctx context.Context, id int64, errMsg string) error
	CheckAndMarkPublishedFunc func(ctx context.Context, eventID int64) (bool, error)
	DeleteOldSentEntriesFunc  func(ctx context.Context, days time.Time) (int64, error)
	DeleteOldDeadEntriesFunc  func(ctx context.Context, days time.Time) (int64, error)

	// Track calls for assertions
	InsertEventCalls           []model.OutboxEvent
	FetchPendingEventsCalls    []int
	MarkEventSentCalls         []int64
	MarkEventRetryCalls        []MarkEventRetryCall
	MarkEventDeadCalls         []MarkEventDeadCall
	CheckAndMarkPublishedCalls []int64
	DeleteOldSentEntriesCalls  []time.Time
	DeleteOldDeadEntriesCalls  []time.Time
}

// MarkEventRetryCall represents a call to MarkEventRetry
type MarkEventRetryCall struct {
	ID      int64
	Retries int
	Delay   time.Duration
	ErrMsg  string
}

// MarkEventDeadCall represents a call to MarkEventDead
type MarkEventDeadCall struct {
	ID     int64
	ErrMsg string
}

// NewMockRepository creates a new mock repository with default implementations
func NewMockRepository() *MockRepository {
	return &MockRepository{
		InsertEventFunc:           func(ctx context.Context, event model.OutboxEvent) error { return nil },
		FetchPendingEventsFunc:    func(ctx context.Context, limit int) ([]model.OutboxEvent, error) { return nil, nil },
		MarkEventSentFunc:         func(ctx context.Context, id int64) error { return nil },
		MarkEventRetryFunc:        func(ctx context.Context, id int64, retries int, delay time.Duration, errMsg string) error { return nil },
		MarkEventDeadFunc:         func(ctx context.Context, id int64, errMsg string) error { return nil },
		CheckAndMarkPublishedFunc: func(ctx context.Context, eventID int64) (bool, error) { return false, nil },
		DeleteOldSentEntriesFunc:  func(ctx context.Context, days time.Time) (int64, error) { return 0, nil },
		DeleteOldDeadEntriesFunc:  func(ctx context.Context, days time.Time) (int64, error) { return 0, nil },
	}
}

// InsertEvent implements persistence.Repository
func (m *MockRepository) InsertEvent(ctx context.Context, event model.OutboxEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InsertEventCalls = append(m.InsertEventCalls, event)
	if m.InsertEventFunc != nil {
		return m.InsertEventFunc(ctx, event)
	}
	return nil
}

// FetchPendingEvents implements persistence.Repository
func (m *MockRepository) FetchPendingEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FetchPendingEventsCalls = append(m.FetchPendingEventsCalls, limit)
	if m.FetchPendingEventsFunc != nil {
		return m.FetchPendingEventsFunc(ctx, limit)
	}
	return nil, nil
}

// MarkEventSent implements persistence.Repository
func (m *MockRepository) MarkEventSent(ctx context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MarkEventSentCalls = append(m.MarkEventSentCalls, id)
	if m.MarkEventSentFunc != nil {
		return m.MarkEventSentFunc(ctx, id)
	}
	return nil
}

// MarkEventRetry implements persistence.Repository
func (m *MockRepository) MarkEventRetry(ctx context.Context, id int64, retries int, delay time.Duration, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MarkEventRetryCalls = append(m.MarkEventRetryCalls, MarkEventRetryCall{
		ID:      id,
		Retries: retries,
		Delay:   delay,
		ErrMsg:  errMsg,
	})
	if m.MarkEventRetryFunc != nil {
		return m.MarkEventRetryFunc(ctx, id, retries, delay, errMsg)
	}
	return nil
}

// MarkEventDead implements persistence.Repository
func (m *MockRepository) MarkEventDead(ctx context.Context, id int64, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.MarkEventDeadCalls = append(m.MarkEventDeadCalls, MarkEventDeadCall{
		ID:     id,
		ErrMsg: errMsg,
	})
	if m.MarkEventDeadFunc != nil {
		return m.MarkEventDeadFunc(ctx, id, errMsg)
	}
	return nil
}

// CheckAndMarkPublished implements persistence.Repository
func (m *MockRepository) CheckAndMarkPublished(ctx context.Context, eventID int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CheckAndMarkPublishedCalls = append(m.CheckAndMarkPublishedCalls, eventID)
	if m.CheckAndMarkPublishedFunc != nil {
		return m.CheckAndMarkPublishedFunc(ctx, eventID)
	}
	return false, nil
}

// DeleteOldSentEntries implements persistence.Repository
func (m *MockRepository) DeleteOldSentEntries(ctx context.Context, days time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteOldSentEntriesCalls = append(m.DeleteOldSentEntriesCalls, days)
	if m.DeleteOldSentEntriesFunc != nil {
		return m.DeleteOldSentEntriesFunc(ctx, days)
	}
	return 0, nil
}

// DeleteOldDeadEntries implements persistence.Repository
func (m *MockRepository) DeleteOldDeadEntries(ctx context.Context, days time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DeleteOldDeadEntriesCalls = append(m.DeleteOldDeadEntriesCalls, days)
	if m.DeleteOldDeadEntriesFunc != nil {
		return m.DeleteOldDeadEntriesFunc(ctx, days)
	}
	return 0, nil
}

// Reset clears all recorded calls
func (m *MockRepository) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.InsertEventCalls = nil
	m.FetchPendingEventsCalls = nil
	m.MarkEventSentCalls = nil
	m.MarkEventRetryCalls = nil
	m.MarkEventDeadCalls = nil
	m.CheckAndMarkPublishedCalls = nil
	m.DeleteOldSentEntriesCalls = nil
	m.DeleteOldDeadEntriesCalls = nil
}

// Ensure MockRepository implements all required interfaces
// This will cause compile errors if the interface changes
var (
	_ interface {
		DeleteOldSentEntries(ctx context.Context, days time.Time) (int64, error)
		DeleteOldDeadEntries(ctx context.Context, days time.Time) (int64, error)
	} = (*MockRepository)(nil)
)
