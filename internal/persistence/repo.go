package persistence

import (
	"context"
	"fmt"
	"time"

	"transactional-outbox-go/internal/logger"
	"transactional-outbox-go/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type OutboxRepository struct {
	db *pgxpool.Pool
}

func NewOutboxRepository(db *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{db: db}
}

func (r *OutboxRepository) InsertEvent(ctx context.Context, event model.OutboxEvent) error {
	query :=
		`
		INSERT INTO outbox_event (aggregate_id, aggregate_type, event_type, payload, status, next_attempt_at, created, updated)
		VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
		`
	_, err := r.db.Exec(ctx, query, event.AggregateId, event.AggregateType, event.EventType, event.Payload, "PENDING")
	return err
}

func (r *OutboxRepository) FetchPendingEvents(ctx context.Context, limit int) ([]model.OutboxEvent, error) {
	query :=
		`
		SELECT id, aggregate_type, aggregate_id, event_type, payload, status, 
			retries, last_error, next_attempt_at, sent_at, dlq_at,
			created, updated
		FROM outbox_event
		WHERE status = 'PENDING'
			AND next_attempt_at <= NOW()
		ORDER BY created ASC
		LIMIT $1
		`

	rows, err := r.db.Query(ctx, query, limit)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var events []model.OutboxEvent
	for rows.Next() {
		var e model.OutboxEvent
		if err := rows.Scan(
			&e.ID, &e.AggregateType, &e.AggregateId, &e.EventType, &e.Payload, &e.Status,
			&e.Retries, &e.LastError, &e.NextAttemptAt, &e.SentAt, &e.DlqAt,
			&e.Created, &e.Updated,
		); err != nil {
			return nil, err
		}
		events = append(events, e)
	}

	return events, nil
}

func (r *OutboxRepository) MarkEventSent(ctx context.Context, id int64) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		logger.L().Error("Unable to begin mark event sent transaction", zap.Error(err))
		return err
	}

	defer tx.Rollback(ctx)

	cmd1, err := tx.Exec(ctx, `
		UPDATE outbox_event 
		SET status = 'SENT', updated = NOW(), sent_at = NOW()
		WHERE id = $1
		`, id)

	if err != nil {
		logger.L().Error("Unable to execute outbox event transaction", zap.Error(err))
		return err
	}

	if cmd1.RowsAffected() == 0 {
		logger.L().Debug("No Rows updated for outbox_event for SENT event ID = " + fmt.Sprint(id))
	}

	cmd2, err := tx.Exec(ctx, `
		INSERT INTO published_event(event_id) VALUES ($1)
		ON CONFLICT (event_id) DO NOTHING
		`, id)

	if err != nil {
		logger.L().Error("Unable to execute outbox event transaction", zap.Error(err))
		return err
	}

	if cmd2.RowsAffected() == 0 {
		logger.L().Debug("No Rows updated in published_event for SENT event ID = " + fmt.Sprint(id))
	}

	if err := tx.Commit(ctx); err != nil {
		logger.L().Error("Unable to commit mark event sent transaction", zap.Error(err))
		return err
	}

	return nil
}

func (r *OutboxRepository) MarkEventRetry(ctx context.Context, id int64, retries int, delay time.Duration, errMsg string) error {
	nextAttempt := time.Now().Add(delay)
	cmd, err := r.db.Exec(ctx, `
		UPDATE outbox_event 
		SET status = 'RETRY', retries = $1, last_error = $2, next_attempt_at = $3,  updated = NOW()
	  	WHERE id = $4
		`, retries, errMsg, nextAttempt, id)

	if err != nil {
		return err
	}

	if cmd.RowsAffected() == 0 {
		logger.L().Debug("No Rows updated for RETRY event ID = " + fmt.Sprint(id))
	}

	return nil
}

func (r *OutboxRepository) MarkEventDead(ctx context.Context, id int64, errMsg string) error {
	cmd, err := r.db.Exec(ctx, `
		UPDATE outbox_event
		SET status = 'DEAD', last_error = $1, dlq_at = NOW(), updated = NOW()
	  	WHERE id = $2
		`, errMsg, id)

	if err != nil {
		return fmt.Errorf("unable to mark dead event for = %d : %w", id, err)
	}

	if cmd.RowsAffected() == 0 {
		logger.L().Debug("No Rows updated for DEAD event ID = " + fmt.Sprint(id))
	}

	return nil
}

func (r *OutboxRepository) DeleteOldSentEntries(ctx context.Context, days time.Time) (int64, error) {

	tx, err := r.db.Begin(ctx)
	if err != nil {
		logger.L().Error("Unable to begin delete old sent entries transaction", zap.Error(err))
		return 0, fmt.Errorf("unable to begin delete old sent entries transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		DELETE FROM published_event 
		WHERE event_id IN (
			SELECT id FROM outbox_event 
			WHERE status = 'SENT' AND sent_at < $1
		)
	`, days)

	if err != nil {
		logger.L().Error("Unable to begin delete old sent entries transaction", zap.Error(err))
		return 0, fmt.Errorf("failed to delete old SENT entries: %w", err)
	}

	cmd2, err := tx.Exec(ctx, `
		DELETE FROM outbox_event
		WHERE status = 'SENT' AND sent_at < $1
		`, days)

	if err != nil {
		return 0, fmt.Errorf("failed to delete old SENT entries: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit delete old SENT entries: %w", err)
	}

	return cmd2.RowsAffected(), nil
}

func (r *OutboxRepository) CheckAndMarkPublished(ctx context.Context, eventID int64) (bool, error) {
	tx, err := r.db.Begin(ctx)

	if err != nil {
		return false, err
	}

	defer tx.Rollback(ctx)

	res, err := tx.Exec(ctx, `
		INSERT INTO published_event(event_id)
		VALUES ($1)
		ON CONFLICT DO NOTHING
	`, eventID)

	if err != nil {
		return false, err
	}

	alreadyPublished := res.RowsAffected() == 0

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}

	return alreadyPublished, nil
}

func (r *OutboxRepository) DeleteOldDeadEntries(ctx context.Context, days time.Time) (int64, error) {

	tx, err := r.db.Begin(ctx)
	if err != nil {
		logger.L().Error("Unable to begin delete old dead entries transaction", zap.Error(err))
		return 0, fmt.Errorf("unable to begin delete old dead entries transaction: %w", err)
	}

	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		DELETE FROM published_event 
		WHERE event_id IN (
			SELECT id FROM outbox_event 
			WHERE status = 'DEAD' AND dlq_at < $1
		)
	`, days)

	if err != nil {
		logger.L().Error("Unable to begin delete old dead entries transaction", zap.Error(err))
		return 0, fmt.Errorf("failed to delete old DEAD entries: %w", err)
	}

	cmd2, err := tx.Exec(ctx, `
		DELETE FROM outbox_event
		WHERE status = 'DEAD' AND dlq_at < $1
		`, days)

	if err != nil {
		return 0, fmt.Errorf("failed to delete old DEAD entries: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("failed to commit delete old DEAD entries: %w", err)
	}

	return cmd2.RowsAffected(), nil
}
