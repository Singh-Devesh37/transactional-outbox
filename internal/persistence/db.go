package persistence

import (
	"context"
	"fmt"
	"time"
	"transactional-outbox-go/internal/config"
	"transactional-outbox-go/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

func NewDBPool(cfg *config.Config) (*DB, error) {
	dbURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.DB.User,
		cfg.DB.Password,
		cfg.DB.Host,
		cfg.DB.Port,
		cfg.DB.DBName,
		cfg.DB.SSLMode,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)

	if err != nil {
		return nil, fmt.Errorf("unable to create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("unable to ping DB at URL %s: %w", dbURL, err)
	}

	logger.L().Info("Connected to Postgres at " + dbURL)

	return &DB{Pool: pool}, nil
}

func (db *DB) Close() {
	db.Pool.Close()
}
