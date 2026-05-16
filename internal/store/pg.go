package store

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPool builds a pgx pool with tuned defaults (MaxConns=100, statement_timeout=5s).
// The caller owns the returned pool and must Close() on shutdown.
func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse db url: %w", err)
	}

	cfg.MaxConns = 100
	cfg.MinConns = 4
	cfg.MaxConnLifetime = time.Hour
	cfg.MaxConnIdleTime = 30 * time.Minute
	cfg.HealthCheckPeriod = time.Minute

	// Per-connection statement_timeout — bounds worst-case query duration so a
	// wedged client read fails fast (Postgres aborts after 5s and the wire
	// surfaces an error). See README "Known issues" for the scheduled-flow hang.
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"

	// OpenTelemetry tracer on pg queries. Parameterized statement only (no values).
	cfg.ConnConfig.Tracer = otelpgx.NewTracer(otelpgx.WithTrimSQLInSpanName())

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pool open: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pool ping: %w", err)
	}

	return pool, nil
}
