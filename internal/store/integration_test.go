//go:build integration

package store_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mertovun/event-driven-notification-system/internal/store"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// spinUpPostgres starts a containerized Postgres 16 and returns its DSN.
// Caller must Terminate the container.
func spinUpPostgres(t *testing.T) (string, func()) {
	t.Helper()
	ctx := context.Background()

	c, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("notif"),
		postgres.WithUsername("notif"),
		postgres.WithPassword("notif"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	return dsn, func() { _ = c.Terminate(ctx) }
}

func TestMigrateAndCRUD(t *testing.T) {
	dsn, cleanup := spinUpPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Run migrations.
	require.NoError(t, store.Migrate(dsn, logger))

	// Open pool.
	ctx := context.Background()
	pool, err := store.NewPool(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	q := gen.New(pool)

	// Insert a notification.
	nID := uuid.New()
	corrID := "corr-test-1"
	got, err := q.InsertNotification(ctx, gen.InsertNotificationParams{
		ID:              nID,
		BatchID:         uuid.NullUUID{},
		Channel:         "sms",
		Recipient:       "+905551234567",
		Content:         "test message",
		Priority:        5,
		Status:          "pending",
		IdempotencyKey:  ptr("idem-1"),
		ScheduledAt:     pgtype.Timestamptz{},
		CorrelationID:   corrID,
		TemplateID:      uuid.NullUUID{},
		TemplateVersion: nil,
	})
	require.NoError(t, err)
	require.Equal(t, nID, got.ID)
	require.Equal(t, "pending", got.Status)
	require.Equal(t, "sms", got.Channel)

	// Read it back.
	again, err := q.GetNotificationByID(ctx, nID)
	require.NoError(t, err)
	require.Equal(t, got.ID, again.ID)
	require.Equal(t, corrID, again.CorrelationID)

	// CAS transition: pending → queued via MarkQueued.
	queued, err := q.MarkQueued(ctx, nID)
	require.NoError(t, err)
	require.Equal(t, "queued", queued.Status)

	// CAS transition: queued → sending via MarkSendingCAS.
	sending, err := q.MarkSendingCAS(ctx, nID)
	require.NoError(t, err)
	require.Equal(t, "sending", sending.Status)

	// Idempotency: re-inserting the same key should fail with unique-violation.
	_, err = q.InsertNotification(ctx, gen.InsertNotificationParams{
		ID:             uuid.New(),
		Channel:        "sms",
		Recipient:      "+905557654321",
		Content:        "dup",
		Priority:       5,
		Status:         "pending",
		IdempotencyKey: ptr("idem-1"),
		CorrelationID:  "corr-test-2",
	})
	require.Error(t, err, "duplicate idempotency_key must violate the partial unique index")
}

func ptr(s string) *string { return &s }
