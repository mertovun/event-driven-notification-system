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

// TestAuditChainTriggerAndVerify exercises the admin_audit hash chain
// end-to-end: insert several rows via the sqlc helper, then run
// VerifyAuditChain and assert broken_links is 0. This is the test that
// would have caught the migration 0012 type-mismatch (canonical fn
// declared a_id uuid, called with bigint) before it shipped. Without
// it, the integration suite passed because no test inserted into
// admin_audit, and the verifier on an empty table trivially returns 0.
func TestAuditChainTriggerAndVerify(t *testing.T) {
	dsn, cleanup := spinUpPostgres(t)
	defer cleanup()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	require.NoError(t, store.Migrate(dsn, logger))

	ctx := context.Background()
	pool, err := store.NewPool(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	q := gen.New(pool)

	// Genesis (no rows yet): VerifyAuditChain returns 0.
	broken, err := q.VerifyAuditChain(ctx)
	require.NoError(t, err, "VerifyAuditChain on empty table must succeed (this is the call that previously 500'd)")
	require.Equal(t, int64(0), broken, "empty chain must have zero broken links")

	// Insert a few rows; trigger computes prev_hash + row_hash. Any
	// signature mismatch between the trigger and admin_audit_canonical
	// produces SQLSTATE 42883 here (exactly the bug we're guarding).
	// actor_id is NULL on these rows so we don't need to seed api_keys.
	target1 := "dlq-target-1"
	for i := 0; i < 3; i++ {
		_, err := q.InsertAuditEntry(ctx, gen.InsertAuditEntryParams{
			Actor:    "qa-test",
			ActorID:  uuid.NullUUID{}, // NULL — FK is ON DELETE SET NULL, so NULL is a valid column value
			Action:   "dlq_replay",
			TargetID: &target1,
			Details:  []byte(`{"i":` + ptr1(i) + `}`),
		})
		require.NoError(t, err, "InsertAuditEntry must succeed; trigger fired without error")
	}

	// VerifyAuditChain must read 0 — every row's prev_hash links and
	// every row's row_hash recomputes correctly.
	broken, err = q.VerifyAuditChain(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), broken, "chain must be intact after honest inserts")

	// Tamper: mutate a row's details in place without recomputing
	// row_hash. The verifier must catch it.
	_, err = pool.Exec(ctx, `UPDATE admin_audit SET details = '{"tampered":true}'::jsonb WHERE id = 2`)
	require.NoError(t, err)

	broken, err = q.VerifyAuditChain(ctx)
	require.NoError(t, err)
	require.Greater(t, broken, int64(0), "post-tamper content edit must produce broken_links > 0")
}

// ptr1 stringifies an int for inline jsonb. Local helper to keep the
// test self-contained without pulling fmt.
func ptr1(i int) string {
	if i == 0 {
		return "0"
	}
	if i == 1 {
		return "1"
	}
	return "2"
}

// Avoid 'unused' on time import shrinkage.
var _ = time.Second
