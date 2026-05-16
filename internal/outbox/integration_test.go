//go:build integration

package outbox_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mertovun/event-driven-notification-system/internal/outbox"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/store"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

func TestOutboxEndToEnd(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// Postgres.
	pgc, err := postgres.Run(ctx,
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
	defer func() { _ = pgc.Terminate(ctx) }()

	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, store.Migrate(dsn, logger))

	pool, err := store.NewPool(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	q := gen.New(pool)

	// RabbitMQ.
	rmc, err := rabbitmq.Run(ctx, "rabbitmq:3.13-alpine")
	require.NoError(t, err)
	defer func() { _ = rmc.Terminate(ctx) }()

	amqpURL, err := rmc.AmqpURL(ctx)
	require.NoError(t, err)

	pub, err := queue.NewPublisher(ctx, amqpURL, logger)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	// Insert a notification + outbox row in the same transaction.
	notifID := uuid.New()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	qtx := q.WithTx(tx)
	_, err = qtx.InsertNotification(ctx, gen.InsertNotificationParams{
		ID:            notifID,
		Channel:       "sms",
		Recipient:     "+905551234567",
		Content:       "outbox e2e",
		Priority:      5,
		Status:        "pending",
		CorrelationID: "test-corr-1",
	})
	require.NoError(t, err)

	payload, _ := json.Marshal(map[string]any{
		"notification_id": notifID.String(),
		"channel":         "sms",
		"correlation_id":  "test-corr-1",
	})
	_, err = qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
		NotificationID: notifID,
		RoutingKey:     queue.RoutingKey("sms"),
		Payload:        payload,
		Headers:        []byte(`{}`),
		Priority:       5,
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Run the dispatcher in a goroutine; cancel after we observe success.
	dispID, _ := uuid.NewV7()
	cfg := outbox.Default(dispID.String())
	cfg.PollInterval = 100 * time.Millisecond
	disp := outbox.New(pool, q, pub, nil, logger, cfg)

	dispCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = disp.Run(dispCtx) }()

	// Poll until the outbox row's published_at is set and the notification is queued.
	deadline := time.Now().Add(8 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for outbox dispatch")
		}
		var publishedAt *time.Time
		err = pool.QueryRow(ctx,
			"SELECT published_at FROM outbox WHERE notification_id = $1",
			notifID,
		).Scan(&publishedAt)
		require.NoError(t, err)

		var status string
		err = pool.QueryRow(ctx,
			"SELECT status FROM notifications WHERE id = $1",
			notifID,
		).Scan(&status)
		require.NoError(t, err)

		if publishedAt != nil && status == "queued" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
