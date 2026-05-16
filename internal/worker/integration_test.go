//go:build integration

package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/rabbitmq"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/mertovun/event-driven-notification-system/internal/outbox"
	"github.com/mertovun/event-driven-notification-system/internal/provider"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
	"github.com/mertovun/event-driven-notification-system/internal/store"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
	"github.com/mertovun/event-driven-notification-system/internal/worker"
)

func TestDeliveryEndToEnd(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// httptest.Server pretending to be webhook.site.
	var calls atomic.Int32
	mockProv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messageId": "mock-" + fmt.Sprint(calls.Load()),
			"status":    "accepted",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	}))
	defer mockProv.Close()

	// Postgres.
	pgc, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("notif"), postgres.WithUsername("notif"), postgres.WithPassword("notif"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(30*time.Second)),
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

	// Redis.
	rdc, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	defer func() { _ = rdc.Terminate(ctx) }()
	redisURL, err := rdc.ConnectionString(ctx)
	require.NoError(t, err)
	redisOpts, _ := redis.ParseURL(redisURL)
	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()
	limiter := ratelimit.New(rdb)

	// RabbitMQ.
	rmc, err := rabbitmq.Run(ctx, "rabbitmq:3.13-alpine")
	require.NoError(t, err)
	defer func() { _ = rmc.Terminate(ctx) }()
	amqpURL, err := rmc.AmqpURL(ctx)
	require.NoError(t, err)

	pub, err := queue.NewPublisher(ctx, amqpURL, logger)
	require.NoError(t, err)
	defer func() { _ = pub.Close() }()

	// Provider client pointed at the mock server.
	// Disable SSRF guard so httptest.Server's 127.0.0.1 URL works in tests.
	prov, err := provider.NewWithOptions(mockProv.URL, "test/0.1", provider.Options{AllowPrivateAddresses: true})
	require.NoError(t, err)

	// Dispatcher.
	dispID, _ := uuid.NewV7()
	dCfg := outbox.Default(dispID.String())
	dCfg.PollInterval = 100 * time.Millisecond
	disp := outbox.New(pool, q, pub, logger, dCfg)

	// Worker manager — 2 workers per channel for the test.
	mgr := worker.NewManager(amqpURL, pool, q, rdb, limiter, prov, nil, nil, logger, worker.PoolSpec{
		SMSCount: 2, EmailCount: 2, PushCount: 2,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = disp.Run(runCtx) }()
	go func() { _ = mgr.Run(runCtx) }()

	// Allow consumers to subscribe.
	time.Sleep(500 * time.Millisecond)

	// Insert notification + outbox row in one TX, just like the API does.
	notifID := uuid.New()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	qtx := q.WithTx(tx)
	_, err = qtx.InsertNotification(ctx, gen.InsertNotificationParams{
		ID: notifID, Channel: "sms", Recipient: "+905551234567",
		Content: "e2e", Priority: 5, Status: "pending", CorrelationID: "corr-1",
	})
	require.NoError(t, err)
	payload, _ := json.Marshal(map[string]any{
		"notification_id": notifID.String(),
		"channel":         "sms",
		"recipient":       "+905551234567",
		"content":         "e2e",
		"priority":        "normal",
		"correlation_id":  "corr-1",
	})
	_, err = qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
		NotificationID: notifID, RoutingKey: queue.RoutingKey("sms"),
		Payload: payload, Headers: []byte("{}"), Priority: 5,
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Poll until the row reaches 'sent'.
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for delivery; calls=%d", calls.Load())
		}
		var status string
		err := pool.QueryRow(ctx, "SELECT status FROM notifications WHERE id = $1", notifID).Scan(&status)
		require.NoError(t, err)
		if status == "sent" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, int32(1), calls.Load(), "provider should be called exactly once")

	// Verify delivery_attempts row.
	var attempts int
	err = pool.QueryRow(ctx, "SELECT count(*) FROM delivery_attempts WHERE notification_id = $1", notifID).Scan(&attempts)
	require.NoError(t, err)
	require.Equal(t, 1, attempts)
}
