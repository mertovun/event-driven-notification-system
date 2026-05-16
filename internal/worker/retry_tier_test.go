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

// TestRetryTierFailureInjection proves the wait.5s TTL+DLX bounce actually
// works end-to-end. Scope reviewer Tier B Q5: 'The retry tier is unit-tested,
// not end-to-end tested. The brief's mandatory retry path is the most complex
// moving part and has no failure-injection integration test.'
//
// Setup: provider responds 503 (retryable) on the first call, 202 on the
// second. Expected behavior:
//  1. Worker calls provider → 503 → routes to wait.5s
//  2. ~5s later RabbitMQ TTL expires; DLX bounces back to main exchange
//  3. Worker picks up the redelivery → calls provider → 202 → delivered
//  4. Final notifications.status = 'sent', exactly 2 provider calls,
//     2 delivery_attempts rows (1 failed, 1 success).
//
// Timeout: 20s. wait.5s TTL is 5s; allow 3x cushion for cold-start +
// dispatcher cadence.
func TestRetryTierFailureInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Provider that fails first, succeeds on retry.
	var calls atomic.Int64
	mockProv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"injected 503"}`))
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"m","status":"accepted","timestamp":"t"}`))
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

	prov, err := provider.NewWithOptions(mockProv.URL, "test/0.1", provider.Options{AllowPrivateAddresses: true})
	require.NoError(t, err)

	dispID, _ := uuid.NewV7()
	dCfg := outbox.Default(dispID.String())
	dCfg.PollInterval = 100 * time.Millisecond
	disp := outbox.New(pool, q, pub, nil, logger, dCfg)

	// One worker is enough for the failure-injection scenario.
	mgr := worker.NewManager(amqpURL, pool, q, rdb, limiter, prov, pub, nil, nil, logger, worker.PoolSpec{
		SMSCount: 1,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = disp.Run(runCtx) }()
	go func() { _ = mgr.Run(runCtx) }()

	// Let consumers subscribe.
	time.Sleep(500 * time.Millisecond)

	notifID := uuid.New()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	qtx := q.WithTx(tx)
	_, err = qtx.InsertNotification(ctx, gen.InsertNotificationParams{
		ID: notifID, Channel: "sms", Recipient: "+905551234567",
		Content: "retry-tier", Priority: 5, Status: "pending", CorrelationID: "corr-retry",
	})
	require.NoError(t, err)
	payload, _ := json.Marshal(map[string]any{
		"notification_id": notifID.String(),
		"channel":         "sms",
		"recipient":       "+905551234567",
		"content":         "retry-tier",
		"priority":        "normal",
		"correlation_id":  "corr-retry",
	})
	_, err = qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
		NotificationID: notifID, RoutingKey: queue.RoutingKey("sms"),
		Payload: payload, Headers: []byte("{}"), Priority: 5,
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// First attempt should fail and route to wait.5s. Confirm via DB state.
	// We poll for ~3s expecting status to oscillate sending → queued.
	firstFailDeadline := time.Now().Add(4 * time.Second)
	sawFirstFailure := false
	for time.Now().Before(firstFailDeadline) {
		var status string
		var attemptCount int32
		err := pool.QueryRow(ctx,
			"SELECT status, attempt_count FROM notifications WHERE id = $1",
			notifID).Scan(&status, &attemptCount)
		require.NoError(t, err)
		// After the 503, the worker reverts to queued and routes to wait.5s.
		// status will go back to 'queued' with attempt_count incremented.
		if status == "queued" && attemptCount >= 1 {
			sawFirstFailure = true
			t.Logf("first attempt rejected; status=%s attempt_count=%d (delivered=%d)",
				status, attemptCount, calls.Load())
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.True(t, sawFirstFailure, "expected to observe status=queued + attempt_count++ after first 503")

	// Now wait for the wait.5s TTL bounce + second attempt succeeds.
	// 45s budget: wait.5s TTL (5s) + DLX hop (~1s on CI) + worker pickup
	// + provider call + DB commit. Local Docker is ~6s; CI's shared runner
	// adds slack. The assertion is "the bounce happens at all," not "fast."
	deadline := time.Now().Add(45 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for retry-tier bounce; calls=%d", calls.Load())
		}
		var status string
		err := pool.QueryRow(ctx, "SELECT status FROM notifications WHERE id = $1", notifID).Scan(&status)
		require.NoError(t, err)
		if status == "sent" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	require.Equal(t, int64(2), calls.Load(), "expected exactly 2 provider calls: 1 failed + 1 success")

	// Two delivery_attempts rows: one failed, one success.
	var failed, success int
	err = pool.QueryRow(ctx,
		"SELECT count(*) FILTER (WHERE success = false), count(*) FILTER (WHERE success = true) "+
			"FROM delivery_attempts WHERE notification_id = $1",
		notifID).Scan(&failed, &success)
	require.NoError(t, err)
	require.Equal(t, 1, failed, "expected exactly 1 failed delivery_attempt row")
	require.Equal(t, 1, success, "expected exactly 1 successful delivery_attempt row")

	t.Logf("retry-tier round-trip complete: 1 fail + ≥5s wait + 1 success → 'sent'")
	fmt.Println("OK")
}
