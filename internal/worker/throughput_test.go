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

// TestWorkerStageThroughput proves the worker stage actually sustains the
// brief-mandated 100 msg/s/channel rate. The k6 PERFORMANCE.md numbers
// measure POST ingest, not delivery — the panel review specifically flagged
// that gap. This test injects N notifications directly into the outbox
// (skipping the API layer) and asserts the dispatcher + workers drain them
// within the budget implied by the Lua bucket config.
//
// Configuration:
//
//	targetRate    = 100 msg/s/channel (matches Pipeline.rateLimitPerSec)
//	N             = 500 messages on the sms channel
//	minThroughput = 95 msg/s (allow 5% slack for cold-start + container noise)
//	timeout       = 8s (500 / 95 ≈ 5.3s; 2.5s headroom for ramp)
func TestWorkerStageThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Fast 200 OK provider stub.
	var calls atomic.Int64
	mockProv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
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

	// Redis + rate limiter.
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
	dCfg.PollInterval = 50 * time.Millisecond // tight for the throughput test
	dCfg.BatchSize = 100
	disp := outbox.New(pool, q, pub, nil, logger, dCfg)

	// 8 SMS workers — matches the production default (cfg.WorkerCountSMS).
	mgr := worker.NewManager(amqpURL, pool, q, rdb, limiter, prov, pub, nil, nil, logger, worker.PoolSpec{
		SMSCount: 8,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = disp.Run(runCtx) }()
	go func() { _ = mgr.Run(runCtx) }()

	// Let consumers subscribe before we inject load.
	time.Sleep(750 * time.Millisecond)

	// Inject N notifications + outbox rows in a few batches so the API path's
	// TX cost doesn't dominate the timing.
	const N = 500
	ids := make([]uuid.UUID, 0, N)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	qtx := q.WithTx(tx)
	for i := 0; i < N; i++ {
		id := uuid.New()
		ids = append(ids, id)
		_, err := qtx.InsertNotification(ctx, gen.InsertNotificationParams{
			ID:            id,
			Channel:       "sms",
			Recipient:     fmt.Sprintf("+90555%07d", i),
			Content:       fmt.Sprintf("msg-%d", i),
			Priority:      5,
			Status:        "pending",
			CorrelationID: fmt.Sprintf("corr-%d", i),
		})
		require.NoError(t, err)
		payload, _ := json.Marshal(map[string]any{
			"notification_id": id.String(),
			"channel":         "sms",
			"recipient":       fmt.Sprintf("+90555%07d", i),
			"content":         fmt.Sprintf("msg-%d", i),
			"priority":        "normal",
			"correlation_id":  fmt.Sprintf("corr-%d", i),
		})
		_, err = qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
			NotificationID: id,
			RoutingKey:     queue.RoutingKey("sms"),
			Payload:        payload,
			Headers:        []byte("{}"),
			Priority:       5,
		})
		require.NoError(t, err)
	}
	injectStart := time.Now()
	require.NoError(t, tx.Commit(ctx))
	t.Logf("injected %d notifications in %s", N, time.Since(injectStart))

	// Wait for everything to deliver. Cap at 8s — 95 msg/s on 500 messages
	// would be 5.3s, plus container ramp; anything beyond suggests the
	// 100/s/channel guarantee doesn't hold under steady-state load.
	deadline := time.Now().Add(8 * time.Second)
	t0 := time.Now()
	var elapsed time.Duration
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for delivery; delivered=%d / target=%d after %s", calls.Load(), N, time.Since(t0))
		}
		// Count sent rows.
		var sent int
		err := pool.QueryRow(ctx, "SELECT count(*) FROM notifications WHERE status = 'sent'").Scan(&sent)
		require.NoError(t, err)
		if sent >= N {
			elapsed = time.Since(t0)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 100 msg/s configured, but two factors slow us down in this test:
	//   1. Rate limiter is Redis-backed → adds ~ms of latency per call
	//   2. Cold-start: workers + dispatcher take ~250ms to start draining
	// We assert ≥95 msg/s and log the actual rate.
	rate := float64(N) / elapsed.Seconds()
	t.Logf("worker stage delivered %d messages in %s (%.1f msg/s)", N, elapsed, rate)
	if rate < 95 {
		t.Fatalf("worker stage throughput below 95 msg/s/channel SLO: got %.1f msg/s", rate)
	}
	require.Equal(t, int64(N), calls.Load(), "provider should be called exactly once per notification")
}
