// Package config loads the Config struct from environment variables.
package config

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// Config holds the full runtime configuration. Loaded once at startup from env.
type Config struct {
	// Process
	Mode     string `envconfig:"MODE" default:"all"`
	HTTPAddr string `envconfig:"HTTP_ADDR" default:":8080"`
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"`
	// Env gates production-unsafe behaviour: when != "development", the
	// startup refuses to seed the dev API key and fails loudly. ADR-0013
	// promised this check; the panel review noted it was missing.
	Env string `envconfig:"APP_ENV" default:"development"`

	// Postgres
	DatabaseURL string `envconfig:"DATABASE_URL" required:"true"`

	// Redis
	RedisURL string `envconfig:"REDIS_URL" required:"true"`

	// RabbitMQ
	AMQPURL string `envconfig:"AMQP_URL" required:"true"`

	// External provider
	WebhookURL string `envconfig:"WEBHOOK_URL" required:"true"`

	// Worker pool sizes (per channel)
	WorkerCountSMS   int `envconfig:"WORKER_COUNT_SMS" default:"8"`
	WorkerCountEmail int `envconfig:"WORKER_COUNT_EMAIL" default:"8"`
	WorkerCountPush  int `envconfig:"WORKER_COUNT_PUSH" default:"8"`

	// Outbox dispatcher
	OutboxPollInterval time.Duration `envconfig:"OUTBOX_POLL_INTERVAL" default:"250ms"`
	OutboxBatchSize    int           `envconfig:"OUTBOX_BATCH_SIZE" default:"50"`

	// OpenTelemetry
	OTELEndpoint    string  `envconfig:"OTEL_EXPORTER_OTLP_ENDPOINT" default:""`
	OTELSampleRatio float64 `envconfig:"OTEL_SAMPLE_RATIO" default:"0.1"`
	OTELDisabled    bool    `envconfig:"OTEL_SDK_DISABLED" default:"false"`

	// API key for development seed (overridden in production)
	DevAPIKey string `envconfig:"DEV_API_KEY" default:""`

	// Profiling. When true, the HTTP server mounts /debug/pprof/*. Default off in prod.
	PProfEnabled bool `envconfig:"PPROF_ENABLED" default:"false"`

	// Scheduler feature flag. Off by default because the scheduled-notification
	// path triggers the pgx v5 bgreader wedge documented in KNOWN_ISSUES.md.
	// Operators who accept the risk (or test against a different driver
	// version) enable it explicitly. ADR-0027 captures the decision.
	SchedulerEnabled bool `envconfig:"SCHEDULER_ENABLED" default:"false"`
}

// Load reads the Config from the environment.
// It returns an error if a required value is missing or a value is malformed.
func Load() (Config, error) {
	var c Config
	if err := envconfig.Process("", &c); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return c, nil
}
