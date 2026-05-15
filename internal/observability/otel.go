package observability

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// OTelConfig holds the runtime knobs for OpenTelemetry tracing.
type OTelConfig struct {
	Endpoint   string  // OTLP gRPC target, e.g. "otel-collector:4317". Empty disables.
	SampleRate float64 // 0..1
	Disabled   bool    // SDK no-op when true
	Service    string  // service.name
	Version    string
	InstanceID string
	Env        string
}

// SetupTracing initializes the global OTel tracer provider + propagator.
// Returns a shutdown func; safe to call even when disabled.
func SetupTracing(ctx context.Context, cfg OTelConfig, logger *slog.Logger) (shutdown func(context.Context) error, tp trace.TracerProvider, err error) {
	// W3C propagator is always set so we can extract/inject even when SDK is off.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if cfg.Disabled || cfg.Endpoint == "" {
		logger.Info("opentelemetry disabled — no-op tracer provider")
		np := noop.NewTracerProvider()
		otel.SetTracerProvider(np)
		return func(context.Context) error { return nil }, np, nil
	}

	// Use our own resource attributes; skipping resource.Default() to avoid
	// schema-URL conflicts when the otel SDK's default schema bumps ahead of semconv.
	res := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.Service),
		semconv.ServiceVersion(cfg.Version),
		semconv.ServiceInstanceID(cfg.InstanceID),
		semconv.DeploymentEnvironmentName(cfg.Env),
		semconv.HostName(hostname()),
	)

	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	exporter, err := otlptracegrpc.New(dialCtx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		// Don't fail boot on collector unavailability — fall back to no-op and warn.
		logger.Warn("otel exporter connect failed; tracing disabled", "endpoint", cfg.Endpoint, "err", err)
		np := noop.NewTracerProvider()
		otel.SetTracerProvider(np)
		return func(context.Context) error { return nil }, np, nil
	}

	ratio := cfg.SampleRate
	if ratio <= 0 {
		ratio = 0.1
	}
	if ratio > 1 {
		ratio = 1
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
	)
	otel.SetTracerProvider(tracerProvider)
	logger.Info("opentelemetry tracing on", "endpoint", cfg.Endpoint, "sample_rate", ratio)

	return func(c context.Context) error {
		flushCtx, cancel := context.WithTimeout(c, 5*time.Second)
		defer cancel()
		var errs []error
		if err := tracerProvider.ForceFlush(flushCtx); err != nil {
			errs = append(errs, err)
		}
		if err := tracerProvider.Shutdown(flushCtx); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}, tracerProvider, nil
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return h
}
