package observability

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestPIIRedact_Phone(t *testing.T) {
	t.Parallel()
	got := maskPhone("+905551234567")
	if !strings.HasPrefix(got, "+90") {
		t.Errorf("phone: %q lost prefix", got)
	}
	if !strings.HasSuffix(got, "67") {
		t.Errorf("phone: %q lost suffix", got)
	}
	if strings.Contains(got, "555123") {
		t.Errorf("phone: %q leaks middle digits", got)
	}
}

func TestPIIRedact_Email(t *testing.T) {
	t.Parallel()
	if got := maskEmail("alice@example.com"); got != "a***@example.com" {
		t.Errorf("email: %q", got)
	}
	if got := maskEmail("a@b.com"); got != "a***@b.com" {
		t.Errorf("short email: %q", got)
	}
	if got := maskEmail("garbage"); got != "[REDACTED]" {
		t.Errorf("no @: %q", got)
	}
}

func TestPIIRedact_Handler(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	h := WrapHandler(base)
	logger := slog.New(h)

	logger.LogAttrs(context.Background(), slog.LevelInfo, "test",
		slog.String("recipient", "+905551234567"),
		slog.String("email", "alice@example.com"),
		slog.String("content", "secret message here"),
		slog.String("api_key", "live-abc-123"),
		slog.String("channel", "sms"),
	)
	out := buf.String()
	if strings.Contains(out, "905551234567") {
		t.Errorf("phone leaked: %s", out)
	}
	if strings.Contains(out, "alice@example.com") {
		t.Errorf("email leaked: %s", out)
	}
	if strings.Contains(out, "secret message here") {
		t.Errorf("content leaked: %s", out)
	}
	if strings.Contains(out, "live-abc-123") {
		t.Errorf("api_key leaked: %s", out)
	}
	if !strings.Contains(out, `"channel":"sms"`) {
		t.Errorf("non-PII channel lost: %s", out)
	}
}
