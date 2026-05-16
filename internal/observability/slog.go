package observability

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
)

// PIIRedactHandler wraps a base slog.Handler and masks attribute values whose
// keys match well-known PII fields.
//
// Masking rules:
//
//	recipient / phone — keep '+' + 2 leading + last 2: "+90******67"
//	email             — keep first char + domain: "m***@example.com"
//	content           — never the raw string; log length + sha256:first8
//	api_key / secret  — redact entirely
type PIIRedactHandler struct {
	base slog.Handler
}

// WrapHandler wraps any slog.Handler in PII redaction.
func WrapHandler(base slog.Handler) slog.Handler {
	if base == nil {
		base = slog.Default().Handler()
	}
	return &PIIRedactHandler{base: base}
}

func (h *PIIRedactHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.base.Enabled(ctx, lvl)
}

func (h *PIIRedactHandler) Handle(ctx context.Context, r slog.Record) error {
	rr := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		rr.AddAttrs(redact(a))
		return true
	})
	return h.base.Handle(ctx, rr)
}

func (h *PIIRedactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	red := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		red = append(red, redact(a))
	}
	return &PIIRedactHandler{base: h.base.WithAttrs(red)}
}

func (h *PIIRedactHandler) WithGroup(name string) slog.Handler {
	return &PIIRedactHandler{base: h.base.WithGroup(name)}
}

func redact(a slog.Attr) slog.Attr {
	switch strings.ToLower(a.Key) {
	case "recipient", "phone":
		return slog.String(a.Key, maskPhone(a.Value.String()))
	case "email":
		return slog.String(a.Key, maskEmail(a.Value.String()))
	case "content", "body":
		s := a.Value.String()
		return slog.String(a.Key, "len="+itoa(len(s))+" sha256:"+shortHash(s))
	case "api_key", "apikey", "secret", "password", "token":
		return slog.String(a.Key, "[REDACTED]")
	}
	return a
}

func maskPhone(s string) string {
	// E.164: +<countrycode><digits>. Keep '+', first 2 digits, last 2 digits.
	if len(s) < 6 {
		return "[REDACTED]"
	}
	prefix := s[:3] // '+' + 2 digits
	suffix := s[len(s)-2:]
	mid := strings.Repeat("*", max(1, len(s)-5))
	return prefix + mid + suffix
}

func maskEmail(s string) string {
	at := strings.IndexByte(s, '@')
	if at <= 0 {
		return "[REDACTED]"
	}
	local := s[:at]
	domain := s[at:]
	if len(local) <= 1 {
		return local + "***" + domain
	}
	return local[:1] + "***" + domain
}

func shortHash(s string) string {
	if s == "" {
		return "empty"
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:4])
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
