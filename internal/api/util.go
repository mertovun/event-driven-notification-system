package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// contextWithTimeout is a tiny shim so callers can mock time.Now in tests later if needed.
func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

// WriteProblem writes an RFC 7807 problem details response. The full Problem
// struct and detailed mapping live in errors.go (step 2.2). This is the
// minimal sink used by middleware before that lands; keep it stable.
func WriteProblem(w http.ResponseWriter, _ *http.Request, status int, typ, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type":   typ,
		"title":  title,
		"status": status,
		"detail": detail,
	})
}
