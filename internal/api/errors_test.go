package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

// TestWriteErrorAsProblem covers the sentinel→status mapping.
// Every known sentinel is exercised plus an unmapped error.
func TestWriteErrorAsProblem(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		err      error
		want     int
		wantType string
	}{
		{"not_found", notification.ErrNotFound, http.StatusNotFound, "/problems/not-found"},
		{"template_not_found", notification.ErrTemplateNotFound, http.StatusNotFound, "/problems/not-found"},
		{"idem_conflict", notification.ErrIdempotencyConflict, http.StatusConflict, "/problems/idempotency-conflict"},
		{"idem_in_flight", notification.ErrIdempotencyInFlight, http.StatusConflict, "/problems/idempotency-in-flight"},
		{"invalid_state", notification.ErrInvalidState, http.StatusConflict, "/problems/invalid-state"},
		{"validation", notification.ErrValidation, http.StatusBadRequest, "/problems/validation"},
		{"unknown_channel", notification.ErrUnknownChannel, http.StatusBadRequest, "/problems/validation"},
		{"template_render", notification.ErrTemplateRender, http.StatusBadRequest, "/problems/validation"},
		{"rate_limited", notification.ErrRateLimited, http.StatusTooManyRequests, "/problems/rate-limited"},
		{"context_deadline", context.DeadlineExceeded, http.StatusGatewayTimeout, "/problems/timeout"},
		{"context_canceled", context.Canceled, http.StatusGatewayTimeout, "/problems/timeout"},
		{"unknown", errors.New("some unknown failure"), http.StatusInternalServerError, "about:blank"},
		{"wrapped_known", fmt.Errorf("wrap: %w", notification.ErrNotFound), http.StatusNotFound, "/problems/not-found"},
		{"nil", nil, http.StatusInternalServerError, "about:blank"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			WriteErrorAsProblem(w, r, tc.err)

			if w.Code != tc.want {
				t.Fatalf("status: got %d, want %d (err=%v)", w.Code, tc.want, tc.err)
			}
			if ct := w.Header().Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("content-type: got %q, want application/problem+json", ct)
			}

			var p map[string]any
			if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
				t.Fatalf("body unmarshal: %v", err)
			}
			if got := p["type"]; got != tc.wantType {
				t.Errorf("type: got %q, want %q", got, tc.wantType)
			}
			if got := p["status"]; got != float64(tc.want) { // JSON numbers decode as float64
				t.Errorf("status field: got %v, want %d", got, tc.want)
			}
		})
	}
}

func TestProblemMarshalJSONExtras(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteValidationProblem(w, r, []FieldError{
		{Field: "recipient", Message: "must be E.164"},
		{Field: "content", Message: "must not be empty"},
	})
	var p map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	errsList, ok := p["errors"].([]any)
	if !ok {
		t.Fatalf("expected errors[] array, got %T (%v)", p["errors"], p["errors"])
	}
	if len(errsList) != 2 {
		t.Fatalf("errors length: got %d, want 2", len(errsList))
	}
}

func TestRateLimitedAddsRetryAfter(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	WriteErrorAsProblem(w, r, notification.ErrRateLimited)
	if w.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on rate-limited response")
	}
}
