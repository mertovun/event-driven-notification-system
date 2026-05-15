package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

// Problem implements RFC 7807. The struct fields use exact RFC 7807 names so JSON
// encoding produces the spec body. Extension members live in the embedded map.
type Problem struct {
	Type     string         `json:"type"`             // URI reference identifying the problem type
	Title    string         `json:"title"`            // short human-readable summary
	Status   int            `json:"status"`           // HTTP status code
	Detail   string         `json:"detail,omitempty"` // human-readable explanation
	Instance string         `json:"instance,omitempty"`
	Extra    map[string]any `json:"-"` // marshaled into the top-level object via custom MarshalJSON
}

// MarshalJSON flattens Extra into the top-level JSON object alongside the standard fields,
// matching how RFC 7807 extension members are presented on the wire.
func (p Problem) MarshalJSON() ([]byte, error) {
	out := map[string]any{
		"type":   p.Type,
		"title":  p.Title,
		"status": p.Status,
	}
	if p.Detail != "" {
		out["detail"] = p.Detail
	}
	if p.Instance != "" {
		out["instance"] = p.Instance
	}
	for k, v := range p.Extra {
		out[k] = v
	}
	return json.Marshal(out)
}

// WriteProblem now produces the full RFC 7807 body. Replaces the placeholder
// in util.go from step 2.1.
func WriteProblemFull(w http.ResponseWriter, r *http.Request, p Problem) {
	if p.Instance == "" {
		if cid := CorrelationIDFrom(r.Context()); cid != "" {
			p.Instance = "/requests/" + cid
		}
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// FieldError captures one validation failure for the `errors[]` extension member.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// WriteValidationProblem emits 400 Bad Request with the `errors[]` extension member
// detailing the per-field violations.
func WriteValidationProblem(w http.ResponseWriter, r *http.Request, fieldErrs []FieldError) {
	WriteProblemFull(w, r, Problem{
		Type:   "/problems/validation",
		Title:  "Validation Failed",
		Status: http.StatusBadRequest,
		Detail: "one or more fields failed validation",
		Extra:  map[string]any{"errors": fieldErrs},
	})
}

// WriteErrorAsProblem is the single error→HTTP boundary. Every handler funnels
// errors through this — never write status codes directly.
// Maps known sentinels to specific Problem shapes; falls back to 500 for unknown.
func WriteErrorAsProblem(w http.ResponseWriter, r *http.Request, err error) {
	if err == nil {
		// Defensive: a nil error reaching the mapper is itself a bug.
		WriteProblemFull(w, r, Problem{
			Type: "about:blank", Title: "Internal Server Error",
			Status: http.StatusInternalServerError, Detail: "nil error reached mapper",
		})
		return
	}

	switch {
	case errors.Is(err, notification.ErrNotFound),
		errors.Is(err, notification.ErrTemplateNotFound):
		WriteProblemFull(w, r, Problem{
			Type:   "/problems/not-found",
			Title:  "Not Found",
			Status: http.StatusNotFound,
			Detail: err.Error(),
		})

	case errors.Is(err, notification.ErrIdempotencyConflict):
		WriteProblemFull(w, r, Problem{
			Type:   "/problems/idempotency-conflict",
			Title:  "Idempotency-Key Conflict",
			Status: http.StatusConflict,
			Detail: "Idempotency-Key reused with a different request body",
		})

	case errors.Is(err, notification.ErrIdempotencyInFlight):
		p := Problem{
			Type:   "/problems/idempotency-in-flight",
			Title:  "Idempotent Request In Flight",
			Status: http.StatusConflict,
			Detail: "an earlier request with this Idempotency-Key is still being processed",
		}
		w.Header().Set("Retry-After", "1")
		WriteProblemFull(w, r, p)

	case errors.Is(err, notification.ErrInvalidState):
		WriteProblemFull(w, r, Problem{
			Type:   "/problems/invalid-state",
			Title:  "Invalid State Transition",
			Status: http.StatusConflict,
			Detail: err.Error(),
		})

	case errors.Is(err, notification.ErrValidation),
		errors.Is(err, notification.ErrUnknownChannel),
		errors.Is(err, notification.ErrTemplateRender):
		WriteProblemFull(w, r, Problem{
			Type:   "/problems/validation",
			Title:  "Validation Failed",
			Status: http.StatusBadRequest,
			Detail: err.Error(),
		})

	case errors.Is(err, notification.ErrRateLimited):
		w.Header().Set("Retry-After", "1")
		WriteProblemFull(w, r, Problem{
			Type:   "/problems/rate-limited",
			Title:  "Too Many Requests",
			Status: http.StatusTooManyRequests,
			Detail: err.Error(),
		})

	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		WriteProblemFull(w, r, Problem{
			Type:   "/problems/timeout",
			Title:  "Request Timeout",
			Status: http.StatusGatewayTimeout,
			Detail: err.Error(),
		})

	default:
		// Unknown — log and emit 500 without leaking the error string.
		logger := LoggerFrom(r.Context())
		logger.ErrorContext(r.Context(), "unmapped error in handler",
			"err", err, "errf", fmt.Sprintf("%+v", err))
		WriteProblemFull(w, r, Problem{
			Type:   "about:blank",
			Title:  "Internal Server Error",
			Status: http.StatusInternalServerError,
			Detail: "an unexpected error occurred",
		})
	}
}
