package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertovun/event-driven-notification-system/internal/idempotency"
	"github.com/mertovun/event-driven-notification-system/internal/notification"
	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
	tmpl "github.com/mertovun/event-driven-notification-system/internal/template"
)

// notificationCreateRequest mirrors the JSON request body for POST /v1/notifications.
// Two shapes are valid: raw `content`, OR `template_id` + `variables`. See docs/01 §4.1.
type notificationCreateRequest struct {
	Channel     notification.Channel  `json:"channel"`
	Recipient   string                `json:"recipient"`
	Content     string                `json:"content,omitempty"`
	TemplateID  *uuid.UUID            `json:"template_id,omitempty"`
	Variables   map[string]any        `json:"variables,omitempty"`
	Priority    notification.Priority `json:"priority"`
	ScheduledAt *time.Time            `json:"scheduled_at,omitempty"`
}

// notificationResponse is the canonical response body for create + get.
type notificationResponse struct {
	ID              uuid.UUID             `json:"id"`
	BatchID         *uuid.UUID            `json:"batch_id"`
	Channel         notification.Channel  `json:"channel"`
	Recipient       string                `json:"recipient"`
	Content         string                `json:"content"`
	TemplateID      *uuid.UUID            `json:"template_id,omitempty"`
	TemplateVersion *int32                `json:"template_version,omitempty"`
	Priority        notification.Priority `json:"priority"`
	Status          notification.Status   `json:"status"`
	AttemptCount    int32                 `json:"attempt_count"`
	LastError       *string               `json:"last_error,omitempty"`
	ScheduledAt     *time.Time            `json:"scheduled_at,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	SentAt          *time.Time            `json:"sent_at,omitempty"`
	CorrelationID   string                `json:"correlation_id"`
}

// notificationsHandler holds the deps the resource handlers need.
type notificationsHandler struct {
	pool    *pgxpool.Pool
	q       *gen.Queries
	idem    *idempotency.Store
	metrics *observability.Metrics
}

func newNotificationsHandler(d Deps, idem *idempotency.Store) *notificationsHandler {
	return &notificationsHandler{pool: d.Pool, q: d.Queries, idem: idem, metrics: d.Metrics}
}

// create handles POST /v1/notifications (single).
//
//  1. Decode + validate.
//  2. If Idempotency-Key present: BeginOrReplay → on replay, return cached body verbatim.
//  3. Open a Postgres TX: insert notifications, insert outbox row, commit.
//  4. Finalize idempotency with the canonical response.
//  5. Return 201.
func (h *notificationsHandler) create(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytesDefault))
	dec.DisallowUnknownFields()

	var req notificationCreateRequest
	if err := dec.Decode(&req); err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: err.Error()}})
		return
	}
	// Default priority before validating so empty → normal.
	if req.Priority == "" {
		req.Priority = notification.PriorityNormal
	}

	if fe := validateCreate(req); len(fe) > 0 {
		WriteValidationProblem(w, r, fe)
		return
	}

	// Shape B: resolve template + render at create time. See docs/12 §3.
	// We mutate req.Content in place so the rest of the handler treats this as Shape A.
	var templateVersion *int32
	if req.TemplateID != nil {
		row, err := h.q.GetTemplateByID(r.Context(), *req.TemplateID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
				return
			}
			WriteErrorAsProblem(w, r, fmt.Errorf("get template: %w", err))
			return
		}
		if row.DeprecatedAt.Valid {
			WriteValidationProblem(w, r, []FieldError{{Field: "template_id", Message: "template is deprecated"}})
			return
		}
		if missing := tmpl.CheckRequired(row.RequiredVars, req.Variables); len(missing) > 0 {
			fe := make([]FieldError, 0, len(missing))
			for _, m := range missing {
				fe = append(fe, FieldError{Field: "variables." + m, Message: "missing required variable"})
			}
			WriteValidationProblem(w, r, fe)
			return
		}
		rendered, err := tmpl.Render(row.Body, req.Variables)
		if err != nil {
			WriteValidationProblem(w, r, []FieldError{{Field: "variables", Message: err.Error()}})
			return
		}
		// Channel-specific length check on rendered content.
		if fe := validateContent(req.Channel, rendered); len(fe) > 0 {
			WriteValidationProblem(w, r, fe)
			return
		}
		req.Content = rendered
		templateVersion = &row.Version
	}

	corrID := CorrelationIDFrom(r.Context())
	idemKey := r.Header.Get("Idempotency-Key")

	// Idempotency claim (if a key was supplied).
	var idemBodyHash string
	if idemKey != "" {
		hash, err := idempotency.CanonicalHash(req)
		if err != nil {
			WriteErrorAsProblem(w, r, fmt.Errorf("hash request body: %w", err))
			return
		}
		idemBodyHash = hash

		replay, err := h.idem.BeginOrReplay(r.Context(), idemKey, idemBodyHash)
		switch {
		case errors.Is(err, idempotency.ErrConflict):
			WriteErrorAsProblem(w, r, notification.ErrIdempotencyConflict)
			return
		case errors.Is(err, idempotency.ErrInFlight):
			WriteErrorAsProblem(w, r, notification.ErrIdempotencyInFlight)
			return
		case err != nil:
			WriteErrorAsProblem(w, r, fmt.Errorf("idempotency: %w", err))
			return
		}
		if replay != nil {
			// Replay: emit the stored response verbatim.
			if h.metrics != nil {
				h.metrics.IdempotencyReplayTotal.WithLabelValues("POST /v1/notifications").Inc()
			}
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Idempotency-Replayed", "true")
			w.WriteHeader(replay.StatusCode)
			_, _ = w.Write(replay.Body)
			return
		}
		// We own the in-flight slot. If anything below fails, release it so retries can proceed.
		defer func() {
			if r.Context().Err() != nil { // best-effort release on cancel
				_ = h.idem.Release(context.Background(), idemKey)
			}
		}()
	}

	resp, status, err := h.persist(r.Context(), req, idemKey, corrID, templateVersion)
	if err != nil {
		if idemKey != "" {
			_ = h.idem.Release(r.Context(), idemKey)
		}
		WriteErrorAsProblem(w, r, err)
		return
	}

	body, mErr := json.Marshal(resp)
	if mErr != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("marshal response: %w", mErr))
		return
	}

	// Finalize idempotency before responding so a fast retry sees the canonical body.
	if idemKey != "" {
		_ = h.idem.Finalize(r.Context(), idemKey, idempotency.Record{
			BodyHash:   idemBodyHash,
			StatusCode: status,
			Body:       body,
		})
	}

	if h.metrics != nil {
		h.metrics.NotificationsCreatedTotal.WithLabelValues(string(req.Channel), string(req.Priority)).Inc()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
	_, _ = w.Write([]byte("\n"))
}

// persist runs the durable side of the create: notification row + outbox row in one TX.
// Returns the canonical response and status (201 normally; 202 when scheduled).
func (h *notificationsHandler) persist(
	ctx context.Context,
	req notificationCreateRequest,
	idemKey, corrID string,
	templateVersion *int32,
) (notificationResponse, int, error) {
	id := uuid.Must(uuid.NewV7())
	priorityInt := req.Priority.Int16()
	status := notification.StatusPending
	var schedAt pgtype.Timestamptz
	if req.ScheduledAt != nil && req.ScheduledAt.After(time.Now()) {
		status = notification.StatusScheduled
		schedAt = pgtype.Timestamptz{Time: *req.ScheduledAt, Valid: true}
	}

	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return notificationResponse{}, 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	qtx := h.q.WithTx(tx)

	var tplID uuid.NullUUID
	if req.TemplateID != nil {
		tplID = uuid.NullUUID{UUID: *req.TemplateID, Valid: true}
	}

	row, err := qtx.InsertNotification(ctx, gen.InsertNotificationParams{
		ID:              id,
		BatchID:         uuid.NullUUID{},
		Channel:         string(req.Channel),
		Recipient:       req.Recipient,
		Content:         req.Content,
		Priority:        priorityInt,
		Status:          string(status),
		IdempotencyKey:  nullableString(idemKey),
		ScheduledAt:     schedAt,
		CorrelationID:   corrID,
		TemplateID:      tplID,
		TemplateVersion: templateVersion,
	})
	if err != nil {
		return notificationResponse{}, 0, fmt.Errorf("insert notification: %w", err)
	}

	// If pending (not scheduled): write the outbox row in the same TX.
	// Scheduled notifications go through the scheduler dispatcher → outbox later.
	if status == notification.StatusPending {
		envelope := map[string]any{
			"notification_id": id.String(),
			"channel":         req.Channel,
			"recipient":       req.Recipient,
			"content":         req.Content,
			"priority":        req.Priority,
			"correlation_id":  corrID,
		}
		payload, _ := json.Marshal(envelope)

		headers := map[string]any{
			"x-correlation-id": corrID,
		}
		if idemKey != "" {
			headers["x-idempotency-key"] = idemKey
		}
		headersJSON, _ := json.Marshal(headers)

		if _, err := qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
			NotificationID: id,
			RoutingKey:     "notification." + string(req.Channel),
			Payload:        payload,
			Headers:        headersJSON,
			Priority:       priorityInt,
		}); err != nil {
			return notificationResponse{}, 0, fmt.Errorf("insert outbox: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return notificationResponse{}, 0, fmt.Errorf("commit: %w", err)
	}

	return rowToResponse(row), http.StatusCreated, nil
}

// validateCreate runs per-field checks. Enforces the oneOf:
// exactly one of (content) OR (template_id + variables) must be supplied.
// Channel-specific content length validation runs on the *rendered* content
// (which is the raw content if shape A, or the rendered template if shape B —
// caller has rendered before calling this when shape B is in use).
func validateCreate(req notificationCreateRequest) []FieldError {
	var out []FieldError
	if !req.Channel.Valid() {
		out = append(out, FieldError{Field: "channel", Message: "must be sms | email | push"})
		return out
	}
	out = append(out, validateRecipient(req.Channel, req.Recipient)...)

	switch {
	case req.Content != "" && req.TemplateID != nil:
		out = append(out, FieldError{Field: "content", Message: "must not be set when template_id is provided"})
	case req.Content == "" && req.TemplateID == nil:
		out = append(out, FieldError{Field: "content", Message: "must be set when template_id is not provided"})
	case req.Content != "":
		out = append(out, validateContent(req.Channel, req.Content)...)
	}

	if !req.Priority.Valid() {
		out = append(out, FieldError{Field: "priority", Message: "must be high | normal | low"})
	}
	if req.ScheduledAt != nil {
		max := time.Now().Add(30 * 24 * time.Hour)
		if req.ScheduledAt.After(max) {
			out = append(out, FieldError{Field: "scheduled_at", Message: "must be ≤ 30 days in the future"})
		}
	}
	return out
}

func rowToResponse(row gen.Notification) notificationResponse {
	resp := notificationResponse{
		ID:            row.ID,
		Channel:       notification.Channel(row.Channel),
		Recipient:     row.Recipient,
		Content:       row.Content,
		Priority:      notification.PriorityFromInt(row.Priority),
		Status:        notification.Status(row.Status),
		AttemptCount:  row.AttemptCount,
		CreatedAt:     row.CreatedAt.Time,
		UpdatedAt:     row.UpdatedAt.Time,
		CorrelationID: row.CorrelationID,
	}
	if row.BatchID.Valid {
		b := row.BatchID.UUID
		resp.BatchID = &b
	}
	if row.TemplateID.Valid {
		t := row.TemplateID.UUID
		resp.TemplateID = &t
	}
	if row.TemplateVersion != nil {
		v := *row.TemplateVersion
		resp.TemplateVersion = &v
	}
	if row.LastError != nil {
		le := *row.LastError
		resp.LastError = &le
	}
	if row.ScheduledAt.Valid {
		t := row.ScheduledAt.Time
		resp.ScheduledAt = &t
	}
	if row.SentAt.Valid {
		t := row.SentAt.Time
		resp.SentAt = &t
	}
	return resp
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
