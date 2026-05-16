package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// adminHandler exposes DLQ inspection and replay endpoints. All routes require
// the `admin` scope, enforced upstream by RequireScope().
type adminHandler struct {
	notifH *notificationsHandler // reused for outbox-row insert helper
	q      *gen.Queries
}

func newAdminHandler(d Deps, notifH *notificationsHandler) *adminHandler {
	return &adminHandler{notifH: notifH, q: d.Queries}
}

type deadLetterResponse struct {
	ID             int64     `json:"id"`
	NotificationID uuid.UUID `json:"notification_id"`
	Reason         string    `json:"reason"`
	DLQAt          time.Time `json:"dlq_at"`
	Payload        any       `json:"payload"`
}

type dlqListResponse struct {
	Items []deadLetterResponse `json:"items"`
}

// list — GET /v1/admin/dead-letters
// Optional ?channel=sms filter.
func (h *adminHandler) list(w http.ResponseWriter, r *http.Request) {
	channel := r.URL.Query().Get("channel")
	if channel != "" && !notification.Channel(channel).Valid() {
		WriteValidationProblem(w, r, []FieldError{{Field: "channel", Message: "invalid"}})
		return
	}
	rows, err := h.q.ListDeadLetters(r.Context(), gen.ListDeadLettersParams{
		ChannelFilter: channel,
		PageLimit:     100,
	})
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("list dead letters: %w", err))
		return
	}
	resp := dlqListResponse{Items: make([]deadLetterResponse, 0, len(rows))}
	for _, row := range rows {
		var payload any
		_ = json.Unmarshal(row.Payload, &payload)
		resp.Items = append(resp.Items, deadLetterResponse{
			ID: row.ID, NotificationID: row.NotificationID,
			Reason: row.Reason, DLQAt: row.DlqAt.Time, Payload: payload,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// get — GET /v1/admin/dead-letters/{id}
func (h *adminHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a notification UUID"}})
		return
	}
	row, err := h.q.GetDeadLetterByNotification(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteErrorAsProblem(w, r, notification.ErrNotFound)
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("get dead letter: %w", err))
		return
	}
	var payload any
	_ = json.Unmarshal(row.Payload, &payload)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(deadLetterResponse{
		ID: row.ID, NotificationID: row.NotificationID,
		Reason: row.Reason, DLQAt: row.DlqAt.Time, Payload: payload,
	})
}

// replay — POST /v1/admin/dead-letters/{id}/replay
// Reset-in-place model: status dead_letter → queued, attempt_count = 0,
// write a fresh outbox row, log to admin_audit. See docs/13 Part B §14.
type replayResponse struct {
	NotificationID uuid.UUID `json:"notification_id"`
	NewStatus      string    `json:"new_status"`
}

func (h *adminHandler) replay(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a notification UUID"}})
		return
	}

	// Open a TX. Update notifications row, reset counters, write outbox row, audit.
	tx, err := h.notifH.pool.Begin(r.Context())
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("begin tx: %w", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }()
	qtx := h.q.WithTx(tx)

	notif, err := qtx.GetNotificationByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteErrorAsProblem(w, r, notification.ErrNotFound)
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("get notification: %w", err))
		return
	}
	if notif.Status != string(notification.StatusDeadLetter) {
		WriteErrorAsProblem(w, r, fmt.Errorf("%w: only dead_letter is replayable, got %s",
			notification.ErrInvalidState, notif.Status))
		return
	}

	// Reset notifications row.
	if _, err := tx.Exec(r.Context(), `
		UPDATE notifications
		   SET status = 'pending', attempt_count = 0,
		       last_error = NULL, sent_at = NULL, updated_at = now()
		 WHERE id = $1`, id); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("reset notification: %w", err))
		return
	}

	// Remove the dead_letters row — the audit log retains the history.
	if err := qtx.DeleteDeadLetter(r.Context(), id); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("delete dead letter: %w", err))
		return
	}

	// Fresh outbox row so the dispatcher republishes.
	envelope := map[string]any{
		"notification_id": id.String(),
		"channel":         notif.Channel,
		"recipient":       notif.Recipient,
		"content":         notif.Content,
		"priority":        notification.PriorityFromInt(notif.Priority),
		"correlation_id":  notif.CorrelationID,
	}
	payload, _ := json.Marshal(envelope)
	headers, _ := json.Marshal(map[string]any{"x-correlation-id": notif.CorrelationID})
	if _, err := qtx.InsertOutbox(r.Context(), gen.InsertOutboxParams{
		NotificationID: id,
		RoutingKey:     "notification." + notif.Channel,
		Payload:        payload,
		Headers:        headers,
		Priority:       notif.Priority,
	}); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("insert outbox: %w", err))
		return
	}

	// Audit.
	k, _ := AuthedKeyFrom(r.Context())
	tid := id.String()
	details, _ := json.Marshal(map[string]any{"reason": "manual replay"})
	if _, err := qtx.InsertAuditEntry(r.Context(), gen.InsertAuditEntryParams{
		Actor:    k.Name,
		Action:   "dlq_replay",
		TargetID: &tid,
		Details:  details,
	}); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("audit insert: %w", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("commit: %w", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(replayResponse{NotificationID: id, NewStatus: "pending"})
}

// purge — DELETE /v1/admin/dead-letters/{id}
func (h *adminHandler) purge(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a notification UUID"}})
		return
	}
	if err := h.q.DeleteDeadLetter(r.Context(), id); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("delete dead letter: %w", err))
		return
	}
	k, _ := AuthedKeyFrom(r.Context())
	tid := id.String()
	_, _ = h.q.InsertAuditEntry(r.Context(), gen.InsertAuditEntryParams{
		Actor: k.Name, Action: "dlq_purge", TargetID: &tid, Details: []byte("{}"),
	})
	w.WriteHeader(http.StatusNoContent)
}
