package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

const (
	defaultPageSize = 50
	maxPageSize     = 200
)

// cursor is opaque to clients. We base64url-encode a JSON {ts, id} where the
// pair points to the *last row of the previous page*. Subsequent queries use
// the keyset predicate `(created_at, id) < (ts, id)`.
type cursor struct {
	TS time.Time `json:"ts"`
	ID uuid.UUID `json:"id"`
}

func (c cursor) encode() (string, error) {
	raw, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodeCursor(s string) (cursor, error) {
	if s == "" {
		return cursor{}, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return cursor{}, fmt.Errorf("decode cursor: %w", err)
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return cursor{}, fmt.Errorf("unmarshal cursor: %w", err)
	}
	return c, nil
}

type listResponse struct {
	Items      []notificationResponse `json:"items"`
	NextCursor *string                `json:"next_cursor,omitempty"`
	HasMore    bool                   `json:"has_more"`
}

// list handles GET /v1/notifications with cursor pagination and filtering.
// Query params: status, channel, batch_id, created_after, created_before, cursor, limit.
// Uses squirrel because the WHERE clause is dynamic (sqlc can't generate
// that one cleanly — dynamic filter sets aren't its strength).
func (h *notificationsHandler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Pagination knobs.
	limit := defaultPageSize
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			WriteValidationProblem(w, r, []FieldError{{Field: "limit", Message: "must be a positive integer"}})
			return
		}
		if n > maxPageSize {
			n = maxPageSize
		}
		limit = n
	}

	cur, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "cursor", Message: "malformed"}})
		return
	}

	// Build the predicate.
	builder := sq.
		Select("id", "batch_id", "channel", "recipient", "content", "priority",
			"status", "idempotency_key", "attempt_count", "last_error", "scheduled_at",
			"created_at", "updated_at", "sent_at", "correlation_id",
			"template_id", "template_version").
		From("notifications").
		OrderBy("created_at DESC", "id DESC").
		Limit(uint64(limit + 1)). // +1 to detect has_more
		PlaceholderFormat(sq.Dollar)

	// AuthZ — non-admin keys see only their own rows. Admin scope bypasses.
	if k, ok := AuthedKeyFrom(r.Context()); !ok || !k.HasScope(ScopeAdmin) {
		owner := ownerFromCtx(r.Context())
		if !owner.Valid {
			// No identity — return empty list rather than 401 (middleware
			// already authenticated; this only catches missing-context bugs).
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(listResponse{Items: []notificationResponse{}})
			return
		}
		builder = builder.Where(sq.Eq{"created_by": owner.UUID})
	}

	if s := q.Get("status"); s != "" {
		if !notification.Status(s).Valid() {
			WriteValidationProblem(w, r, []FieldError{{Field: "status", Message: "invalid"}})
			return
		}
		builder = builder.Where(sq.Eq{"status": s})
	}
	if c := q.Get("channel"); c != "" {
		if !notification.Channel(c).Valid() {
			WriteValidationProblem(w, r, []FieldError{{Field: "channel", Message: "invalid"}})
			return
		}
		builder = builder.Where(sq.Eq{"channel": c})
	}
	if b := q.Get("batch_id"); b != "" {
		bid, err := uuid.Parse(b)
		if err != nil {
			WriteValidationProblem(w, r, []FieldError{{Field: "batch_id", Message: "must be a UUID"}})
			return
		}
		builder = builder.Where(sq.Eq{"batch_id": bid})
	}
	if v := q.Get("created_after"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			WriteValidationProblem(w, r, []FieldError{{Field: "created_after", Message: "must be RFC3339"}})
			return
		}
		builder = builder.Where(sq.GtOrEq{"created_at": t})
	}
	if v := q.Get("created_before"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			WriteValidationProblem(w, r, []FieldError{{Field: "created_before", Message: "must be RFC3339"}})
			return
		}
		builder = builder.Where(sq.Lt{"created_at": t})
	}

	// Keyset predicate: (created_at, id) < (cursor.ts, cursor.id) for newest-first ordering.
	if !cur.TS.IsZero() {
		builder = builder.Where(
			sq.Expr("(created_at, id) < (?, ?)", cur.TS, cur.ID),
		)
	}

	sqlStr, args, err := builder.ToSql()
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("build query: %w", err))
		return
	}

	rows, err := h.pool.Query(r.Context(), sqlStr, args...)
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("list query: %w", err))
		return
	}
	defer rows.Close()

	var out []notificationResponse
	for rows.Next() {
		var row scanRow
		if err := rows.Scan(
			&row.id, &row.batchID, &row.channel, &row.recipient, &row.content,
			&row.priority, &row.status, &row.idempotencyKey, &row.attemptCount, &row.lastError,
			&row.scheduledAt, &row.createdAt, &row.updatedAt, &row.sentAt, &row.correlationID,
			&row.templateID, &row.templateVersion,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				break
			}
			WriteErrorAsProblem(w, r, fmt.Errorf("scan row: %w", err))
			return
		}
		out = append(out, row.toResponse())
	}
	if err := rows.Err(); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("iterate rows: %w", err))
		return
	}

	resp := listResponse{Items: out}
	if len(out) > limit {
		// Slice off the lookahead row and emit a cursor pointing at the last *returned* row.
		resp.Items = out[:limit]
		last := resp.Items[len(resp.Items)-1]
		c := cursor{TS: last.CreatedAt, ID: last.ID}
		enc, _ := c.encode()
		resp.NextCursor = &enc
		resp.HasMore = true
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// scanRow buffers a row from the raw squirrel query so we can re-use rowToResponse-style mapping.
// We can't pass a generated gen.Notification directly into the squirrel pipeline.
type scanRow struct {
	id              uuid.UUID
	batchID         uuid.NullUUID
	channel         string
	recipient       string
	content         string
	priority        int16
	status          string
	idempotencyKey  *string
	attemptCount    int32
	lastError       *string
	scheduledAt     *time.Time
	createdAt       time.Time
	updatedAt       time.Time
	sentAt          *time.Time
	correlationID   string
	templateID      uuid.NullUUID
	templateVersion *int32
}

func (s scanRow) toResponse() notificationResponse {
	resp := notificationResponse{
		ID:            s.id,
		Channel:       notification.Channel(s.channel),
		Recipient:     s.recipient,
		Content:       s.content,
		Priority:      notification.PriorityFromInt(s.priority),
		Status:        notification.Status(s.status),
		AttemptCount:  s.attemptCount,
		CreatedAt:     s.createdAt,
		UpdatedAt:     s.updatedAt,
		CorrelationID: s.correlationID,
	}
	if s.batchID.Valid {
		b := s.batchID.UUID
		resp.BatchID = &b
	}
	if s.lastError != nil {
		resp.LastError = s.lastError
	}
	resp.ScheduledAt = s.scheduledAt
	resp.SentAt = s.sentAt
	return resp
}
