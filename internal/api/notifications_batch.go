package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// MaxBatchItems is the per-request batch ceiling per docs/01 §9.
const MaxBatchItems = 1000

type batchCreateRequest struct {
	Items []notificationCreateRequest `json:"items"`
}

type batchItemResult struct {
	Index  int                 `json:"index"`
	ID     *uuid.UUID          `json:"id,omitempty"`
	Status notification.Status `json:"status,omitempty"`
	Error  *Problem            `json:"error,omitempty"`
}

type batchCreateResponse struct {
	BatchID       uuid.UUID         `json:"batch_id"`
	Status        string            `json:"status"` // accepted | partial | rejected
	TotalCount    int               `json:"total_count"`
	AcceptedCount int               `json:"accepted_count"`
	RejectedCount int               `json:"rejected_count"`
	Items         []batchItemResult `json:"items"`
}

// createBatch handles POST /v1/notifications/batch.
//
// Validation is per-item. The whole batch lands in one TX so we get all-or-nothing
// on the durable side: if any DB insert fails, we roll back and respond 500.
// Per-item *validation* failures are returned alongside the accepted items;
// they don't fail the batch — they just reduce accepted_count.
func (h *notificationsHandler) createBatch(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytesBatch))
	dec.DisallowUnknownFields()

	var req batchCreateRequest
	if err := dec.Decode(&req); err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: err.Error()}})
		return
	}
	if len(req.Items) == 0 {
		WriteValidationProblem(w, r, []FieldError{{Field: "items", Message: "must not be empty"}})
		return
	}
	if len(req.Items) > MaxBatchItems {
		WriteValidationProblem(w, r, []FieldError{
			{Field: "items", Message: fmt.Sprintf("max %d items per batch", MaxBatchItems)},
		})
		return
	}

	corrID := CorrelationIDFrom(r.Context())

	// Pre-validate each item; collect errors but keep going.
	results := make([]batchItemResult, len(req.Items))
	valid := make([]int, 0, len(req.Items))
	for i, it := range req.Items {
		if it.Priority == "" {
			it.Priority = notification.PriorityNormal
			req.Items[i] = it
		}
		if fe := validateCreate(it); len(fe) > 0 {
			results[i] = batchItemResult{
				Index: i,
				Error: &Problem{
					Type:   "/problems/validation",
					Title:  "Validation Failed",
					Status: http.StatusBadRequest,
					Detail: "field validation failed",
					Extra:  map[string]any{"errors": fe},
				},
			}
			continue
		}
		valid = append(valid, i)
	}

	batchID := uuid.Must(uuid.NewV7())

	if len(valid) == 0 {
		// All items failed validation — return rejected without writing anything.
		resp := batchCreateResponse{
			BatchID:       batchID,
			Status:        "rejected",
			TotalCount:    len(req.Items),
			AcceptedCount: 0,
			RejectedCount: len(req.Items),
			Items:         results,
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// One TX for the whole batch.
	tx, err := h.pool.BeginTx(r.Context(), pgx.TxOptions{})
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("begin tx: %w", err))
		return
	}
	defer func() { _ = tx.Rollback(r.Context()) }() // no-op if committed

	qtx := h.q.WithTx(tx)

	batchStatus := "accepted"
	if len(valid) < len(req.Items) {
		batchStatus = "partial"
	}

	if _, err := qtx.InsertBatch(r.Context(), gen.InsertBatchParams{
		ID:             batchID,
		TotalCount:     int32(len(req.Items)),
		AcceptedCount:  int32(len(valid)),
		RejectedCount:  int32(len(req.Items) - len(valid)),
		IdempotencyKey: nullableString(r.Header.Get("Idempotency-Key")),
		Status:         batchStatus,
	}); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("insert batch: %w", err))
		return
	}

	for _, i := range valid {
		it := req.Items[i]
		row, err := h.insertBatchItem(r.Context(), qtx, batchID, it, corrID)
		if err != nil {
			WriteErrorAsProblem(w, r, fmt.Errorf("insert batch item %d: %w", i, err))
			return
		}
		results[i] = batchItemResult{
			Index:  i,
			ID:     &row.ID,
			Status: notification.Status(row.Status),
		}
	}

	if err := tx.Commit(r.Context()); err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("commit: %w", err))
		return
	}

	resp := batchCreateResponse{
		BatchID:       batchID,
		Status:        batchStatus,
		TotalCount:    len(req.Items),
		AcceptedCount: len(valid),
		RejectedCount: len(req.Items) - len(valid),
		Items:         results,
	}
	status := http.StatusCreated
	if batchStatus == "partial" {
		status = http.StatusMultiStatus
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

// insertBatchItem inserts a single notifications row + outbox row for one batch item.
// Returns the inserted notification row.
func (h *notificationsHandler) insertBatchItem(
	ctx context.Context,
	qtx *gen.Queries,
	batchID uuid.UUID,
	it notificationCreateRequest,
	corrID string,
) (gen.Notification, error) {
	id := uuid.Must(uuid.NewV7())
	priorityInt := it.Priority.Int16()
	status := notification.StatusPending

	row, err := qtx.InsertNotification(ctx, gen.InsertNotificationParams{
		ID:              id,
		BatchID:         uuid.NullUUID{UUID: batchID, Valid: true},
		Channel:         string(it.Channel),
		Recipient:       it.Recipient,
		Content:         it.Content,
		Priority:        priorityInt,
		Status:          string(status),
		CorrelationID:   corrID,
		TemplateID:      uuid.NullUUID{},
		TemplateVersion: nil,
	})
	if err != nil {
		return gen.Notification{}, err
	}

	envelope := map[string]any{
		"notification_id": id.String(),
		"channel":         it.Channel,
		"recipient":       it.Recipient,
		"content":         it.Content,
		"priority":        it.Priority,
		"correlation_id":  corrID,
	}
	payload, _ := json.Marshal(envelope)
	headers, _ := json.Marshal(map[string]any{
		"x-correlation-id": corrID,
	})

	if _, err := qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
		NotificationID: id,
		RoutingKey:     "notification." + string(it.Channel),
		Payload:        payload,
		Headers:        headers,
		Priority:       priorityInt,
	}); err != nil {
		return gen.Notification{}, err
	}
	return row, nil
}

// get handles GET /v1/notifications/{id}.
func (h *notificationsHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a UUID"}})
		return
	}
	row, err := h.q.GetNotificationByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteErrorAsProblem(w, r, notification.ErrNotFound)
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("get notification: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rowToResponse(row))
}

// getByBatch handles GET /v1/notifications/batch/{batchId}.
// Returns the batch metadata plus all its notifications.
type batchDetailResponse struct {
	Batch struct {
		ID            uuid.UUID `json:"id"`
		Status        string    `json:"status"`
		TotalCount    int32     `json:"total_count"`
		AcceptedCount int32     `json:"accepted_count"`
		RejectedCount int32     `json:"rejected_count"`
	} `json:"batch"`
	Items []notificationResponse `json:"items"`
}

func (h *notificationsHandler) getByBatch(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "batchId"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "batchId", Message: "must be a UUID"}})
		return
	}
	batch, err := h.q.GetBatchByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteErrorAsProblem(w, r, notification.ErrNotFound)
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("get batch: %w", err))
		return
	}
	items, err := h.q.GetNotificationsByBatchID(r.Context(), uuid.NullUUID{UUID: id, Valid: true})
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("list batch items: %w", err))
		return
	}

	resp := batchDetailResponse{Items: make([]notificationResponse, len(items))}
	resp.Batch.ID = batch.ID
	resp.Batch.Status = batch.Status
	resp.Batch.TotalCount = batch.TotalCount
	resp.Batch.AcceptedCount = batch.AcceptedCount
	resp.Batch.RejectedCount = batch.RejectedCount
	for i, it := range items {
		resp.Items[i] = rowToResponse(it)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// cancel handles DELETE /v1/notifications/{id}.
// CAS update: only transitions pending/queued/scheduled → cancelled.
// Returns 409 if the row is past those states (sending/sent/failed/etc).
func (h *notificationsHandler) cancel(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a UUID"}})
		return
	}

	row, err := h.q.CancelPendingOrQueued(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Either not found OR already past cancellable states.
			// Disambiguate so the client gets a meaningful 404 vs 409.
			_, gerr := h.q.GetNotificationByID(r.Context(), id)
			if errors.Is(gerr, pgx.ErrNoRows) {
				WriteErrorAsProblem(w, r, notification.ErrNotFound)
				return
			}
			WriteErrorAsProblem(w, r, fmt.Errorf("%w: cannot cancel from current state", notification.ErrInvalidState))
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("cancel: %w", err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rowToResponse(row))
}
