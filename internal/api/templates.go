package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
	tmpl "github.com/mertovun/event-driven-notification-system/internal/template"
)

type templateCreateRequest struct {
	Name    string               `json:"name"`
	Channel notification.Channel `json:"channel"`
	Body    string               `json:"body"`
}

type templateResponse struct {
	ID           uuid.UUID            `json:"id"`
	Name         string               `json:"name"`
	Channel      notification.Channel `json:"channel"`
	Version      int32                `json:"version"`
	Body         string               `json:"body"`
	RequiredVars []string             `json:"required_vars"`
	CreatedAt    time.Time            `json:"created_at"`
	UpdatedAt    time.Time            `json:"updated_at"`
	DeprecatedAt *time.Time           `json:"deprecated_at,omitempty"`
}

type templatesHandler struct {
	q *gen.Queries
}

func newTemplatesHandler(d Deps) *templatesHandler {
	return &templatesHandler{q: d.Queries}
}

// create handles POST /v1/templates.
// Parses the body, extracts required_vars, persists. Returns 201.
func (h *templatesHandler) create(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytesDefault))
	dec.DisallowUnknownFields()

	var req templateCreateRequest
	if err := dec.Decode(&req); err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: err.Error()}})
		return
	}

	if fe := validateTemplateCreate(req); len(fe) > 0 {
		WriteValidationProblem(w, r, fe)
		return
	}

	_, requiredVars, err := tmpl.Parse(req.Body)
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: err.Error()}})
		return
	}

	row, err := h.q.InsertTemplate(r.Context(), gen.InsertTemplateParams{
		Name:         req.Name,
		Channel:      string(req.Channel),
		Version:      1,
		Body:         req.Body,
		RequiredVars: requiredVars,
		CreatedBy:    ownerFromCtx(r.Context()),
	})
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("insert template: %w", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(templateRowToResponse(row))
}

// get handles GET /v1/templates/{id}. Owner-or-admin gated; cross-key reads
// surface as 404 to deny existence-oracle behaviour.
func (h *templatesHandler) get(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a UUID"}})
		return
	}
	row, err := h.q.GetTemplateByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("get template: %w", err))
		return
	}
	// Admin sees everything; non-admin only sees their own rows. Templates
	// pre-dating the created_by column (NULL) are admin-only.
	if k, ok := AuthedKeyFrom(r.Context()); !ok || !k.HasScope(ScopeAdmin) {
		owner := ownerFromCtx(r.Context())
		if !owner.Valid || !row.CreatedBy.Valid || row.CreatedBy.UUID != owner.UUID {
			WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(templateRowToResponse(row))
}

// list handles GET /v1/templates with cursor pagination.
type templateListResponse struct {
	Items []templateResponse `json:"items"`
}

func (h *templatesHandler) list(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			WriteValidationProblem(w, r, []FieldError{{Field: "limit", Message: "must be a positive integer"}})
			return
		}
		if n > 200 {
			n = 200
		}
		limit = n
	}

	// Non-admin sees only its own templates. Admin bypasses scope for
	// support/audit visibility.
	var rows []gen.Template
	var err error
	if k, ok := AuthedKeyFrom(r.Context()); ok && k.HasScope(ScopeAdmin) {
		rows, err = h.q.ListActiveTemplates(r.Context(), int32(limit))
	} else {
		owner := ownerFromCtx(r.Context())
		if !owner.Valid {
			rows = nil
		} else {
			rows, err = h.q.ListActiveTemplatesScoped(r.Context(), gen.ListActiveTemplatesScopedParams{
				CreatedBy: owner,
				PageLimit: int32(limit),
			})
		}
	}
	if err != nil {
		WriteErrorAsProblem(w, r, fmt.Errorf("list templates: %w", err))
		return
	}
	resp := templateListResponse{Items: make([]templateResponse, len(rows))}
	for i, row := range rows {
		resp.Items[i] = templateRowToResponse(row)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// put handles PUT /v1/templates/{id}: replace body, bump version. name/channel immutable.
type templateUpdateRequest struct {
	Body string `json:"body"`
}

func (h *templatesHandler) put(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a UUID"}})
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytesDefault))
	dec.DisallowUnknownFields()

	var req templateUpdateRequest
	if err := dec.Decode(&req); err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: err.Error()}})
		return
	}
	if req.Body == "" {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: "must not be empty"}})
		return
	}

	_, requiredVars, err := tmpl.Parse(req.Body)
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "body", Message: err.Error()}})
		return
	}

	// Owner-or-admin gate. Without it, any writer-scope key can mutate
	// any template body, which is a stored-template-injection vector
	// against every other key that renders the same template.
	var row gen.Template
	if k, ok := AuthedKeyFrom(r.Context()); ok && k.HasScope(ScopeAdmin) {
		row, err = h.q.BumpTemplateVersion(r.Context(), gen.BumpTemplateVersionParams{
			ID:           id,
			Body:         req.Body,
			RequiredVars: requiredVars,
		})
	} else {
		owner := ownerFromCtx(r.Context())
		if !owner.Valid {
			WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
			return
		}
		row, err = h.q.BumpTemplateVersionScoped(r.Context(), gen.BumpTemplateVersionScopedParams{
			ID:           id,
			Body:         req.Body,
			RequiredVars: requiredVars,
			CreatedBy:    owner,
		})
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// 404 covers both not-found and not-owned (no existence oracle).
			WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
			return
		}
		WriteErrorAsProblem(w, r, fmt.Errorf("bump template: %w", err))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(templateRowToResponse(row))
}

// del handles DELETE /v1/templates/{id}: soft-deprecate.
func (h *templatesHandler) del(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		WriteValidationProblem(w, r, []FieldError{{Field: "id", Message: "must be a UUID"}})
		return
	}
	// Owner-or-admin gate.
	if k, ok := AuthedKeyFrom(r.Context()); ok && k.HasScope(ScopeAdmin) {
		if err := h.q.DeprecateTemplate(r.Context(), id); err != nil {
			WriteErrorAsProblem(w, r, fmt.Errorf("deprecate template: %w", err))
			return
		}
	} else {
		owner := ownerFromCtx(r.Context())
		if !owner.Valid {
			WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
			return
		}
		n, err := h.q.DeprecateTemplateScoped(r.Context(), gen.DeprecateTemplateScopedParams{
			ID:        id,
			CreatedBy: owner,
		})
		if err != nil {
			WriteErrorAsProblem(w, r, fmt.Errorf("deprecate template: %w", err))
			return
		}
		if n == 0 {
			WriteErrorAsProblem(w, r, notification.ErrTemplateNotFound)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func validateTemplateCreate(req templateCreateRequest) []FieldError {
	var fe []FieldError
	if req.Name == "" || len(req.Name) > 100 {
		fe = append(fe, FieldError{Field: "name", Message: "must be 1..100 chars"})
	}
	if !req.Channel.Valid() {
		fe = append(fe, FieldError{Field: "channel", Message: "must be sms | email | push"})
	}
	if req.Body == "" || len(req.Body) > 32768 {
		fe = append(fe, FieldError{Field: "body", Message: "must be 1..32768 chars"})
	}
	return fe
}

func templateRowToResponse(row gen.Template) templateResponse {
	resp := templateResponse{
		ID:           row.ID,
		Name:         row.Name,
		Channel:      notification.Channel(row.Channel),
		Version:      row.Version,
		Body:         row.Body,
		RequiredVars: row.RequiredVars,
		CreatedAt:    row.CreatedAt.Time,
		UpdatedAt:    row.UpdatedAt.Time,
	}
	if row.DeprecatedAt.Valid {
		t := row.DeprecatedAt.Time
		resp.DeprecatedAt = &t
	}
	return resp
}
