package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type stubVerifier struct {
	broken int64
	err    error
	calls  int
}

func (s *stubVerifier) VerifyOnce(_ context.Context) (int64, error) {
	s.calls++
	return s.broken, s.err
}

func TestVerifyAuditChain_intact(t *testing.T) {
	t.Parallel()
	v := &stubVerifier{broken: 0}
	h := &adminHandler{verifier: v}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/admin/audit/verify", nil)
	h.verifyAuditChain(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d (body=%s)", w.Code, w.Body.String())
	}
	if v.calls != 1 {
		t.Fatalf("verifier calls: want 1, got %d", v.calls)
	}
	var body struct {
		BrokenLinks int64 `json:"broken_links"`
		Intact      bool  `json:"intact"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.BrokenLinks != 0 || !body.Intact {
		t.Fatalf("body: want {0, true}, got %+v", body)
	}
}

func TestVerifyAuditChain_broken(t *testing.T) {
	t.Parallel()
	v := &stubVerifier{broken: 3}
	h := &adminHandler{verifier: v}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/admin/audit/verify", nil)
	h.verifyAuditChain(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status: want 200, got %d", w.Code)
	}
	var body struct {
		BrokenLinks int64 `json:"broken_links"`
		Intact      bool  `json:"intact"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.BrokenLinks != 3 || body.Intact {
		t.Fatalf("body: want {3, false}, got %+v", body)
	}
}

func TestVerifyAuditChain_queryError(t *testing.T) {
	t.Parallel()
	v := &stubVerifier{err: errors.New("db down")}
	h := &adminHandler{verifier: v}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/admin/audit/verify", nil)
	h.verifyAuditChain(w, r)

	if w.Code == http.StatusOK {
		t.Fatalf("status: want non-200 on verifier error, got 200")
	}
}

func TestVerifyAuditChain_notWired(t *testing.T) {
	t.Parallel()
	h := &adminHandler{verifier: nil}

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/admin/audit/verify", nil)
	h.verifyAuditChain(w, r)

	if w.Code == http.StatusOK {
		t.Fatalf("status: want non-200 when verifier unwired, got 200")
	}
}
