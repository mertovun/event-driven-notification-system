package api

import (
	"context"
	"net/http"
	"time"
)

// contextWithTimeout exists so tests can later swap in a fake clock.
func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

// WriteProblem is the simple sink used by middleware (Recoverer, NotFound).
// Handlers should call WriteErrorAsProblem (errors.go) which has the full
// sentinel→status mapping. This shim survives so existing call sites stay
// terse for the simple cases.
func WriteProblem(w http.ResponseWriter, r *http.Request, status int, typ, title, detail string) {
	WriteProblemFull(w, r, Problem{
		Type:   typ,
		Title:  title,
		Status: status,
		Detail: detail,
	})
}
