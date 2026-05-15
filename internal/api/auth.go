package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/crypto/argon2"

	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Argon2id parameters per docs/05-security-and-networking.md §1.
// Tuned to take ~50ms on a modern core — fast enough for the request path,
// slow enough to make brute-force expensive.
const (
	argonTime    uint32 = 2
	argonMemory  uint32 = 64 * 1024 // 64 MB
	argonThreads uint8  = 1
	argonSaltLen uint32 = 16
	argonKeyLen  uint32 = 32

	keyPrefixLen = 8 // chars of the raw key stored for fast candidate lookup
)

// Scope tokens. Match exactly what the api_keys.scopes column carries.
const (
	ScopeRead  = "notifications:read"
	ScopeWrite = "notifications:write"
	ScopeAdmin = "admin"
)

// HashAPIKey produces the argon2id hash + the public prefix.
// Raw key format we mint is `<8 random base32 chars>` || `<random suffix>`.
func HashAPIKey(raw string) (hash string, prefix string, err error) {
	if len(raw) < keyPrefixLen {
		return "", "", fmt.Errorf("raw key too short")
	}
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", "", fmt.Errorf("salt: %w", err)
	}
	digest := argon2.IDKey([]byte(raw), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	encoded := fmt.Sprintf("argon2id$%d$%d$%d$%s$%s",
		argonTime, argonMemory, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(digest),
	)
	return encoded, raw[:keyPrefixLen], nil
}

// VerifyAPIKey checks the raw key against an encoded argon2id hash in constant time.
func VerifyAPIKey(raw, encoded string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "argon2id" {
		return false, fmt.Errorf("malformed hash")
	}
	var t, m uint32
	var p uint8
	if _, err := fmt.Sscanf(parts[1], "%d", &t); err != nil {
		return false, err
	}
	if _, err := fmt.Sscanf(parts[2], "%d", &m); err != nil {
		return false, err
	}
	if _, err := fmt.Sscanf(parts[3], "%d", &p); err != nil {
		return false, err
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, err
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, err
	}
	got := argon2.IDKey([]byte(raw), salt, t, m, p, uint32(len(want)))
	return subtle.ConstantTimeCompare(want, got) == 1, nil
}

// authedKey holds the row that authenticated the current request.
type authedKey struct {
	ID     string
	Name   string
	Scopes []string
}

func (k authedKey) HasScope(want string) bool {
	for _, s := range k.Scopes {
		if s == want {
			return true
		}
	}
	return false
}

const ctxKeyAuthedKey ctxKey = 100

// AuthedKeyFrom pulls the authenticated key off context. Empty if missing.
func AuthedKeyFrom(ctx context.Context) (authedKey, bool) {
	k, ok := ctx.Value(ctxKeyAuthedKey).(authedKey)
	return k, ok
}

// AuthMiddleware validates the Authorization: Bearer <key> header against api_keys.
// Returns 401 on missing/invalid; attaches authedKey to context on success.
//
// The lookup is a two-step "narrow then verify" per docs/05 §1:
//  1. Read first `keyPrefixLen` chars → SELECT ... WHERE key_prefix = $1.
//  2. For each candidate, argon2 verify; succeed on first match (constant-time per row).
func AuthMiddleware(q *gen.Queries) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := extractBearer(r)
			if !ok {
				WriteProblem(w, r, http.StatusUnauthorized,
					"/problems/unauthorized", "Unauthorized", "missing or malformed Authorization header")
				return
			}
			if len(raw) < keyPrefixLen {
				WriteProblem(w, r, http.StatusUnauthorized,
					"/problems/unauthorized", "Unauthorized", "key too short")
				return
			}
			prefix := raw[:keyPrefixLen]

			cands, err := q.ListActiveAPIKeysByPrefix(r.Context(), prefix)
			if err != nil {
				WriteErrorAsProblem(w, r, fmt.Errorf("api_keys lookup: %w", err))
				return
			}

			for _, c := range cands {
				okMatch, verr := VerifyAPIKey(raw, c.HashedKey)
				if verr != nil {
					continue
				}
				if okMatch {
					ctx := context.WithValue(r.Context(), ctxKeyAuthedKey, authedKey{
						ID:     c.ID.String(),
						Name:   c.Name,
						Scopes: c.Scopes,
					})
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			WriteProblem(w, r, http.StatusUnauthorized,
				"/problems/unauthorized", "Unauthorized", "invalid api key")
		})
	}
}

// RequireScope enforces a scope on a route subtree. Use after AuthMiddleware.
func RequireScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k, ok := AuthedKeyFrom(r.Context())
			if !ok {
				WriteProblem(w, r, http.StatusUnauthorized,
					"/problems/unauthorized", "Unauthorized", "no authenticated key in context")
				return
			}
			if !k.HasScope(scope) {
				WriteProblem(w, r, http.StatusForbidden,
					"/problems/forbidden", "Forbidden", "missing required scope: "+scope)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func extractBearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	raw := strings.TrimPrefix(h, prefix)
	if raw == "" {
		return "", false
	}
	return raw, true
}

// SeedDevKey inserts (or upserts) the dev API key with full scopes.
// Called at startup when DEV_API_KEY is non-empty. The raw key value comes from env.
func SeedDevKey(ctx context.Context, q *gen.Queries, raw string) error {
	if raw == "" {
		return errors.New("dev key seed: empty key")
	}
	hash, prefix, err := HashAPIKey(raw)
	if err != nil {
		return fmt.Errorf("hash: %w", err)
	}
	// Stable name so re-seeding doesn't create duplicates.
	_, err = q.UpsertAPIKey(ctx, gen.UpsertAPIKeyParams{
		Name:      "dev-seed",
		HashedKey: hash,
		KeyPrefix: prefix,
		Scopes:    []string{ScopeRead, ScopeWrite, ScopeAdmin},
	})
	return err
}

// (Unused but kept here to document the contract: a hex digest of the raw key
// would be wrong because it's a deterministic fingerprint — argon2id with a
// random salt is what we use.)
var _ = hex.EncodeToString
