package api

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/argon2"

	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Argon2id parameters: OWASP-recommended baseline.
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
// Verification path (in order):
//  1. Cache check: GET auth:v1:<sha256(raw)> from Redis. On hit, attach and proceed. (~0.3ms)
//  2. Cache miss: read first `keyPrefixLen` chars → SELECT ... WHERE key_prefix = $1.
//  3. For each candidate, argon2id verify. On match, cache the result and attach. (~50ms)
//
// The cache (rdb) is optional. Passing nil falls back to the verify-every-time path,
// which is the correct behaviour for tests that don't want a Redis dependency.
func AuthMiddleware(q *gen.Queries, rdb *redis.Client) func(http.Handler) http.Handler {
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

			// Fast path: Redis cache of recently verified bearers.
			// Revocation honoured immediately via a per-id revocation marker:
			// if the admin revoke endpoint marked this key revoked, treat the
			// cached entry as a miss so the slow path re-queries Postgres
			// (which will reject the now-revoked row).
			if hit := lookupAuthCache(r.Context(), rdb, raw); hit != nil {
				if !isKeyRevoked(r.Context(), rdb, hit.ID) {
					ctx := context.WithValue(r.Context(), ctxKeyAuthedKey, *hit)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				// Cache hit but key revoked — fall through to slow path.
				// The DB query excludes revoked rows, so the bearer will 401.
				invalidateAuthCache(r.Context(), rdb, raw)
			}

			// Slow path: prefix-narrowed candidate lookup + argon2id verify.
			prefix := raw[:keyPrefixLen]

			// Brute-force gate. Each prefix has a small budget of failed
			// verifies per window; an attacker hammering random prefixes hits
			// the lock before they can run more than a few argon2 cycles.
			// Cached hits skip this check entirely (their cost is ~0.3ms).
			if isAuthFailureLocked(r.Context(), rdb, prefix) {
				w.Header().Set("Retry-After", "60")
				WriteProblem(w, r, http.StatusTooManyRequests,
					"/problems/rate-limited", "Too Many Requests",
					"too many failed authentication attempts for this key prefix")
				return
			}

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
					ak := authedKey{
						ID:     c.ID.String(),
						Name:   c.Name,
						Scopes: c.Scopes,
					}
					storeAuthCache(r.Context(), rdb, raw, ak)
					ctx := context.WithValue(r.Context(), ctxKeyAuthedKey, ak)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}

			// Verify failed (or no candidates) — record the attempt and
			// 401. If the counter just tripped the limit, the next request
			// for this prefix will short-circuit at the gate above.
			_ = noteAuthFailure(r.Context(), rdb, prefix)
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

// HasDevSeedRow reports whether an active 'dev-seed' row exists in api_keys.
// Used by the startup guard to warn when a non-development deploy is still
// carrying the dev key. Errors return false (best-effort audit).
func HasDevSeedRow(ctx context.Context, q *gen.Queries) (bool, error) {
	return q.HasDevSeedRow(ctx)
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
