// Package idempotency implements the Idempotency-Key cache (Redis) with body-hash
// canonicalization and replay semantics. 24h TTL; same key + same body → replay
// stored response; same key + different body → 409.
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// DefaultTTL is the post-Finalize idempotency window (24h).
const DefaultTTL = 24 * time.Hour

// InFlightTTL is the short window during which a request is allowed to
// finish before the slot is considered abandoned. Matches ADR-0006: a
// crashed handler unwedges the key in InFlightTTL rather than the full
// DefaultTTL.
const InFlightTTL = 10 * time.Second

// Sentinels surfaced to the API mapper. See internal/notification/errors.go for the
// canonical domain sentinels; we wrap our store errors with those at call sites.
var (
	// ErrConflict — same key, different request body. 409.
	ErrConflict = errors.New("idempotency: key reused with different body")
	// ErrInFlight — a request with this key is still being processed.
	ErrInFlight = errors.New("idempotency: request in flight")
)

// Record is what we store in Redis for each Idempotency-Key.
type Record struct {
	BodyHash   string `json:"body_hash"`
	StatusCode int    `json:"status"`
	Body       []byte `json:"body"` // canonical response body, replayed verbatim
	InFlight   bool   `json:"in_flight,omitempty"`
}

// Store is the small adapter around Redis. The interface is intentionally narrow
// so a test can swap it for an in-memory fake.
type Store struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewStore(rdb *redis.Client, ttl time.Duration) *Store {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Store{rdb: rdb, ttl: ttl}
}

// CanonicalHash produces a stable hash of an arbitrary JSON-serializable value.
// Implementation: re-marshal through map[string]any to normalize key ordering,
// then sha256. This makes `{a:1,b:2}` and `{b:2,a:1}` collide as intended for
// idempotency body comparison.
func CanonicalHash(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	var m any
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", fmt.Errorf("unmarshal: %w", err)
	}
	canon, err := canonicalize(m)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(canon)
	if err != nil {
		return "", fmt.Errorf("re-marshal: %w", err)
	}
	sum := sha256.Sum256(out)
	return hex.EncodeToString(sum[:]), nil
}

// canonicalize recursively sorts map keys so json.Marshal produces deterministic output.
func canonicalize(v any) (any, error) {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([][2]any, 0, len(x))
		for _, k := range keys {
			cv, err := canonicalize(x[k])
			if err != nil {
				return nil, err
			}
			out = append(out, [2]any{k, cv})
		}
		// Marshal as a 2-element-tuple list so json.Marshal preserves order.
		return out, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			ce, err := canonicalize(e)
			if err != nil {
				return nil, err
			}
			out[i] = ce
		}
		return out, nil
	default:
		return x, nil
	}
}

// key namespaces the idempotency cache by api_key_id when ownerID is non-empty.
// Without a per-owner prefix, two API keys submitting Idempotency-Key: "abc"
// with different bodies would either collide (409) or worse, replay each
// other's responses. Pre-existing single-tenant keys with ownerID == "" fall
// back to the legacy "idem:<key>" format for backwards compatibility — there
// shouldn't be any in production but the code shouldn't panic on them.
func key(ownerID, idemKey string) string {
	if ownerID == "" {
		return "idem:" + idemKey
	}
	return "idem:" + ownerID + ":" + idemKey
}

// BeginOrReplay attempts to claim the idempotency key with an in-flight marker.
// Returns (replay=nil, ok=true) when the caller now owns the slot and should proceed
// to do the real work, then call Finalize.
// Returns (replay=non-nil) when an existing record matches the same body — replay it verbatim.
// Returns ErrConflict when an existing record has a different body hash.
// Returns ErrInFlight when an earlier request with this key is still being processed.
//
// Two TTLs apply:
//   - Initial SETNX uses InFlightTTL (10s) so a crashed handler unwedges
//     the slot in seconds instead of 24h.
//   - Finalize re-SETs the slot with the long DefaultTTL (24h) for replays.
//
// ownerID scopes the cache by the authenticated api_key_id; "" falls back to
// the legacy single-tenant namespace for test/migration paths.
func (s *Store) BeginOrReplay(ctx context.Context, ownerID, idemKey, bodyHash string) (replay *Record, err error) {
	if idemKey == "" {
		return nil, errors.New("empty idempotency key")
	}

	k := key(ownerID, idemKey)

	// Atomic claim attempt: SET NX with in-flight marker carrying our body
	// hash. Short TTL on the claim so a crashed handler unwedges quickly.
	inFlight := &Record{BodyHash: bodyHash, InFlight: true}
	raw, _ := json.Marshal(inFlight)
	ok, err := s.rdb.SetNX(ctx, k, raw, InFlightTTL).Result()
	if err != nil {
		return nil, fmt.Errorf("setnx: %w", err)
	}
	if ok {
		// We own the slot. Caller will Finalize once the work is done.
		return nil, nil
	}

	// Slot exists; GET and decide.
	existing, err := s.rdb.Get(ctx, k).Bytes()
	if err != nil {
		// Race: someone deleted between SetNX and Get. Treat as in-flight.
		if errors.Is(err, redis.Nil) {
			return nil, ErrInFlight
		}
		return nil, fmt.Errorf("get: %w", err)
	}
	var rec Record
	if err := json.Unmarshal(existing, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal record: %w", err)
	}
	if rec.BodyHash != bodyHash {
		return nil, ErrConflict
	}
	if rec.InFlight {
		return nil, ErrInFlight
	}
	return &rec, nil
}

// Finalize stores the canonical response so future replays can return it verbatim.
// Refreshes the TTL to s.ttl (the long replay window) so an in-flight claim
// that's now complete survives the InFlightTTL ceiling.
func (s *Store) Finalize(ctx context.Context, ownerID, idemKey string, rec Record) error {
	rec.InFlight = false
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return s.rdb.Set(ctx, key(ownerID, idemKey), raw, s.ttl).Err()
}

// Release deletes the in-flight marker. Use this when the handler decided NOT to
// persist a canonical response (e.g., the work failed before status was determined).
func (s *Store) Release(ctx context.Context, ownerID, idemKey string) error {
	return s.rdb.Del(ctx, key(ownerID, idemKey)).Err()
}
