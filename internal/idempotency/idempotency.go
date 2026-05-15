// Package idempotency implements the Idempotency-Key cache (Redis) with body-hash
// canonicalization and replay semantics. See docs/01-domain-and-api.md §8.
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

// DefaultTTL is the idempotency window per docs/01 §8.
const DefaultTTL = 24 * time.Hour

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

func key(idemKey string) string { return "idem:" + idemKey }

// BeginOrReplay attempts to claim the idempotency key with an in-flight marker.
// Returns (replay=nil, ok=true) when the caller now owns the slot and should proceed
// to do the real work, then call Finalize.
// Returns (replay=non-nil) when an existing record matches the same body — replay it verbatim.
// Returns ErrConflict when an existing record has a different body hash.
// Returns ErrInFlight when an earlier request with this key is still being processed.
func (s *Store) BeginOrReplay(ctx context.Context, idemKey, bodyHash string) (replay *Record, err error) {
	if idemKey == "" {
		return nil, errors.New("empty idempotency key")
	}

	k := key(idemKey)

	// Atomic claim attempt: SET NX with in-flight marker carrying our body hash.
	inFlight := &Record{BodyHash: bodyHash, InFlight: true}
	raw, _ := json.Marshal(inFlight)
	ok, err := s.rdb.SetNX(ctx, k, raw, s.ttl).Result()
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
func (s *Store) Finalize(ctx context.Context, idemKey string, rec Record) error {
	rec.InFlight = false
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	// Refresh TTL on finalize to match the spec's "24h from final write".
	return s.rdb.Set(ctx, key(idemKey), raw, s.ttl).Err()
}

// Release deletes the in-flight marker. Use this when the handler decided NOT to
// persist a canonical response (e.g., the work failed before status was determined).
func (s *Store) Release(ctx context.Context, idemKey string) error {
	return s.rdb.Del(ctx, key(idemKey)).Err()
}
