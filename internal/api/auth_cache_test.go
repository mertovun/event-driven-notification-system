package api

import (
	"strings"
	"testing"
)

func TestAuthCacheKey_DeterministicAndHashed(t *testing.T) {
	t.Parallel()
	raw := "dev1234-do-not-use-in-prod-xxxxxxxxxx"
	k1 := authCacheKey(raw)
	k2 := authCacheKey(raw)
	if k1 != k2 {
		t.Fatalf("authCacheKey must be deterministic: %s vs %s", k1, k2)
	}
	if !strings.HasPrefix(k1, "auth:v1:") {
		t.Errorf("expected prefix auth:v1: got %s", k1)
	}
	if strings.Contains(k1, raw) {
		t.Errorf("raw bearer must NOT appear in cache key: %s", k1)
	}
}

func TestAuthCacheKey_DifferentInputs(t *testing.T) {
	t.Parallel()
	a := authCacheKey("key-a-xxxxxx")
	b := authCacheKey("key-b-xxxxxx")
	if a == b {
		t.Fatal("different bearers must produce different cache keys")
	}
}

func TestAuthCacheKey_PrefixOnlyChange(t *testing.T) {
	t.Parallel()
	// Two keys sharing the same key_prefix (first 8 chars) but with different
	// suffixes must still produce distinct cache keys — the hash covers the
	// whole raw bearer, not just the prefix.
	a := authCacheKey("samepfx-suffix-a")
	b := authCacheKey("samepfx-suffix-b")
	if a == b {
		t.Fatal("cache key must depend on the full raw bearer, not just the prefix")
	}
}
