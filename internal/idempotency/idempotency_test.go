package idempotency

import "testing"

func TestCanonicalHash_KeyOrderIrrelevant(t *testing.T) {
	t.Parallel()
	a, err := CanonicalHash(map[string]any{"a": 1, "b": 2, "c": []any{"x", "y"}})
	if err != nil {
		t.Fatalf("hash a: %v", err)
	}
	b, err := CanonicalHash(map[string]any{"c": []any{"x", "y"}, "b": 2, "a": 1})
	if err != nil {
		t.Fatalf("hash b: %v", err)
	}
	if a != b {
		t.Fatalf("canonical hashes should match for key-order-different inputs:\n  a=%s\n  b=%s", a, b)
	}
}

func TestCanonicalHash_DifferentValuesDiffer(t *testing.T) {
	t.Parallel()
	a, _ := CanonicalHash(map[string]any{"recipient": "+90555111", "content": "hi"})
	b, _ := CanonicalHash(map[string]any{"recipient": "+90555111", "content": "HI"})
	if a == b {
		t.Fatalf("different content values must produce different hashes")
	}
}

func TestCanonicalHash_NestedMaps(t *testing.T) {
	t.Parallel()
	a, _ := CanonicalHash(map[string]any{
		"outer": map[string]any{"x": 1, "y": 2},
	})
	b, _ := CanonicalHash(map[string]any{
		"outer": map[string]any{"y": 2, "x": 1},
	})
	if a != b {
		t.Fatalf("nested map key order must not affect hash")
	}
}

func TestCanonicalHash_ArrayOrderMatters(t *testing.T) {
	t.Parallel()
	a, _ := CanonicalHash(map[string]any{"items": []any{"a", "b"}})
	b, _ := CanonicalHash(map[string]any{"items": []any{"b", "a"}})
	if a == b {
		t.Fatalf("array order is semantic and must affect hash")
	}
}

func TestCanonicalHash_StructValue(t *testing.T) {
	t.Parallel()
	type req struct {
		Channel   string `json:"channel"`
		Recipient string `json:"recipient"`
	}
	r1 := req{Channel: "sms", Recipient: "+90555"}
	r2 := req{Channel: "sms", Recipient: "+90555"}
	h1, err := CanonicalHash(r1)
	if err != nil {
		t.Fatalf("hash r1: %v", err)
	}
	h2, err := CanonicalHash(r2)
	if err != nil {
		t.Fatalf("hash r2: %v", err)
	}
	if h1 != h2 {
		t.Fatalf("identical struct values must produce identical hashes")
	}
}
