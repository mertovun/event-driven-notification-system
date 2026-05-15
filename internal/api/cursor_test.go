package api

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	t.Parallel()
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)
	orig := cursor{TS: now, ID: id}

	enc, err := orig.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if enc == "" {
		t.Fatal("encoded cursor must not be empty")
	}

	back, err := decodeCursor(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !back.TS.Equal(orig.TS) {
		t.Errorf("ts: got %v, want %v", back.TS, orig.TS)
	}
	if back.ID != orig.ID {
		t.Errorf("id: got %v, want %v", back.ID, orig.ID)
	}
}

func TestDecodeCursor_Empty(t *testing.T) {
	t.Parallel()
	c, err := decodeCursor("")
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if !c.TS.IsZero() {
		t.Errorf("empty cursor should produce zero time")
	}
	if c.ID != uuid.Nil {
		t.Errorf("empty cursor should produce zero uuid")
	}
}

func TestDecodeCursor_Malformed(t *testing.T) {
	t.Parallel()
	cases := []string{
		"!!!not-base64!!!",
		"notvalidatall", // base64 but not JSON
		"e30",           // base64 "{}" — valid JSON, zero cursor
	}
	for _, c := range cases {
		c := c
		t.Run(c, func(t *testing.T) {
			t.Parallel()
			_, err := decodeCursor(c)
			// "e30" is valid JSON `{}` — that's fine, both fields stay zero.
			// The other two should fail.
			if c == "e30" && err != nil {
				t.Errorf("expected e30 to decode as zero cursor, got err=%v", err)
			}
			if c != "e30" && err == nil {
				t.Errorf("expected error for malformed cursor %q", c)
			}
		})
	}
}
