package ws

import (
	"testing"

	"github.com/google/uuid"
)

func TestParseFilter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"", false},
		{"channel:sms", false},
		{"batch_id:" + uuid.NewString(), false},
		{"channel:sms,batch_id:" + uuid.NewString(), false},
		{"channel:fax", true},
		{"batch_id:not-a-uuid", true},
		{"oddkey:value", true},
		{"malformed", true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			_, err := ParseFilter(tc.in)
			if tc.wantErr && err == nil {
				t.Errorf("expected error for %q", tc.in)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error for %q: %v", tc.in, err)
			}
		})
	}
}

func TestFilter_Matches(t *testing.T) {
	t.Parallel()
	bid := uuid.New()
	zero := Filter{}
	chanOnly := Filter{Channel: "sms"}
	batchOnly := Filter{BatchID: &bid}

	if !zero.Matches(nil, "sms") {
		t.Error("zero filter must match everything")
	}
	if !chanOnly.Matches(nil, "sms") {
		t.Error("channel filter must match same channel")
	}
	if chanOnly.Matches(nil, "email") {
		t.Error("channel filter must reject other channels")
	}
	if !batchOnly.Matches(&bid, "sms") {
		t.Error("batch filter must match same batch")
	}
	if batchOnly.Matches(nil, "sms") {
		t.Error("batch filter must reject nil batch")
	}
	other := uuid.New()
	if batchOnly.Matches(&other, "sms") {
		t.Error("batch filter must reject other batch")
	}
}
