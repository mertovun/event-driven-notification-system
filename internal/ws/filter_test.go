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

// TestFilter_OwnerGate exercises the per-subscriber owner gate. Without
// AdminBypass + a matching OwnerID, no event passes — that's the security
// property the new filter is enforcing.
func TestFilter_OwnerGate(t *testing.T) {
	t.Parallel()
	owner := uuid.New()
	other := uuid.New()

	t.Run("unauthed subscriber sees nothing", func(t *testing.T) {
		t.Parallel()
		f := Filter{} // no OwnerID, no AdminBypass
		if f.Matches(nil, "sms", &owner) {
			t.Error("filter without OwnerID or AdminBypass must reject all events")
		}
	})
	t.Run("subscriber sees own events", func(t *testing.T) {
		t.Parallel()
		f := Filter{OwnerID: &owner}
		if !f.Matches(nil, "sms", &owner) {
			t.Error("subscriber's own event must pass")
		}
	})
	t.Run("subscriber does NOT see other tenant's events", func(t *testing.T) {
		t.Parallel()
		f := Filter{OwnerID: &owner}
		if f.Matches(nil, "sms", &other) {
			t.Error("other-tenant event must be filtered out")
		}
	})
	t.Run("subscriber does NOT see legacy nil-owner events", func(t *testing.T) {
		t.Parallel()
		f := Filter{OwnerID: &owner}
		if f.Matches(nil, "sms", nil) {
			t.Error("legacy nil-owner event must be admin-only")
		}
	})
	t.Run("admin sees everything", func(t *testing.T) {
		t.Parallel()
		f := Filter{AdminBypass: true}
		if !f.Matches(nil, "sms", &owner) {
			t.Error("admin must see owned events")
		}
		if !f.Matches(nil, "sms", &other) {
			t.Error("admin must see other-tenant events")
		}
		if !f.Matches(nil, "sms", nil) {
			t.Error("admin must see legacy nil-owner events")
		}
	})
}

// TestFilter_UserSuppliedClauses keeps coverage on the original
// BatchID/Channel filters now that they compose with the owner gate.
// AdminBypass=true is used here so the gate doesn't shadow the other
// clauses being tested.
func TestFilter_UserSuppliedClauses(t *testing.T) {
	t.Parallel()
	bid := uuid.New()
	other := uuid.New()

	chanOnly := Filter{Channel: "sms", AdminBypass: true}
	batchOnly := Filter{BatchID: &bid, AdminBypass: true}
	chanAndBatch := Filter{Channel: "sms", BatchID: &bid, AdminBypass: true}
	wide := Filter{AdminBypass: true}

	if !wide.Matches(nil, "sms", nil) {
		t.Error("admin wide filter must match everything")
	}
	if !chanOnly.Matches(nil, "sms", nil) {
		t.Error("channel filter must match same channel")
	}
	if chanOnly.Matches(nil, "email", nil) {
		t.Error("channel filter must reject other channels")
	}
	if !batchOnly.Matches(&bid, "sms", nil) {
		t.Error("batch filter must match same batch")
	}
	if batchOnly.Matches(nil, "sms", nil) {
		t.Error("batch filter must reject nil batch")
	}
	if batchOnly.Matches(&other, "sms", nil) {
		t.Error("batch filter must reject other batch")
	}
	if !chanAndBatch.Matches(&bid, "sms", nil) {
		t.Error("composite filter must match when both clauses agree")
	}
	if chanAndBatch.Matches(&bid, "email", nil) {
		t.Error("composite filter must reject when channel mismatches")
	}
	if chanAndBatch.Matches(&other, "sms", nil) {
		t.Error("composite filter must reject when batch mismatches")
	}
}
