package notification

import "testing"

func TestChannelValid(t *testing.T) {
	cases := []struct {
		in   Channel
		want bool
	}{
		{ChannelSMS, true},
		{ChannelEmail, true},
		{ChannelPush, true},
		{"SMS", false},
		{"", false},
		{"fax", false},
	}
	for _, tc := range cases {
		if got := tc.in.Valid(); got != tc.want {
			t.Errorf("Channel(%q).Valid() = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestPriorityInt16(t *testing.T) {
	cases := []struct {
		in   Priority
		want int16
	}{
		{PriorityHigh, 9},
		{PriorityNormal, 5},
		{PriorityLow, 1},
		{"", 5},        // empty falls back to normal
		{"garbage", 5}, // unknown falls back to normal
	}
	for _, tc := range cases {
		if got := tc.in.Int16(); got != tc.want {
			t.Errorf("Priority(%q).Int16() = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestPriorityValid(t *testing.T) {
	cases := []struct {
		in   Priority
		want bool
	}{
		{PriorityHigh, true},
		{PriorityNormal, true},
		{PriorityLow, true},
		{"", true}, // empty is valid (defaults to normal downstream)
		{"urgent", false},
		{"HIGH", false},
	}
	for _, tc := range cases {
		if got := tc.in.Valid(); got != tc.want {
			t.Errorf("Priority(%q).Valid() = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestPriorityFromInt(t *testing.T) {
	cases := []struct {
		in   int16
		want Priority
	}{
		{9, PriorityHigh},
		{5, PriorityNormal},
		{1, PriorityLow},
		{0, PriorityNormal},  // unmapped → normal
		{42, PriorityNormal}, // unmapped → normal
		{-1, PriorityNormal}, // unmapped → normal
	}
	for _, tc := range cases {
		if got := PriorityFromInt(tc.in); got != tc.want {
			t.Errorf("PriorityFromInt(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestPriorityRoundTrip(t *testing.T) {
	for _, p := range []Priority{PriorityHigh, PriorityNormal, PriorityLow} {
		if got := PriorityFromInt(p.Int16()); got != p {
			t.Errorf("round trip %q → %d → %q failed", p, p.Int16(), got)
		}
	}
}

func TestStatusValid(t *testing.T) {
	cases := []struct {
		in   Status
		want bool
	}{
		{StatusPending, true},
		{StatusScheduled, true},
		{StatusQueued, true},
		{StatusSending, true},
		{StatusSent, true},
		{StatusFailed, true},
		{StatusCancelled, true},
		{StatusDeadLetter, true},
		{"", false},
		{"PENDING", false},
		{"unknown", false},
	}
	for _, tc := range cases {
		if got := tc.in.Valid(); got != tc.want {
			t.Errorf("Status(%q).Valid() = %v, want %v", tc.in, got, tc.want)
		}
	}
}
