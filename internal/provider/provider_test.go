package provider

import (
	"net/netip"
	"testing"
)

func TestDenyZone(t *testing.T) {
	t.Parallel()
	cases := []struct {
		ip   string
		zone string
		deny bool
	}{
		{"127.0.0.1", "loopback", true},
		{"169.254.0.1", "link-local", true},
		{"169.254.169.254", "metadata", true},
		{"10.0.0.1", "rfc1918", true},
		{"192.168.1.1", "rfc1918", true},
		{"172.20.0.1", "rfc1918", true},
		{"100.64.0.1", "cgnat", true},
		{"fc00::1", "unique-local-v6", true},
		{"8.8.8.8", "", false},
		{"1.1.1.1", "", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.ip, func(t *testing.T) {
			t.Parallel()
			ip := netip.MustParseAddr(tc.ip)
			zone, blocked := denyZone(ip)
			if blocked != tc.deny {
				t.Errorf("ip=%s: blocked=%v, want %v", tc.ip, blocked, tc.deny)
			}
			if blocked && zone != tc.zone {
				t.Errorf("ip=%s: zone=%q, want %q", tc.ip, zone, tc.zone)
			}
		})
	}
}

func TestErrorKindIsRetryable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		k    ErrorKind
		want bool
	}{
		{ErrNetwork, true},
		{ErrTimeout, true},
		{ErrServer, true},
		{ErrRateLimited, true},
		{ErrSSRFBlocked, false},
		{ErrClient, false},
		{ErrUnknown, false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.k.String(), func(t *testing.T) {
			t.Parallel()
			if tc.k.IsRetryable() != tc.want {
				t.Errorf("%s.IsRetryable() = %v, want %v", tc.k, tc.k.IsRetryable(), tc.want)
			}
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	if d := parseRetryAfter("5"); d.Seconds() != 5 {
		t.Errorf("seconds: %v", d)
	}
	if d := parseRetryAfter(""); d != 0 {
		t.Errorf("empty: %v", d)
	}
	if d := parseRetryAfter("not-a-number"); d != 0 {
		t.Errorf("garbage: %v", d)
	}
}

func TestNew_RejectsBadEndpoint(t *testing.T) {
	t.Parallel()
	if _, err := New("", ""); err == nil {
		t.Error("expected error on empty endpoint")
	}
	if _, err := New("not-a-url://", ""); err == nil {
		t.Error("expected error on bad scheme")
	}
}
