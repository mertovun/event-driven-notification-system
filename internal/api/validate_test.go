package api

import (
	"strings"
	"testing"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

func TestValidateRecipient_SMS(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool // valid?
	}{
		{"+905551234567", true},
		{"+1234567890", true},
		{"905551234567", false},          // no leading +
		{"+05551234567", false},          // first digit must be 1-9
		{"+9", false},                    // too short
		{"+12345678901234567890", false}, // too long for E.164
		{"", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			errs := validateRecipient(notification.ChannelSMS, tc.in)
			if (len(errs) == 0) != tc.want {
				t.Errorf("recipient %q: got valid=%v, want valid=%v (errs=%v)", tc.in, len(errs) == 0, tc.want, errs)
			}
		})
	}
}

func TestValidateRecipient_Email(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"a@b.com", true},
		{"alice@example.io", true},
		{"plainstring", false},
		{"@no-local", false},
		{"", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			errs := validateRecipient(notification.ChannelEmail, tc.in)
			if (len(errs) == 0) != tc.want {
				t.Errorf("recipient %q: got valid=%v, want valid=%v", tc.in, len(errs) == 0, tc.want)
			}
		})
	}
}

func TestValidateRecipient_Push(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want bool
	}{
		{"abcdef0123456789", true}, // 16 chars
		{strings.Repeat("a", 256), true},
		{"short", false},                  // < 16
		{strings.Repeat("x", 513), false}, // > 512
		{"", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in[:min(8, len(tc.in))]+"…", func(t *testing.T) {
			t.Parallel()
			errs := validateRecipient(notification.ChannelPush, tc.in)
			if (len(errs) == 0) != tc.want {
				t.Errorf("recipient %q: got valid=%v, want valid=%v", tc.in, len(errs) == 0, tc.want)
			}
		})
	}
}

func TestValidateContent_LengthCaps(t *testing.T) {
	t.Parallel()
	cases := []struct {
		channel notification.Channel
		length  int
		ok      bool
	}{
		{notification.ChannelSMS, 1, true},
		{notification.ChannelSMS, 1000, true},
		{notification.ChannelSMS, 1001, false},
		{notification.ChannelEmail, 256 * 1024, true},
		{notification.ChannelEmail, 256*1024 + 1, false},
		{notification.ChannelPush, 4 * 1024, true},
		{notification.ChannelPush, 4*1024 + 1, false},
		{notification.ChannelSMS, 0, false}, // empty
	}
	for _, tc := range cases {
		tc := tc
		t.Run("", func(t *testing.T) {
			t.Parallel()
			errs := validateContent(tc.channel, strings.Repeat("a", tc.length))
			if (len(errs) == 0) != tc.ok {
				t.Errorf("%s len=%d: got valid=%v, want valid=%v", tc.channel, tc.length, len(errs) == 0, tc.ok)
			}
		})
	}
}

func TestValidateCreate_RequiresContentXorTemplate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		req       notificationCreateRequest
		wantValid bool
		wantField string
	}{
		{
			name: "valid_content",
			req: notificationCreateRequest{
				Channel: notification.ChannelSMS, Recipient: "+905551234567",
				Content: "hi", Priority: notification.PriorityNormal,
			},
			wantValid: true,
		},
		{
			name: "missing_content_and_template",
			req: notificationCreateRequest{
				Channel: notification.ChannelSMS, Recipient: "+905551234567",
				Priority: notification.PriorityNormal,
			},
			wantValid: false,
			wantField: "content",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			errs := validateCreate(tc.req)
			if tc.wantValid && len(errs) != 0 {
				t.Fatalf("expected valid, got errs: %v", errs)
			}
			if !tc.wantValid {
				if len(errs) == 0 {
					t.Fatalf("expected validation errors")
				}
				found := false
				for _, e := range errs {
					if e.Field == tc.wantField {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error on field %q, got: %v", tc.wantField, errs)
				}
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
