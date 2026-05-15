package queue

import (
	"testing"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

func TestRoutingKey(t *testing.T) {
	t.Parallel()
	cases := []struct {
		channel string
		want    string
	}{
		{"sms", "notification.sms"},
		{"email", "notification.email"},
		{"push", "notification.push"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.channel, func(t *testing.T) {
			t.Parallel()
			got := RoutingKey(tc.channel)
			if got != tc.want {
				t.Errorf("RoutingKey(%q) = %q, want %q", tc.channel, got, tc.want)
			}
		})
	}
}

func TestPriorityMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		label notification.Priority
		want  int16
	}{
		{notification.PriorityHigh, 9},
		{notification.PriorityNormal, 5},
		{notification.PriorityLow, 1},
		{"", 5}, // empty → normal
	}
	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.label), func(t *testing.T) {
			t.Parallel()
			got := tc.label.Int16()
			if got != tc.want {
				t.Errorf("(%q).Int16() = %d, want %d", tc.label, got, tc.want)
			}
		})
	}
}

func TestPriorityFromInt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   int16
		want notification.Priority
	}{
		{9, notification.PriorityHigh},
		{5, notification.PriorityNormal},
		{1, notification.PriorityLow},
		{0, notification.PriorityNormal}, // unknown defaults to normal
	}
	for _, tc := range cases {
		tc := tc
		t.Run("", func(t *testing.T) {
			t.Parallel()
			got := notification.PriorityFromInt(tc.in)
			if got != tc.want {
				t.Errorf("PriorityFromInt(%d) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestChannelsListedAllValid(t *testing.T) {
	t.Parallel()
	for _, c := range Channels {
		if !notification.Channel(c).Valid() {
			t.Errorf("Channels[]: %q is not a valid notification.Channel", c)
		}
	}
}

func TestRedactURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   string
		want string
	}{
		{"amqp://user:pass@host:5672/", "amqp://***@host:5672/"},
		{"amqp://guest:guest@rabbitmq:5672/", "amqp://***@rabbitmq:5672/"},
		{"amqp://host:5672/", "amqp://host:5672/"}, // no creds → unchanged
		{"amqp://nocreds", "amqp://nocreds"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got := redactURL(tc.in)
			if got != tc.want {
				t.Errorf("redactURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
