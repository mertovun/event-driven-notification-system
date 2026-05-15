package worker

import (
	"encoding/json"
	"testing"
)

func TestEnvelopeDecode(t *testing.T) {
	t.Parallel()
	raw := []byte(`{
		"notification_id": "019e2cc1-b65a-75de-9485-b097b8ed1663",
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "hi",
		"priority": "normal",
		"correlation_id": "corr-1"
	}`)
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.NotificationID == "" {
		t.Error("notification_id empty")
	}
	if env.Channel != "sms" {
		t.Errorf("channel: %q", env.Channel)
	}
	if env.CorrelationID != "corr-1" {
		t.Errorf("correlation: %q", env.CorrelationID)
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s    string
		n    int
		want string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "hello"},
		{"", 5, ""},
		{"abc", 3, "abc"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.s, func(t *testing.T) {
			t.Parallel()
			got := truncate(tc.s, tc.n)
			if got != tc.want {
				t.Errorf("truncate(%q, %d) = %q, want %q", tc.s, tc.n, got, tc.want)
			}
		})
	}
}
