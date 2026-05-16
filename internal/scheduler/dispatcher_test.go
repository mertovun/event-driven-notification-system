package scheduler

import "testing"

func TestPriorityLabel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in   int16
		want string
	}{
		{9, "high"},
		{5, "normal"},
		{1, "low"},
		{0, "normal"},  // unknown defaults
		{42, "normal"}, // unknown defaults
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			got := priorityLabel(tc.in)
			if got != tc.want {
				t.Errorf("priorityLabel(%d) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
