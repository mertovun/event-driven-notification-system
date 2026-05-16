package ws

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Filter restricts which status events a connection wants.
// Empty Filter (zero value) matches everything.
type Filter struct {
	BatchID *uuid.UUID
	Channel string // "", "sms", "email", "push"
}

// ParseFilter parses the ?filter= query param. Grammar: comma-separated
// key:value pairs, e.g. "batch_id:UUID,channel:sms". AND-composed.
// Returns the zero Filter when input is empty.
func ParseFilter(s string) (Filter, error) {
	var f Filter
	if s == "" {
		return f, nil
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), ":", 2)
		if len(kv) != 2 {
			return Filter{}, fmt.Errorf("malformed filter %q", part)
		}
		k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		switch k {
		case "batch_id":
			id, err := uuid.Parse(v)
			if err != nil {
				return Filter{}, fmt.Errorf("filter batch_id: %w", err)
			}
			f.BatchID = &id
		case "channel":
			switch v {
			case "sms", "email", "push":
				f.Channel = v
			default:
				return Filter{}, fmt.Errorf("filter channel: %q invalid", v)
			}
		default:
			return Filter{}, fmt.Errorf("unknown filter key %q", k)
		}
	}
	return f, nil
}

// Matches reports whether an event passes the filter.
func (f Filter) Matches(eventBatchID *uuid.UUID, eventChannel string) bool {
	if f.Channel != "" && f.Channel != eventChannel {
		return false
	}
	if f.BatchID != nil {
		if eventBatchID == nil || *eventBatchID != *f.BatchID {
			return false
		}
	}
	return true
}
