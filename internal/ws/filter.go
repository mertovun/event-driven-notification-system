package ws

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Filter restricts which status events a connection wants.
//
// BatchID / Channel are caller-supplied via the ?filter= query string.
//
// OwnerID + AdminBypass are auth-derived and are the per-subscriber owner
// gate; they mirror the per-key row-ownership pattern on the HTTP read
// path (ADR-0019). With OwnerID set, the subscriber only sees events
// whose StatusEvent.CreatedBy equals their key id; AdminBypass=true
// short-circuits the gate so admin-scope readers see everything, the
// same way the *Scoped sqlc queries fall back to unscoped for admin.
// Without this gate any `notifications:read` subscriber could observe
// the status timeline of every other tenant — PII-light but a metadata
// side channel (volumes + correlation_ids).
type Filter struct {
	BatchID     *uuid.UUID
	Channel     string // "", "sms", "email", "push"
	OwnerID     *uuid.UUID
	AdminBypass bool
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

// Matches reports whether an event passes the filter. eventCreatedBy
// is the api_key_id that owns the underlying notification (StatusEvent
// .CreatedBy); nil means the event has no recorded owner (legacy rows
// pre-ownership migration). A nil-owned event is conservatively
// admin-only.
func (f Filter) Matches(eventBatchID *uuid.UUID, eventChannel string, eventCreatedBy *uuid.UUID) bool {
	if !f.AdminBypass {
		if f.OwnerID == nil {
			return false
		}
		if eventCreatedBy == nil || *eventCreatedBy != *f.OwnerID {
			return false
		}
	}
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
